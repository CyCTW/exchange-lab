//! Gateway 執行緒：模擬一群用戶的下單行為，並扮演交易所 gateway 的角色
//! （指派訂單編號、每用戶限流、把請求路由到用戶所屬的風控分片、收執行回報）。
//!
//! 送單依固定速率排程，延遲從「預定送出時間」起算（避免 coordinated omission）。

use std::sync::Arc;

use super::model::*;
use super::msg::*;
use crate::histogram::Histogram;
use crate::idle::{Clock, Idle};
use crate::ring::{Consumer, Producer};
use crate::types::*;
use crate::workload::Rng;

#[derive(Default)]
pub struct GatewayStats {
    pub id: usize,
    pub users: usize,
    pub actions: u64,
    pub sent_new: u64,
    pub sent_cancel: u64,
    pub throttled: u64,
    pub responses: u64,
    pub acks: u64,
    pub rejects: [u64; RejectCode::COUNT],
    pub fills: u64,
    pub done: u64,
    pub cancel_rejects: u64,
    /// 請求 → 回應（Ack / 拒絕 / 撤單完成）的端到端延遲。
    pub response_latency: Histogram,
    /// 排程落後的最大值：大於 0 表示 gateway 自己就跟不上設定的速率。
    pub max_behind_ns: u64,
}

struct TokenBucket {
    tokens: f64,
    cap: f64,
    per_ns: f64,
    last: u64,
}

impl TokenBucket {
    fn new(per_sec: u64) -> Self {
        let cap = (per_sec as f64 / 10.0).max(10.0);
        TokenBucket {
            tokens: cap,
            cap,
            per_ns: per_sec as f64 / 1e9,
            last: 0,
        }
    }
    fn take(&mut self, n: u64, now: u64) -> bool {
        let dt = now.saturating_sub(self.last);
        self.last = self.last.max(now);
        self.tokens = (self.tokens + dt as f64 * self.per_ns).min(self.cap);
        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            true
        } else {
            false
        }
    }
}

struct UserState {
    id: UserId,
    /// 做市商負責的商品。
    symbol: SymbolId,
    next_seq: u32,
    bucket: TokenBucket,
    /// 一般用戶目前的掛單（gateway 端依執行回報維護的視圖）。
    live: Vec<(OrderId, SymbolId)>,
    /// 做市商目前的買/賣報價。
    bid: Option<OrderId>,
    ask: Option<OrderId>,
    turn: bool,
}

impl UserState {
    fn next_id(&mut self) -> OrderId {
        self.next_seq += 1;
        make_order_id(self.id, self.next_seq)
    }
}

pub struct GatewayIo {
    pub to_risk: Vec<Producer<ToRisk>>,
    pub from_risk: Vec<Consumer<ToGateway>>,
}

pub struct Gateway {
    cfg: Arc<SimConfig>,
    path: Arc<PricePath>,
    picker: WeightedPicker,
    /// 每個商品的「價格單位」，用來把報價偏移量縮放到合理的 tick 數。
    unit: Vec<Price>,
    users: Vec<UserState>,
    mm_by_symbol: Vec<Vec<usize>>,
    mms: Vec<usize>,
    retail: Vec<usize>,
    takers: Vec<usize>,
    io: GatewayIo,
    clock: Clock,
    start_ns: u64,
    rng: Rng,
    idle: Idle,
    stats: GatewayStats,
    shutdowns: usize,
}

impl Gateway {
    pub fn new(
        id: usize,
        cfg: Arc<SimConfig>,
        symbols: &[SymbolSpec],
        path: Arc<PricePath>,
        io: GatewayIo,
        clock: Clock,
        start_ns: u64,
    ) -> Self {
        let g = cfg.gateways;
        let mut users = Vec::new();
        let (mut mms, mut retail, mut takers) = (vec![], vec![], vec![]);
        let mut mm_by_symbol = vec![vec![]; cfg.symbols];
        for u in (id..cfg.users).step_by(g) {
            let u = u as UserId;
            let li = users.len();
            let kind = cfg.user_kind(u);
            let limit = match kind {
                UserKind::MarketMaker => cfg.mm_msgs_per_sec,
                _ => cfg.user_msgs_per_sec,
            };
            match kind {
                UserKind::MarketMaker => {
                    mms.push(li);
                    mm_by_symbol[cfg.mm_symbol(u) as usize].push(li);
                }
                UserKind::Retail => retail.push(li),
                UserKind::Taker => takers.push(li),
            }
            users.push(UserState {
                id: u,
                symbol: if kind == UserKind::MarketMaker {
                    cfg.mm_symbol(u)
                } else {
                    0
                },
                next_seq: 0,
                bucket: TokenBucket::new(limit),
                live: Vec::with_capacity(8),
                bid: None,
                ask: None,
                turn: false,
            });
        }
        let stats = GatewayStats {
            id,
            users: users.len(),
            ..Default::default()
        };
        Gateway {
            picker: WeightedPicker::new(&cfg.zipf_weights()),
            unit: symbols
                .iter()
                .map(|s| (s.init_price / 5_000).max(1))
                .collect(),
            rng: Rng::new(cfg.seed ^ (0xA11CE + id as u64 * 104_729)),
            idle: Idle::new(cfg.idle),
            cfg,
            path,
            users,
            mm_by_symbol,
            mms,
            retail,
            takers,
            io,
            clock,
            start_ns,
            stats,
            shutdowns: 0,
        }
    }

    pub fn run(mut self) -> GatewayStats {
        let per_gateway = (self.cfg.rate as f64 / self.cfg.gateways as f64).max(1.0);
        let interval = 1e9 / per_gateway;
        let end_ns = self.start_ns + self.cfg.duration.as_nanos() as u64;
        let mut k: u64 = 0;
        loop {
            let due = self.start_ns + (k as f64 * interval) as u64;
            if due >= end_ns {
                break;
            }
            let now = self.clock.now_ns();
            if now >= due {
                self.stats.max_behind_ns = self.stats.max_behind_ns.max(now - due);
                self.act(due);
                k += 1;
                self.poll_reports();
                self.idle.reset();
            } else if self.poll_reports() > 0 {
                self.idle.reset();
            } else if due - now > 200_000 {
                self.idle.idle();
            } else {
                std::hint::spin_loop();
            }
        }

        for r in 0..self.cfg.risk_shards {
            self.push(r, ToRisk::Shutdown);
        }
        while self.shutdowns < self.cfg.risk_shards {
            if self.poll_reports() == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
        self.stats
    }

    fn act(&mut self, t: u64) {
        self.stats.actions += 1;
        let r = self.rng.below(100);
        let msgs = if r < 60 && !self.mms.is_empty() {
            self.mm_action(t)
        } else if r < 90 && !self.retail.is_empty() {
            self.retail_action(t)
        } else if !self.takers.is_empty() {
            self.taker_action(t)
        } else if !self.retail.is_empty() {
            self.retail_action(t)
        } else {
            self.mm_action(t)
        };
        for m in msgs.into_iter().flatten() {
            self.send(m);
        }
    }

    fn fair(&self, symbol: SymbolId, t: u64) -> Price {
        self.path
            .at(symbol as usize, t.saturating_sub(self.start_ns))
    }

    /// 做市商：輪流更新買價/賣價，每次「撤舊單 + 掛新單」。
    fn mm_action(&mut self, t: u64) -> [Option<ToRisk>; 2] {
        let sym = self.picker.pick(&mut self.rng);
        let pool = if self.mm_by_symbol[sym].is_empty() {
            &self.mms
        } else {
            &self.mm_by_symbol[sym]
        };
        let li = pool[self.rng.below(pool.len() as u64) as usize];
        let symbol = self.users[li].symbol;
        let fair = self.fair(symbol, t);
        let off = self.unit[symbol as usize] * (1 + self.rng.below(3) as Price);
        let qty = 1 + self.rng.below(20);
        let u = &mut self.users[li];
        let side = if u.turn { Side::Buy } else { Side::Sell };
        u.turn = !u.turn;
        let slot = match side {
            Side::Buy => &mut u.bid,
            Side::Sell => &mut u.ask,
        };
        let old = *slot;
        if !u.bucket.take(1 + old.is_some() as u64, t) {
            self.stats.throttled += 1;
            return [None, None];
        }
        let cancel = old.map(|order_id| ToRisk::Cancel { order_id, t });
        let order_id = u.next_id();
        let slot = match side {
            Side::Buy => &mut u.bid,
            Side::Sell => &mut u.ask,
        };
        *slot = Some(order_id);
        let price = match side {
            Side::Buy => fair - off,
            Side::Sell => fair + off,
        };
        let new = ToRisk::New {
            order_id,
            symbol,
            side,
            price,
            qty,
            tif: TimeInForce::Gtc,
            t,
        };
        [cancel, Some(new)]
    }

    /// 一般用戶：掛被動限價單，或撤掉自己的某張掛單。
    fn retail_action(&mut self, t: u64) -> [Option<ToRisk>; 2] {
        let li = self.retail[self.rng.below(self.retail.len() as u64) as usize];
        let want_cancel = {
            let live = self.users[li].live.len();
            live >= 5 || (live > 0 && self.rng.below(100) < 30)
        };
        if !self.users[li].bucket.take(1, t) {
            self.stats.throttled += 1;
            return [None, None];
        }
        if want_cancel {
            let u = &mut self.users[li];
            let i = self.rng.below(u.live.len() as u64) as usize;
            let (order_id, _) = u.live.swap_remove(i);
            return [Some(ToRisk::Cancel { order_id, t }), None];
        }
        let symbol = self.picker.pick(&mut self.rng) as SymbolId;
        let side = if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let off = self.unit[symbol as usize] * (1 + self.rng.below(30) as Price);
        let fair = self.fair(symbol, t);
        let price = match side {
            Side::Buy => fair - off,
            Side::Sell => fair + off,
        };
        let qty = 1 + self.rng.below(10);
        let u = &mut self.users[li];
        let order_id = u.next_id();
        u.live.push((order_id, symbol));
        [
            Some(ToRisk::New {
                order_id,
                symbol,
                side,
                price,
                qty,
                tif: TimeInForce::Gtc,
                t,
            }),
            None,
        ]
    }

    /// 主動吃單者：以穿價的 IOC 吃掉對手方掛單。
    fn taker_action(&mut self, t: u64) -> [Option<ToRisk>; 2] {
        let li = self.takers[self.rng.below(self.takers.len() as u64) as usize];
        if !self.users[li].bucket.take(1, t) {
            self.stats.throttled += 1;
            return [None, None];
        }
        let symbol = self.picker.pick(&mut self.rng) as SymbolId;
        let side = if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let off = self.unit[symbol as usize] * 5;
        let fair = self.fair(symbol, t);
        let price = match side {
            Side::Buy => fair + off,
            Side::Sell => fair - off,
        };
        let qty = 1 + self.rng.below(10);
        let order_id = self.users[li].next_id();
        [
            Some(ToRisk::New {
                order_id,
                symbol,
                side,
                price,
                qty,
                tif: TimeInForce::Ioc,
                t,
            }),
            None,
        ]
    }

    fn send(&mut self, m: ToRisk) {
        let (user, is_new) = match m {
            ToRisk::New { order_id, .. } => (order_user(order_id), true),
            ToRisk::Cancel { order_id, .. } => (order_user(order_id), false),
            ToRisk::Shutdown => unreachable!(),
        };
        if is_new {
            self.stats.sent_new += 1;
        } else {
            self.stats.sent_cancel += 1;
        }
        self.push(self.cfg.risk_of(user), m);
    }

    /// 佇列滿時一邊等一邊消化回報——否則會和風控分片互相等待而死結（見 docs/08）。
    fn push(&mut self, r: usize, mut m: ToRisk) {
        loop {
            match self.io.to_risk[r].try_push(m) {
                Ok(()) => return,
                Err(back) => {
                    m = back;
                    if self.poll_reports() == 0 {
                        std::hint::spin_loop();
                    }
                }
            }
        }
    }

    fn poll_reports(&mut self) -> usize {
        let mut n = 0;
        for r in 0..self.io.from_risk.len() {
            for _ in 0..256 {
                let Some(m) = self.io.from_risk[r].try_pop() else {
                    break;
                };
                n += 1;
                self.on_report(m);
            }
        }
        n
    }

    fn on_report(&mut self, m: ToGateway) {
        let ToGateway::Report {
            order_id,
            kind,
            response,
            t,
        } = m
        else {
            self.shutdowns += 1;
            return;
        };
        if response {
            self.stats.responses += 1;
            let now = self.clock.now_ns();
            self.stats.response_latency.record(now.saturating_sub(t));
        }
        match kind {
            ExecKind::Ack => self.stats.acks += 1,
            ExecKind::Rejected(code) => {
                self.stats.rejects[code as usize] += 1;
                self.forget(order_id);
            }
            ExecKind::Fill { leaves, .. } => {
                self.stats.fills += 1;
                if leaves == 0 {
                    self.forget(order_id);
                }
            }
            ExecKind::Done { .. } => {
                self.stats.done += 1;
                self.forget(order_id);
            }
            ExecKind::CancelRejected => self.stats.cancel_rejects += 1,
        }
    }

    fn forget(&mut self, order_id: OrderId) {
        let li = self
            .cfg
            .local_index(order_user(order_id), self.cfg.gateways);
        let u = &mut self.users[li];
        if u.bid == Some(order_id) {
            u.bid = None;
        }
        if u.ask == Some(order_id) {
            u.ask = None;
        }
        if let Some(i) = u.live.iter().position(|x| x.0 == order_id) {
            u.live.swap_remove(i);
        }
    }
}
