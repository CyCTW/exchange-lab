//! Gateway：會員 session 的入口。
//!
//! 一個 gateway 執行緒負責一群會員（member % G）。它同時扮演兩個角色：
//! - **模擬會員**：做市商以改單更新報價、經紀商掛單/撤單/市價、自營商 IOC 吃單。
//! - **交易所 gateway**：指派訂單編號、session 限流、事前風控（數量上限、價格合理性、
//!   信用額度），通過後送往定序器；收執行回報、維護每個會員的未結委託與部位。
//!
//! 事前風控在定序「之前」完成，被擋下的委託不會進入 journal，不影響撮合的確定性。

use std::collections::HashMap;
use std::sync::Arc;

use super::model::*;
use super::msg::*;
use crate::histogram::Histogram;
use crate::idle::{Clock, Idle};
use crate::ring::{Consumer, Producer};
use crate::types::*;
use crate::workload::{PricePath, Rng, WeightedPicker};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GwReject {
    Throttled = 0,
    MaxQty = 1,
    PriceCollar = 2,
    Credit = 3,
}

impl GwReject {
    pub const ALL: [GwReject; 4] = [
        GwReject::Throttled,
        GwReject::MaxQty,
        GwReject::PriceCollar,
        GwReject::Credit,
    ];
    pub fn name(self) -> &'static str {
        match self {
            GwReject::Throttled => "限流",
            GwReject::MaxQty => "超過單筆數量上限",
            GwReject::PriceCollar => "價格偏離參考價",
            GwReject::Credit => "信用額度不足",
        }
    }
}

#[derive(Default)]
pub struct GatewayStats {
    pub id: usize,
    pub members: usize,
    pub actions: u64,
    pub sent_new: u64,
    pub sent_cancel: u64,
    pub sent_replace: u64,
    pub sent_mass_cancel: u64,
    pub gw_rejects: [u64; 4],
    pub responses: u64,
    pub acks: u64,
    pub fills: u64,
    pub done: u64,
    pub cancel_rejects: u64,
    pub rejects: HashMap<String, u64>,
    pub response_latency: Histogram,
    pub max_behind_ns: u64,
    /// 結束時：此 gateway 所有會員在各商品的淨部位、淨現金流。
    pub net_position: Vec<i64>,
    pub net_cash: i128,
    pub open_orders: usize,
    /// 依未結委託重算的信用占用是否與帳上一致。
    pub credit_consistent: bool,
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
    fn take(&mut self, now: u64) -> bool {
        let dt = now.saturating_sub(self.last);
        self.last = self.last.max(now);
        self.tokens = (self.tokens + dt as f64 * self.per_ns).min(self.cap);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct MemberState {
    id: MemberId,
    bucket: TokenBucket,
    next_seq: u64,
    credit_limit: i64,
    open_notional: i64,
    /// 做市商負責的商品與目前的 [買, 賣] 報價。
    insts: Vec<InstrumentId>,
    quotes: Vec<[Option<OrderId>; 2]>,
    turn: bool,
    /// 經紀商目前的掛單。
    live: Vec<OrderId>,
    offline_until: u64,
}

impl MemberState {
    fn next_id(&mut self) -> OrderId {
        self.next_seq += 1;
        make_order_id(self.id, self.next_seq)
    }
}

struct OpenOrder {
    li: usize,
    inst: InstrumentId,
    price: Price,
    leaves: Qty,
}

pub struct GatewayIo {
    pub to_seq: Producer<Request>,
    pub from_part: Vec<Consumer<ToGateway>>,
}

pub struct Gateway {
    id: usize,
    cfg: Arc<ExchangeConfig>,
    insts: Arc<Vec<Instrument>>,
    path: Arc<PricePath>,
    picker: WeightedPicker,
    unit: Vec<Price>,
    members: Vec<MemberState>,
    mms: Vec<usize>,
    brokers: Vec<usize>,
    props: Vec<usize>,
    mm_by_inst: Vec<Vec<(usize, usize)>>,
    orders: HashMap<OrderId, OpenOrder>,
    net_pos: Vec<i64>,
    cash: i128,
    io: GatewayIo,
    clock: Clock,
    start_ns: u64,
    rng: Rng,
    idle: Idle,
    stats: GatewayStats,
    shutdowns: usize,
    disconnected: bool,
}

impl Gateway {
    pub fn new(
        id: usize,
        cfg: Arc<ExchangeConfig>,
        insts: Arc<Vec<Instrument>>,
        path: Arc<PricePath>,
        io: GatewayIo,
        clock: Clock,
        start_ns: u64,
    ) -> Self {
        let mut members = vec![];
        let (mut mms, mut brokers, mut props) = (vec![], vec![], vec![]);
        let mut mm_by_inst = vec![vec![]; cfg.instruments];
        for m in (id..cfg.members).step_by(cfg.gateways) {
            let m = m as MemberId;
            let li = members.len();
            let kind = cfg.member_kind(m);
            let insts = if kind == MemberKind::MarketMaker {
                cfg.mm_instruments(m)
            } else {
                vec![]
            };
            match kind {
                MemberKind::MarketMaker => {
                    mms.push(li);
                    for (k, &i) in insts.iter().enumerate() {
                        mm_by_inst[i as usize].push((li, k));
                    }
                }
                MemberKind::Broker => brokers.push(li),
                MemberKind::Prop => props.push(li),
            }
            let limit = if kind == MemberKind::MarketMaker {
                cfg.mm_msgs_per_sec
            } else {
                cfg.member_msgs_per_sec
            };
            members.push(MemberState {
                id: m,
                bucket: TokenBucket::new(limit),
                next_seq: 0,
                credit_limit: cfg.credit_limit(m),
                open_notional: 0,
                quotes: vec![[None, None]; insts.len()],
                insts,
                turn: false,
                live: Vec::with_capacity(256),
                offline_until: 0,
            });
        }
        Gateway {
            id,
            picker: WeightedPicker::new(&cfg.weights()),
            unit: insts.iter().map(|s| (s.reference / 2_000).max(1)).collect(),
            rng: Rng::new(cfg.seed ^ (0xB0B + id as u64 * 104_729)),
            idle: Idle::new(cfg.idle),
            net_pos: vec![0; cfg.instruments],
            stats: GatewayStats {
                id,
                members: members.len(),
                ..Default::default()
            },
            cfg,
            insts,
            path,
            members,
            mms,
            brokers,
            props,
            mm_by_inst,
            orders: HashMap::with_capacity(1 << 16),
            cash: 0,
            io,
            clock,
            start_ns,
            shutdowns: 0,
            disconnected: false,
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
                self.poll();
                self.idle.reset();
            } else if self.poll() > 0 {
                self.idle.reset();
            } else if due - now > 200_000 {
                self.idle.idle();
            } else {
                std::hint::spin_loop();
            }
        }
        self.push(Request::Shutdown);
        while self.shutdowns < self.cfg.partitions {
            if self.poll() == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
        self.finish()
    }

    fn elapsed(&self, t: u64) -> u64 {
        t.saturating_sub(self.start_ns)
    }

    fn act(&mut self, t: u64) {
        let elapsed = self.elapsed(t);
        let phase = self.cfg.phase_at(elapsed);
        if phase == Phase::Closed {
            return;
        }
        self.maybe_disconnect(t);
        self.stats.actions += 1;
        let r = self.rng.below(100);
        let req = if r < 55 && !self.mms.is_empty() {
            self.mm_action(t)
        } else if r < 85 && !self.brokers.is_empty() {
            self.broker_action(t, phase)
        } else if !self.props.is_empty() {
            self.prop_action(t, phase)
        } else {
            None
        };
        if let Some(req) = req {
            self.send(req);
        }
    }

    /// 模擬一個經紀商斷線：交易所自動撤掉它所有委託（cancel-on-disconnect），一段時間後重新連線。
    fn maybe_disconnect(&mut self, t: u64) {
        if !self.cfg.disconnect || self.disconnected || self.id != 0 || self.brokers.is_empty() {
            return;
        }
        if self.elapsed(t) < self.cfg.at(DISCONNECT_AT) {
            return;
        }
        self.disconnected = true;
        let li = self.brokers[0];
        self.members[li].offline_until = self.start_ns + self.cfg.at(RECONNECT_AT);
        let member = self.members[li].id;
        self.stats.sent_mass_cancel += 1;
        self.push(Request::MassCancel { member, t });
    }

    fn fair(&self, inst: InstrumentId, t: u64) -> Price {
        self.path.at(inst as usize, self.elapsed(t))
    }

    fn gw_reject(&mut self, r: GwReject) -> Option<Request> {
        self.stats.gw_rejects[r as usize] += 1;
        None
    }

    /// 事前風控。通過就占用信用額度並登記未結委託。
    fn pre_trade(
        &mut self,
        li: usize,
        id: OrderId,
        inst: InstrumentId,
        price: Price,
        qty: Qty,
    ) -> Result<(), GwReject> {
        if qty > self.cfg.max_order_qty {
            return Err(GwReject::MaxQty);
        }
        let r = self.insts[inst as usize].reference;
        if (price - r).abs() * 100 > r * self.cfg.price_collar_pct {
            return Err(GwReject::PriceCollar);
        }
        let notional = price * qty as i64;
        let m = &mut self.members[li];
        if m.open_notional.saturating_add(notional) > m.credit_limit {
            return Err(GwReject::Credit);
        }
        m.open_notional += notional;
        self.orders.insert(
            id,
            OpenOrder {
                li,
                inst,
                price,
                leaves: qty,
            },
        );
        Ok(())
    }

    /// 做市商：輪流更新某個商品的買價或賣價。已有報價就用改單（Replace），否則新掛。
    fn mm_action(&mut self, t: u64) -> Option<Request> {
        let inst = self.picker.pick(&mut self.rng);
        let (li, k) = if self.mm_by_inst[inst].is_empty() {
            let li = self.mms[self.rng.below(self.mms.len() as u64) as usize];
            let n = self.members[li].insts.len();
            if n == 0 {
                return None;
            }
            (li, self.rng.below(n as u64) as usize)
        } else {
            let c = &self.mm_by_inst[inst];
            c[self.rng.below(c.len() as u64) as usize]
        };
        let inst = self.members[li].insts[k];
        let fair = self.fair(inst, t);
        let off = self.unit[inst as usize] * (1 + self.rng.below(3) as Price);
        let qty = 100 * (1 + self.rng.below(10));
        let m = &mut self.members[li];
        let side = if m.turn { Side::Buy } else { Side::Sell };
        m.turn = !m.turn;
        if !m.bucket.take(t) {
            return self.gw_reject(GwReject::Throttled);
        }
        let price = match side {
            Side::Buy => fair - off,
            Side::Sell => fair + off,
        };
        let new_id = m.next_id();
        let old = m.quotes[k][side as usize];
        if let Err(r) = self.pre_trade(li, new_id, inst, price, qty) {
            return self.gw_reject(r);
        }
        self.members[li].quotes[k][side as usize] = Some(new_id);
        Some(match old {
            Some(old_id) => Request::Replace {
                old_id,
                new_id,
                inst,
                side,
                price,
                qty,
                t,
            },
            None => Request::New {
                order_id: new_id,
                inst,
                side,
                price,
                qty,
                tif: TimeInForce::Gtc,
                t,
            },
        })
    }

    /// 經紀商：代客掛單、撤單，連續交易時偶爾送市價（以穿價 IOC 表示）。
    fn broker_action(&mut self, t: u64, phase: Phase) -> Option<Request> {
        let li = self.brokers[self.rng.below(self.brokers.len() as u64) as usize];
        if t < self.members[li].offline_until {
            return None;
        }
        let live = self.members[li].live.len();
        let cancel = live >= 200 || (live > 0 && self.rng.below(100) < 35);
        if !self.members[li].bucket.take(t) {
            return self.gw_reject(GwReject::Throttled);
        }
        if cancel {
            let m = &mut self.members[li];
            let id = m
                .live
                .swap_remove(self.rng.below(m.live.len() as u64) as usize);
            let inst = self.orders.get(&id)?.inst;
            return Some(Request::Cancel {
                order_id: id,
                inst,
                t,
            });
        }
        let inst = self.picker.pick(&mut self.rng) as InstrumentId;
        let side = if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let sign = if side == Side::Buy { 1 } else { -1 };
        let fair = self.fair(inst, t);
        let unit = self.unit[inst as usize];
        let mut qty = 100 * (1 + self.rng.below(10));
        let (mut price, tif) = if phase.is_call() {
            // 集合競價期間：有積極也有保守的價格，競價時才會有成交量。
            (
                fair + sign * unit * (self.rng.below(11) as Price - 5),
                TimeInForce::Gtc,
            )
        } else if self.rng.below(100) < 25 {
            (fair + sign * unit * 10, TimeInForce::Ioc)
        } else {
            (
                fair - sign * unit * (1 + self.rng.below(20) as Price),
                TimeInForce::Gtc,
            )
        };
        if self.rng.below(1_000_000) < self.cfg.fat_finger_ppm {
            // 肥手指：數量多打三個 0，或價格打錯一倍。
            if self.rng.below(2) == 0 {
                qty *= 1_000;
            } else {
                price *= 2;
            }
        }
        let id = self.members[li].next_id();
        if let Err(r) = self.pre_trade(li, id, inst, price, qty) {
            return self.gw_reject(r);
        }
        if tif == TimeInForce::Gtc {
            self.members[li].live.push(id);
        }
        Some(Request::New {
            order_id: id,
            inst,
            side,
            price,
            qty,
            tif,
            t,
        })
    }

    /// 自營商：連續交易時段以 IOC 吃掉對手價。
    fn prop_action(&mut self, t: u64, phase: Phase) -> Option<Request> {
        if phase != Phase::Continuous {
            return None;
        }
        let li = self.props[self.rng.below(self.props.len() as u64) as usize];
        if !self.members[li].bucket.take(t) {
            return self.gw_reject(GwReject::Throttled);
        }
        let inst = self.picker.pick(&mut self.rng) as InstrumentId;
        let side = if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let sign = if side == Side::Buy { 1 } else { -1 };
        let price = self.fair(inst, t) + sign * self.unit[inst as usize] * 5;
        let qty = 100 * (1 + self.rng.below(5));
        let id = self.members[li].next_id();
        if let Err(r) = self.pre_trade(li, id, inst, price, qty) {
            return self.gw_reject(r);
        }
        Some(Request::New {
            order_id: id,
            inst,
            side,
            price,
            qty,
            tif: TimeInForce::Ioc,
            t,
        })
    }

    fn send(&mut self, r: Request) {
        match r {
            Request::New { .. } => self.stats.sent_new += 1,
            Request::Cancel { .. } => self.stats.sent_cancel += 1,
            Request::Replace { .. } => self.stats.sent_replace += 1,
            _ => {}
        }
        self.push(r);
    }

    /// 佇列滿時一邊等一邊消化回報——否則會和撮合分區互相等待而死結。
    fn push(&mut self, mut r: Request) {
        loop {
            match self.io.to_seq.try_push(r) {
                Ok(()) => return,
                Err(back) => {
                    r = back;
                    if self.poll() == 0 {
                        std::hint::spin_loop();
                    }
                }
            }
        }
    }

    fn poll(&mut self) -> usize {
        let mut n = 0;
        for p in 0..self.io.from_part.len() {
            for _ in 0..256 {
                let Some(m) = self.io.from_part[p].try_pop() else {
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
            ExecKind::Rejected(r) => {
                *self.stats.rejects.entry(format!("{r:?}")).or_default() += 1;
                self.close(order_id);
            }
            ExecKind::Fill {
                side,
                price,
                qty,
                leaves,
            } => {
                self.stats.fills += 1;
                let o = self
                    .orders
                    .get_mut(&order_id)
                    .expect("fill for unknown order");
                o.leaves -= qty;
                debug_assert_eq!(o.leaves, leaves);
                let (li, inst, limit) = (o.li, o.inst, o.price);
                self.members[li].open_notional -= limit * qty as i64;
                let signed = if side == Side::Buy {
                    qty as i64
                } else {
                    -(qty as i64)
                };
                self.net_pos[inst as usize] += signed;
                self.cash -= signed as i128 * price as i128;
                if leaves == 0 {
                    self.close(order_id);
                }
            }
            ExecKind::Done { .. } => {
                self.stats.done += 1;
                self.close(order_id);
            }
            ExecKind::CancelRejected => self.stats.cancel_rejects += 1,
        }
    }

    /// 委託結束：釋放剩餘的信用占用，從會員的報價/掛單清單移除。
    fn close(&mut self, id: OrderId) {
        let Some(o) = self.orders.remove(&id) else {
            return;
        };
        let m = &mut self.members[o.li];
        m.open_notional -= o.price * o.leaves as i64;
        for q in m.quotes.iter_mut() {
            for s in q.iter_mut() {
                if *s == Some(id) {
                    *s = None;
                }
            }
        }
        if let Some(i) = m.live.iter().position(|x| *x == id) {
            m.live.swap_remove(i);
        }
    }

    fn finish(mut self) -> GatewayStats {
        let mut expected = vec![0i64; self.members.len()];
        for o in self.orders.values() {
            expected[o.li] += o.price * o.leaves as i64;
        }
        self.stats.credit_consistent = self
            .members
            .iter()
            .zip(&expected)
            .all(|(m, e)| m.open_notional == *e && m.open_notional >= 0);
        self.stats.open_orders = self.orders.len();
        self.stats.net_position = std::mem::take(&mut self.net_pos);
        self.stats.net_cash = self.cash;
        self.stats
    }
}
