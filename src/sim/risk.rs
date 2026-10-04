//! 風控/帳戶分片執行緒：依「用戶」分片，擁有該分片所有用戶的餘額。
//!
//! - 新單：檢查並凍結資金（買單凍結 價格×數量 的計價幣，賣單凍結數量的基礎幣），
//!   通過後送到該商品所屬的撮合分片。
//! - 結算：撮合分片回送成交/取消，在這裡解凍、轉帳，並產生執行回報給 gateway。
//!
//! 和撮合引擎一樣，整個分片的狀態只由這一條執行緒擁有，沒有鎖。

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use super::idle::Idle;
use super::model::*;
use super::msg::*;
use crate::orderbook::IdHasher;
use crate::ring::{Consumer, Producer};
use crate::types::*;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Balance {
    pub available: i64,
    pub locked: i64,
}

#[derive(Clone, Copy, Debug)]
struct OrderRec {
    symbol: SymbolId,
    side: Side,
    limit: Price,
    remaining: Qty,
}

pub struct RiskIo {
    pub from_gw: Vec<Consumer<ToRisk>>,
    pub from_m: Vec<Consumer<Settle>>,
    pub to_m: Vec<Producer<ToMatcher>>,
    pub to_gw: Vec<Producer<ToGateway>>,
}

#[derive(Default, Debug)]
pub struct RiskStats {
    pub id: usize,
    pub users: usize,
    pub requests: u64,
    pub settlements: u64,
    pub rejected_balance: u64,
    pub forwarded: u64,
}

/// 結束時交回給主執行緒做一致性檢查的狀態。
pub struct RiskFinal {
    pub stats: RiskStats,
    /// `balances[local_user * n_assets + asset]`
    pub balances: Vec<Balance>,
    pub n_assets: usize,
    pub open_orders: usize,
    /// 依未成交訂單重新計算的「應凍結金額」是否與帳上 locked 完全相符。
    pub locks_consistent: bool,
}

pub struct RiskShard {
    cfg: Arc<SimConfig>,
    symbols: Arc<Vec<SymbolSpec>>,
    placement: Arc<Vec<usize>>,
    n_assets: usize,
    balances: Vec<Balance>,
    orders: HashMap<OrderId, OrderRec, BuildHasherDefault<IdHasher>>,
    io: RiskIo,
    idle: Idle,
    stats: RiskStats,
    gw_shutdowns: usize,
    m_shutdowns: usize,
}

impl RiskShard {
    pub fn new(
        id: usize,
        cfg: Arc<SimConfig>,
        symbols: Arc<Vec<SymbolSpec>>,
        placement: Arc<Vec<usize>>,
        io: RiskIo,
    ) -> Self {
        let n_assets = symbols.len() + 1;
        let n_users = cfg.users_in_shard(id, cfg.risk_shards);
        let mut balances = vec![Balance::default(); n_users * n_assets];
        for li in 0..n_users {
            let u = (li * cfg.risk_shards + id) as UserId;
            for a in 0..n_assets {
                balances[li * n_assets + a].available = cfg.initial_balance(u, a as AssetId);
            }
        }
        RiskShard {
            idle: Idle::new(cfg.idle),
            stats: RiskStats {
                id,
                users: n_users,
                ..Default::default()
            },
            orders: HashMap::with_capacity_and_hasher(1 << 16, Default::default()),
            cfg,
            symbols,
            placement,
            n_assets,
            balances,
            io,
            gw_shutdowns: 0,
            m_shutdowns: 0,
        }
    }

    pub fn run(mut self) -> RiskFinal {
        while self.m_shutdowns < self.cfg.matchers {
            // 先處理結算回饋：它會釋放資金，也會解除撮合分片的背壓。
            let mut work = self.drain_settlements();
            for g in 0..self.io.from_gw.len() {
                for _ in 0..256 {
                    let Some(m) = self.io.from_gw[g].try_pop() else {
                        break;
                    };
                    work += 1;
                    self.on_request(m);
                }
            }
            if work == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
        self.finish()
    }

    #[inline]
    fn bal(&mut self, user: UserId, asset: AssetId) -> &mut Balance {
        let li = self.cfg.local_index(user, self.cfg.risk_shards);
        &mut self.balances[li * self.n_assets + asset as usize]
    }

    /// 這筆訂單要凍結的資產與數量。
    fn lock_for(&self, symbol: SymbolId, side: Side, price: Price, qty: Qty) -> (AssetId, i64) {
        let s = &self.symbols[symbol as usize];
        match side {
            Side::Buy => (s.quote, price * qty as i64),
            Side::Sell => (s.base, qty as i64),
        }
    }

    fn on_request(&mut self, m: ToRisk) {
        match m {
            ToRisk::New {
                order_id,
                symbol,
                side,
                price,
                qty,
                tif,
                t,
            } => {
                self.stats.requests += 1;
                let user = order_user(order_id);
                if symbol as usize >= self.symbols.len() || qty == 0 || price <= 0 {
                    return self.report(
                        order_id,
                        ExecKind::Rejected(RejectCode::InvalidOrder),
                        true,
                        t,
                    );
                }
                let (asset, amount) = self.lock_for(symbol, side, price, qty);
                let b = self.bal(user, asset);
                if b.available < amount {
                    self.stats.rejected_balance += 1;
                    return self.report(
                        order_id,
                        ExecKind::Rejected(RejectCode::InsufficientBalance),
                        true,
                        t,
                    );
                }
                b.available -= amount;
                b.locked += amount;
                self.orders.insert(
                    order_id,
                    OrderRec {
                        symbol,
                        side,
                        limit: price,
                        remaining: qty,
                    },
                );
                self.stats.forwarded += 1;
                let m = self.placement[symbol as usize];
                self.send_to_matcher(
                    m,
                    ToMatcher::New {
                        order_id,
                        symbol,
                        side,
                        price,
                        qty,
                        tif,
                        t,
                    },
                );
            }
            ToRisk::Cancel { order_id, t } => {
                self.stats.requests += 1;
                // 訂單編號裡帶有用戶編號，且只會從該用戶的 gateway 進來，等同於做了擁有者檢查。
                match self.orders.get(&order_id) {
                    Some(rec) => {
                        let symbol = rec.symbol;
                        self.stats.forwarded += 1;
                        self.send_to_matcher(
                            self.placement[symbol as usize],
                            ToMatcher::Cancel {
                                order_id,
                                symbol,
                                t,
                            },
                        );
                    }
                    None => self.report(order_id, ExecKind::CancelRejected, true, t),
                }
            }
            ToRisk::Shutdown => {
                self.gw_shutdowns += 1;
                if self.gw_shutdowns == self.cfg.gateways {
                    for m in 0..self.cfg.matchers {
                        self.send_to_matcher(m, ToMatcher::Shutdown);
                    }
                }
            }
        }
    }

    fn drain_settlements(&mut self) -> usize {
        let mut n = 0;
        for m in 0..self.io.from_m.len() {
            for _ in 0..256 {
                let Some(s) = self.io.from_m[m].try_pop() else {
                    break;
                };
                n += 1;
                self.on_settle(s);
            }
        }
        n
    }

    fn release(&mut self, order_id: OrderId, rec: &OrderRec, qty: Qty) {
        let (asset, amount) = self.lock_for(rec.symbol, rec.side, rec.limit, qty);
        let b = self.bal(order_user(order_id), asset);
        b.locked -= amount;
        b.available += amount;
    }

    fn on_settle(&mut self, s: Settle) {
        self.stats.settlements += 1;
        match s {
            Settle::Ack { order_id, t } => self.report(order_id, ExecKind::Ack, true, t),
            Settle::Fill {
                order_id,
                price,
                qty,
                maker,
                t,
            } => {
                let user = order_user(order_id);
                let rec = self
                    .orders
                    .get_mut(&order_id)
                    .expect("fill for unknown order");
                rec.remaining -= qty;
                let rec = *rec;
                if rec.remaining == 0 {
                    self.orders.remove(&order_id);
                }
                let spec = &self.symbols[rec.symbol as usize];
                let (base, quote) = (spec.base, spec.quote);
                let q = qty as i64;
                match rec.side {
                    Side::Buy => {
                        // 以限價凍結、以成交價付款，價差退回可用餘額。
                        let b = self.bal(user, quote);
                        b.locked -= rec.limit * q;
                        b.available += (rec.limit - price) * q;
                        self.bal(user, base).available += q;
                    }
                    Side::Sell => {
                        self.bal(user, base).locked -= q;
                        self.bal(user, quote).available += price * q;
                    }
                }
                self.report(
                    order_id,
                    ExecKind::Fill {
                        price,
                        qty,
                        leaves: rec.remaining,
                        maker,
                    },
                    false,
                    t,
                );
            }
            Settle::Done {
                order_id,
                remaining,
                response,
                t,
            } => {
                let rec = self
                    .orders
                    .remove(&order_id)
                    .expect("done for unknown order");
                debug_assert_eq!(rec.remaining, remaining);
                self.release(order_id, &rec, rec.remaining);
                self.report(order_id, ExecKind::Done { remaining }, response, t);
            }
            Settle::Rejected {
                order_id,
                reason,
                t,
            } => {
                let rec = self
                    .orders
                    .remove(&order_id)
                    .expect("reject for unknown order");
                self.release(order_id, &rec, rec.remaining);
                self.report(
                    order_id,
                    ExecKind::Rejected(RejectCode::from_matcher(reason)),
                    true,
                    t,
                );
            }
            Settle::CancelReject { order_id, t } => {
                self.report(order_id, ExecKind::CancelRejected, true, t)
            }
            Settle::Shutdown => {
                self.m_shutdowns += 1;
                if self.m_shutdowns == self.cfg.matchers {
                    for g in 0..self.cfg.gateways {
                        self.push_gw(g, ToGateway::Shutdown);
                    }
                }
            }
        }
    }

    fn report(&mut self, order_id: OrderId, kind: ExecKind, response: bool, t: u64) {
        let g = self.cfg.gateway_of(order_user(order_id));
        self.push_gw(
            g,
            ToGateway::Report {
                order_id,
                kind,
                response,
                t,
            },
        );
    }

    /// Gateway 在送不出去時也會消化回報，所以這裡單純等待即可。
    fn push_gw(&mut self, g: usize, mut m: ToGateway) {
        while let Err(back) = self.io.to_gw[g].try_push(m) {
            m = back;
            std::hint::spin_loop();
        }
    }

    /// 撮合分片的輸入佇列滿了：一邊等一邊處理結算回饋。
    /// 若單純忙等，撮合分片可能正卡在「送結算給我」而我卡在「送訂單給它」→ 死結。
    fn send_to_matcher(&mut self, m: usize, mut msg: ToMatcher) {
        loop {
            match self.io.to_m[m].try_push(msg) {
                Ok(()) => return,
                Err(back) => {
                    msg = back;
                    if self.drain_settlements() == 0 {
                        std::hint::spin_loop();
                    }
                }
            }
        }
    }

    fn finish(self) -> RiskFinal {
        let mut expected = vec![0i64; self.balances.len()];
        for (&order_id, rec) in &self.orders {
            let (asset, amount) = self.lock_for(rec.symbol, rec.side, rec.limit, rec.remaining);
            let li = self
                .cfg
                .local_index(order_user(order_id), self.cfg.risk_shards);
            expected[li * self.n_assets + asset as usize] += amount;
        }
        let locks_consistent = self
            .balances
            .iter()
            .zip(&expected)
            .all(|(b, e)| b.locked == *e && b.available >= 0);
        RiskFinal {
            open_orders: self.orders.len(),
            stats: self.stats,
            balances: self.balances,
            n_assets: self.n_assets,
            locks_consistent,
        }
    }
}
