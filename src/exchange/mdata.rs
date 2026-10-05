//! 行情接收端：像真實的行情客戶端一樣，只靠各分區頻道的逐筆（L3）事件重建完整限價簿。
//!
//! 結束時把重建出來的簿和撮合分區內部的簿逐筆比對——這驗證了行情流「完整且正確」：
//! 只要漏掉、重複或算錯任何一則 Add / Executed / Deleted，比對就會失敗。

use std::collections::{BTreeMap, HashMap};

use super::model::*;
use super::msg::*;
use crate::histogram::Histogram;
use crate::idle::{Clock, Idle};
use crate::ring::Consumer;
use crate::types::*;

#[derive(Default)]
struct L3Book {
    /// key = (side, 價格排序鍵, 到達順序)：買方價格由高到低、賣方由低到高，同價依到達順序。
    queue: BTreeMap<(u8, i64, u64), (OrderId, Qty)>,
    index: HashMap<OrderId, (u8, i64, u64)>,
}

impl L3Book {
    fn add(&mut self, id: OrderId, side: Side, price: Price, qty: Qty, arrival: u64) {
        let key = (
            side as u8,
            if side == Side::Buy { -price } else { price },
            arrival,
        );
        self.queue.insert(key, (id, qty));
        self.index.insert(id, key);
    }
    fn execute(&mut self, id: OrderId, qty: Qty) -> bool {
        let Some(key) = self.index.get(&id).copied() else {
            return false;
        };
        let e = self.queue.get_mut(&key).unwrap();
        if e.1 < qty {
            return false;
        }
        e.1 -= qty;
        if e.1 == 0 {
            self.queue.remove(&key);
            self.index.remove(&id);
        }
        true
    }
    fn delete(&mut self, id: OrderId) -> bool {
        match self.index.remove(&id) {
            Some(key) => self.queue.remove(&key).is_some(),
            None => false,
        }
    }
    fn snapshot(&self) -> Vec<(Side, Price, OrderId, Qty)> {
        self.queue
            .iter()
            .map(|(&(s, pk, _), &(id, q))| {
                if s == 0 {
                    (Side::Buy, -pk, id, q)
                } else {
                    (Side::Sell, pk, id, q)
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct InstrumentMd {
    pub trades: u64,
    pub volume: u64,
    pub last: Option<Price>,
    pub open: Option<(Price, Qty)>,
    pub close: Option<(Price, Qty)>,
    pub reopen: Option<(Price, Qty)>,
    pub halts: u32,
}

pub struct MdStats {
    pub per_inst: Vec<InstrumentMd>,
    pub messages: u64,
    pub channel_msgs: Vec<u64>,
    /// 序號跳號或重複（應為 0）。
    pub gaps: u64,
    /// 無法套用的事件（例如 Executed 一張不存在的委託；應為 0）。
    pub bad_events: u64,
    pub phases_seen: Vec<Phase>,
    /// 請求預定送出時間 → 行情接收端看到連續交易的成交。
    pub trade_latency: Histogram,
    pub books: Vec<Vec<(Side, Price, OrderId, Qty)>>,
}

pub fn run(
    from_part: Vec<Consumer<MdMsg>>,
    instruments: usize,
    clock: Clock,
    idle: crate::idle::IdleKind,
) -> MdStats {
    let mut idle = Idle::new(idle);
    let mut from_part = from_part;
    let mut books: Vec<L3Book> = (0..instruments).map(|_| L3Book::default()).collect();
    let mut st = MdStats {
        per_inst: vec![InstrumentMd::default(); instruments],
        messages: 0,
        channel_msgs: vec![0; from_part.len()],
        gaps: 0,
        bad_events: 0,
        phases_seen: vec![],
        trade_latency: Histogram::default(),
        books: vec![],
    };
    let mut expected = vec![1u64; from_part.len()];
    let mut arrival = 0u64;
    let mut shutdowns = 0;
    while shutdowns < from_part.len() {
        let mut work = 0;
        for (ch, rx) in from_part.iter_mut().enumerate() {
            for _ in 0..512 {
                let Some(m) = rx.try_pop() else { break };
                work += 1;
                let MdMsg::Event { chan_seq, body } = m else {
                    shutdowns += 1;
                    continue;
                };
                st.messages += 1;
                st.channel_msgs[ch] += 1;
                if chan_seq != expected[ch] {
                    st.gaps += 1;
                }
                expected[ch] = chan_seq + 1;
                let ok = match body {
                    MdBody::Add {
                        inst,
                        id,
                        side,
                        price,
                        qty,
                    } => {
                        arrival += 1;
                        books[inst as usize].add(id, side, price, qty, arrival);
                        true
                    }
                    MdBody::Executed { inst, id, qty } => books[inst as usize].execute(id, qty),
                    MdBody::Deleted { inst, id } => books[inst as usize].delete(id),
                    MdBody::Trade {
                        inst,
                        price,
                        qty,
                        auction,
                        t,
                    } => {
                        if !auction {
                            st.trade_latency.record(clock.now_ns().saturating_sub(t));
                        }
                        let s = &mut st.per_inst[inst as usize];
                        s.trades += 1;
                        s.volume += qty;
                        s.last = Some(price);
                        true
                    }
                    MdBody::AuctionResult {
                        inst,
                        price,
                        qty,
                        phase,
                    } => {
                        let s = &mut st.per_inst[inst as usize];
                        match phase {
                            Phase::PreOpen => s.open = Some((price, qty)),
                            Phase::Closed => s.close = Some((price, qty)),
                            _ => s.reopen = Some((price, qty)),
                        }
                        true
                    }
                    MdBody::Phase(p) => {
                        if ch == 0 {
                            st.phases_seen.push(p);
                        }
                        true
                    }
                    MdBody::Halt(i) => {
                        st.per_inst[i as usize].halts += 1;
                        true
                    }
                    MdBody::Resume(_) => true,
                };
                if !ok {
                    st.bad_events += 1;
                }
            }
        }
        if work == 0 {
            idle.idle();
        } else {
            idle.reset();
        }
    }
    st.books = books.iter().map(|b| b.snapshot()).collect();
    st
}
