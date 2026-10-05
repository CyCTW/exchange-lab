//! 行情發布執行緒：彙整所有撮合分片的成交與最佳買賣價。
//!
//! 真實系統會在這裡編碼成 ITCH/SBE 風格的二進位訊息、加序號、經 UDP multicast 發送；
//! 這裡只維護每個商品的統計，並量測「請求預定送出 → 行情發布」的延遲。

use super::msg::*;
use crate::histogram::Histogram;
use crate::idle::{Clock, Idle};
use crate::ring::Consumer;
use crate::types::*;

#[derive(Clone, Copy, Default, Debug)]
pub struct SymbolMd {
    pub trades: u64,
    pub volume: u64,
    pub last_price: Option<Price>,
    pub top_updates: u64,
    pub bid: Option<(Price, Qty)>,
    pub ask: Option<(Price, Qty)>,
}

pub struct MdStats {
    pub per_symbol: Vec<SymbolMd>,
    /// 觸發成交的請求預定送出時間 → 行情發布執行緒看到該成交。
    pub trade_latency: Histogram,
}

pub fn run(
    from_m: Vec<Consumer<MdMsg>>,
    symbols: usize,
    clock: Clock,
    idle: crate::idle::IdleKind,
) -> MdStats {
    let mut idle = Idle::new(idle);
    let mut st = MdStats {
        per_symbol: vec![SymbolMd::default(); symbols],
        trade_latency: Histogram::default(),
    };
    let mut from_m = from_m;
    let mut shutdowns = 0;
    while shutdowns < from_m.len() {
        let mut work = 0;
        for rx in from_m.iter_mut() {
            for _ in 0..512 {
                let Some(m) = rx.try_pop() else { break };
                work += 1;
                match m {
                    MdMsg::Trade {
                        symbol,
                        price,
                        qty,
                        t,
                        ..
                    } => {
                        st.trade_latency.record(clock.now_ns().saturating_sub(t));
                        let s = &mut st.per_symbol[symbol as usize];
                        s.trades += 1;
                        s.volume += qty;
                        s.last_price = Some(price);
                    }
                    MdMsg::Top {
                        symbol, bid, ask, ..
                    } => {
                        let s = &mut st.per_symbol[symbol as usize];
                        s.top_updates += 1;
                        s.bid = bid;
                        s.ask = ask;
                    }
                    MdMsg::Shutdown => shutdowns += 1,
                }
            }
        }
        if work == 0 {
            idle.idle();
        } else {
            idle.reset();
        }
    }
    st
}
