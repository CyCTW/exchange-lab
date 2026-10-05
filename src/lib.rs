//! exchange-lab：研究高頻、低延遲、高吞吐交易所核心的實驗原型。
//!
//! 架構概念見 `docs/`；可執行的效能實驗見 `src/bin/bench.rs`。

pub mod affinity;
pub mod crypto;
pub mod exchange;
pub mod histogram;
pub mod idle;
pub mod journal;
pub mod orderbook;
pub mod ring;
pub mod types;
pub mod workload;

pub use orderbook::{BookConfig, OrderBook};
pub use types::*;
