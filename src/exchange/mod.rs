//! 傳統交易所模型（本 lab 的主線）：會員經 gateway 事前風控 → 中央定序器 → 依商品分區撮合 →
//! L3 行情；定序後的訊息寫 journal，可離線重播驗證確定性。
//! 交易日包含盤前、開盤集合競價、連續交易、盤中暫停與恢復、收盤集合競價。
//! 設計說明見 docs/07-traditional-architecture.md。

pub mod gateway;
pub mod mdata;
pub mod model;
pub mod msg;
pub mod partition;
pub mod run;
pub mod sequencer;

pub use model::{ExchangeConfig, Placement};
pub use run::{replay, run, ExchangeReport};
