//! 多用戶、多商品的交易所模擬：gateway / 風控（依用戶分片）/ 撮合（依商品分片）/ 行情，
//! 每個元件是一條執行緒，彼此只透過 SPSC ring 溝通。設計說明見 docs/07-simulation-architecture.md。

pub mod gateway;
pub mod idle;
pub mod matcher;
pub mod mdata;
pub mod model;
pub mod msg;
pub mod risk;
pub mod run;

pub use model::{Placement, SimConfig};
pub use run::{run, SimReport};
