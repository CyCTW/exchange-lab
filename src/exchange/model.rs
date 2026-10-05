//! 傳統交易所的靜態設定：商品、會員、交易時段、分區（partition）放置規則。

use std::path::PathBuf;
use std::time::Duration;

use crate::idle::IdleKind;
use crate::types::*;
use crate::workload::{zipf_weights, PricePath, PriceSpec};

pub type MemberId = u16;
pub type InstrumentId = u16;

/// 訂單編號由 gateway 指派：高位是會員編號、低 40 位是該會員的流水號。
/// 任何元件拿到訂單編號就知道它屬於哪個會員、回報該送回哪個 gateway。
pub const ORDER_SEQ_BITS: u32 = 40;

#[inline]
pub fn make_order_id(member: MemberId, seq: u64) -> OrderId {
    ((member as u64) << ORDER_SEQ_BITS) | (seq & ((1 << ORDER_SEQ_BITS) - 1))
}

#[inline]
pub fn member_of(id: OrderId) -> MemberId {
    (id >> ORDER_SEQ_BITS) as MemberId
}

#[derive(Clone, Debug)]
pub struct Instrument {
    pub id: InstrumentId,
    pub symbol: String,
    /// 參考價（前一日收盤價）。漲跌幅限制與 gateway 的價格合理性檢查都以它為基準。
    pub reference: Price,
    /// 漲停 / 跌停價（靜態價格帶）。
    pub lower_limit: Price,
    pub upper_limit: Price,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberKind {
    /// 做市商：在指定商品上持續雙邊報價，以改單（cancel/replace）更新報價。訊息量最大。
    MarketMaker,
    /// 經紀商：代客下單，掛單、撤單、偶爾市價（IOC）。偶有「肥手指」錯誤委託。
    Broker,
    /// 自營/高頻：只在連續交易時段送 IOC 吃單。
    Prop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Phase {
    /// 盤前：接受委託、累積在簿上，不撮合。
    PreOpen = 0,
    /// 連續交易。
    Continuous = 1,
    /// 收盤前集合競價的累積期。
    PreClose = 2,
    /// 收盤：不再接受新委託。
    Closed = 3,
}

impl Phase {
    pub fn from_u8(x: u8) -> Phase {
        match x {
            0 => Phase::PreOpen,
            1 => Phase::Continuous,
            2 => Phase::PreClose,
            _ => Phase::Closed,
        }
    }
    pub fn is_call(self) -> bool {
        matches!(self, Phase::PreOpen | Phase::PreClose)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    /// 商品 i → 分區 i % P。
    Modulo,
    /// 依熱門程度貪婪分配到目前負載最輕的分區。
    Balanced,
}

#[derive(Clone, Debug)]
pub struct ExchangeConfig {
    pub gateways: usize,
    pub partitions: usize,
    pub members: usize,
    pub instruments: usize,
    /// 所有 gateway 合計的目標「動作」速率（每秒）。做市商改單一次動作送一則 Replace。
    pub rate: u64,
    /// 一個交易日壓縮成多長的模擬時間。
    pub duration: Duration,
    pub seed: u64,
    pub idle: IdleKind,
    pub pin: bool,
    pub ring_capacity: usize,
    pub max_orders_per_book: usize,
    pub placement: Placement,
    pub zipf_s: f64,
    /// 定序後的訊息寫到這個檔案；`None` 表示不寫 journal（也就無法做重播驗證）。
    pub journal: Option<PathBuf>,
    /// 每個會員 session 的限流（則/秒）。
    pub mm_msgs_per_sec: u64,
    pub member_msgs_per_sec: u64,
    /// Gateway 的事前風控。
    pub max_order_qty: Qty,
    /// 委託價偏離參考價超過這個百分比就拒絕（比漲跌幅更嚴的「價格合理性」檢查）。
    pub price_collar_pct: i64,
    pub credit_limit_mm: i64,
    pub credit_limit_broker: i64,
    pub credit_limit_prop: i64,
    /// 經紀商每百萬個動作中，發生肥手指（數量或價格離譜）的次數。
    pub fat_finger_ppm: u64,
    /// 盤中暫停最熱門商品一段時間（之後以集合競價恢復）。
    pub halt: bool,
    /// 盤中模擬一個會員斷線（觸發 cancel-on-disconnect）。
    pub disconnect: bool,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        ExchangeConfig {
            gateways: 2,
            partitions: 2,
            members: 60,
            instruments: 40,
            rate: 200_000,
            duration: Duration::from_secs(5),
            seed: 1,
            idle: IdleKind::Backoff,
            pin: false,
            ring_capacity: 1 << 14,
            max_orders_per_book: 1 << 17,
            placement: Placement::Balanced,
            zipf_s: 1.0,
            journal: None,
            mm_msgs_per_sec: 50_000,
            member_msgs_per_sec: 5_000,
            max_order_qty: 5_000,
            price_collar_pct: 7,
            credit_limit_mm: i64::MAX / 4,
            credit_limit_broker: 2_000_000_000,
            credit_limit_prop: 500_000_000,
            fat_finger_ppm: 500,
            halt: true,
            disconnect: true,
        }
    }
}

/// 交易日時程（以整個模擬時間的比例表示）。
pub const OPEN_AT: f64 = 0.15;
pub const PRECLOSE_AT: f64 = 0.85;
pub const CLOSE_AT: f64 = 0.95;
pub const HALT_AT: f64 = 0.45;
pub const RESUME_AT: f64 = 0.50;
pub const DISCONNECT_AT: f64 = 0.60;
pub const RECONNECT_AT: f64 = 0.65;

impl ExchangeConfig {
    pub fn market_makers(&self) -> usize {
        (self.members / 6).max(1)
    }
    pub fn props(&self) -> usize {
        (self.members / 6).max(1)
    }

    pub fn member_kind(&self, m: MemberId) -> MemberKind {
        let m = m as usize;
        if m < self.market_makers() {
            MemberKind::MarketMaker
        } else if m < self.market_makers() + self.props() {
            MemberKind::Prop
        } else {
            MemberKind::Broker
        }
    }

    pub fn credit_limit(&self, m: MemberId) -> i64 {
        match self.member_kind(m) {
            MemberKind::MarketMaker => self.credit_limit_mm,
            MemberKind::Broker => self.credit_limit_broker,
            MemberKind::Prop => self.credit_limit_prop,
        }
    }

    /// 每個商品有兩家做市商負責報價。
    pub fn mm_instruments(&self, m: MemberId) -> Vec<InstrumentId> {
        let n = self.market_makers();
        (0..self.instruments)
            .filter(|&i| i % n == m as usize || (i + 1) % n == m as usize)
            .map(|i| i as InstrumentId)
            .collect()
    }

    pub fn gateway_of(&self, m: MemberId) -> usize {
        m as usize % self.gateways
    }

    pub fn weights(&self) -> Vec<f64> {
        zipf_weights(self.instruments, self.zipf_s)
    }

    /// 商品 → 分區。
    pub fn placement_map(&self) -> Vec<usize> {
        match self.placement {
            Placement::Modulo => (0..self.instruments).map(|s| s % self.partitions).collect(),
            Placement::Balanced => {
                let w = self.weights();
                let mut order: Vec<usize> = (0..self.instruments).collect();
                order.sort_by(|a, b| w[*b].total_cmp(&w[*a]));
                let mut load = vec![0.0f64; self.partitions];
                let mut map = vec![0; self.instruments];
                for s in order {
                    let p = (0..self.partitions)
                        .min_by(|a, b| load[*a].total_cmp(&load[*b]))
                        .unwrap();
                    map[s] = p;
                    load[p] += w[s];
                }
                map
            }
        }
    }

    pub fn instruments_list(&self) -> Vec<Instrument> {
        (0..self.instruments)
            .map(|i| {
                // 參考價分散在 50 ~ 1,000 元，以 0.01 元為一個 tick。
                let x = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40;
                let reference = 5_000 + (x % 95_000) as Price;
                Instrument {
                    id: i as InstrumentId,
                    symbol: format!("{:04}", 1101 + i * 7),
                    reference,
                    lower_limit: reference * 9 / 10,
                    upper_limit: reference * 11 / 10,
                }
            })
            .collect()
    }

    /// 公允價在參考價 ±5% 內漫步。
    pub fn price_path(&self, insts: &[Instrument]) -> PricePath {
        let specs: Vec<PriceSpec> = insts
            .iter()
            .map(|s| PriceSpec {
                init: s.reference,
                lo: s.reference * 95 / 100,
                hi: s.reference * 105 / 100,
                step: (s.reference / 5_000).max(1),
            })
            .collect();
        PricePath::new(
            &specs,
            self.duration.as_millis() as usize + 1_000,
            self.seed,
        )
    }

    pub fn at(&self, frac: f64) -> u64 {
        (self.duration.as_nanos() as f64 * frac) as u64
    }

    /// 依時程判斷目前（相對開始時間 `elapsed_ns`）應處的階段。Gateway 用來決定送什麼單。
    pub fn phase_at(&self, elapsed_ns: u64) -> Phase {
        if elapsed_ns < self.at(OPEN_AT) {
            Phase::PreOpen
        } else if elapsed_ns < self.at(PRECLOSE_AT) {
            Phase::Continuous
        } else if elapsed_ns < self.at(CLOSE_AT) {
            Phase::PreClose
        } else {
            Phase::Closed
        }
    }
}
