//! 模擬市場的靜態設定：資產、商品、用戶類型、分片規則、價格路徑。

use std::time::Duration;

use super::idle::IdleKind;
use crate::types::*;
use crate::workload::Rng;

pub type UserId = u32;
pub type SymbolId = u16;
pub type AssetId = u16;

/// 資產 0 是計價幣（USDT），資產 i+1 是商品 i 的基礎幣。
pub const QUOTE_ASSET: AssetId = 0;

/// 訂單編號由 gateway 指派：高位是用戶編號、低 24 位是該用戶的流水號。
/// 好處：任何元件拿到訂單編號就知道它屬於哪個用戶，也就知道該送去哪個風控分片，
/// 不需要額外的查表或在撮合簿裡多存一個欄位。
pub const ORDER_SEQ_BITS: u32 = 24;

#[inline]
pub fn make_order_id(user: UserId, seq: u32) -> OrderId {
    ((user as u64) << ORDER_SEQ_BITS) | (seq as u64 & ((1 << ORDER_SEQ_BITS) - 1))
}

#[inline]
pub fn order_user(id: OrderId) -> UserId {
    (id >> ORDER_SEQ_BITS) as UserId
}

#[inline]
fn mix(u: UserId) -> u64 {
    let x = (u as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 31)
}

#[derive(Clone, Debug)]
pub struct SymbolSpec {
    pub id: SymbolId,
    pub name: String,
    pub base: AssetId,
    pub quote: AssetId,
    pub min_price: Price,
    pub max_price: Price,
    pub init_price: Price,
}

const NAMES: [&str; 12] = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "TRX", "LTC",
];

pub fn make_symbols(n: usize) -> Vec<SymbolSpec> {
    (0..n)
        .map(|i| {
            let init = (60_000 / (i as Price + 1)).max(1_000);
            let base = NAMES
                .get(i)
                .map(|s| s.to_string())
                .unwrap_or(format!("A{i}"));
            SymbolSpec {
                id: i as SymbolId,
                name: format!("{base}/USDT"),
                base: i as AssetId + 1,
                quote: QUOTE_ASSET,
                min_price: init / 2,
                max_price: init * 3 / 2,
                init_price: init,
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserKind {
    /// 做市商：在單一商品上持續雙邊報價、頻繁撤單重掛。訊息量最大。
    MarketMaker,
    /// 一般用戶：偶爾掛被動限價單、偶爾撤單。人數最多。
    Retail,
    /// 主動吃單者：送 IOC 吃掉對手價。
    Taker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    /// 商品 i → 撮合分片 i % M。簡單，但熱門商品可能擠在同一個分片。
    Modulo,
    /// 依熱門程度貪婪分配到目前負載最輕的分片。
    Balanced,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub gateways: usize,
    pub risk_shards: usize,
    pub matchers: usize,
    pub users: usize,
    pub symbols: usize,
    /// 所有 gateway 合計的目標「動作」速率（每秒）。做市商撤單重掛一次動作會送兩則訊息。
    pub rate: u64,
    pub duration: Duration,
    pub seed: u64,
    pub idle: IdleKind,
    pub pin: bool,
    pub ring_capacity: usize,
    pub max_orders_per_book: usize,
    pub placement: Placement,
    /// 商品熱門度的 Zipf 指數；0 = 均勻。
    pub zipf_s: f64,
    /// 每類用戶的限流（token bucket）：每秒訊息數。
    pub mm_msgs_per_sec: u64,
    pub user_msgs_per_sec: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            gateways: 2,
            risk_shards: 2,
            matchers: 2,
            users: 10_000,
            symbols: 8,
            rate: 200_000,
            duration: Duration::from_secs(5),
            seed: 1,
            idle: IdleKind::Backoff,
            pin: false,
            ring_capacity: 1 << 14,
            max_orders_per_book: 1 << 17,
            placement: Placement::Modulo,
            zipf_s: 1.0,
            mm_msgs_per_sec: 5_000,
            user_msgs_per_sec: 100,
        }
    }
}

impl SimConfig {
    pub fn market_makers(&self) -> usize {
        (self.symbols * 2).max(self.users / 100).min(self.users)
    }

    /// 用戶類型由編號決定，gateway 與風控兩邊用同一個函數，不需要互相溝通。
    /// 用雜湊而不是 `u % 10`：分片也是用 `u % N`，直接取餘數會讓某類用戶全擠在同一個分片。
    pub fn user_kind(&self, u: UserId) -> UserKind {
        if (u as usize) < self.market_makers() {
            UserKind::MarketMaker
        } else if mix(u).is_multiple_of(10) {
            UserKind::Taker
        } else {
            UserKind::Retail
        }
    }

    /// 做市商負責的商品：依熱門度分配，越熱門的商品做市商越多；每個商品至少一個。
    pub fn mm_symbol(&self, u: UserId) -> SymbolId {
        if (u as usize) < self.symbols {
            return u as SymbolId;
        }
        let w = self.zipf_weights();
        let total: f64 = w.iter().sum();
        let mut x = (mix(u) % 1_000_000) as f64 / 1e6 * total;
        for (i, wi) in w.iter().enumerate() {
            if x < *wi {
                return i as SymbolId;
            }
            x -= wi;
        }
        (self.symbols - 1) as SymbolId
    }

    pub fn gateway_of(&self, u: UserId) -> usize {
        u as usize % self.gateways
    }
    pub fn risk_of(&self, u: UserId) -> usize {
        u as usize % self.risk_shards
    }
    pub fn local_index(&self, u: UserId, shards: usize) -> usize {
        u as usize / shards
    }
    pub fn users_in_shard(&self, shard: usize, shards: usize) -> usize {
        (self.users + shards - 1 - shard) / shards
    }

    /// 初始資產：做市商資金雄厚；一般用戶資金有限，會遇到餘額不足被拒。
    pub fn initial_balance(&self, u: UserId, asset: AssetId) -> i64 {
        match (self.user_kind(u), asset) {
            (UserKind::MarketMaker, QUOTE_ASSET) => 1_000_000_000_000,
            (UserKind::MarketMaker, _) => 1_000_000_000,
            (_, QUOTE_ASSET) => 1_000_000,
            (_, _) => 20,
        }
    }

    pub fn zipf_weights(&self) -> Vec<f64> {
        (0..self.symbols)
            .map(|i| 1.0 / ((i + 1) as f64).powf(self.zipf_s))
            .collect()
    }

    /// 商品 → 撮合分片。
    pub fn placement_map(&self) -> Vec<usize> {
        match self.placement {
            Placement::Modulo => (0..self.symbols).map(|s| s % self.matchers).collect(),
            Placement::Balanced => {
                let w = self.zipf_weights();
                let mut order: Vec<usize> = (0..self.symbols).collect();
                order.sort_by(|a, b| w[*b].total_cmp(&w[*a]));
                let mut load = vec![0.0f64; self.matchers];
                let mut map = vec![0; self.symbols];
                for s in order {
                    let m = (0..self.matchers)
                        .min_by(|a, b| load[*a].total_cmp(&load[*b]))
                        .unwrap();
                    map[s] = m;
                    load[m] += w[s];
                }
                map
            }
        }
    }
}

/// 以累積權重表抽樣（Zipf：少數商品吃掉大部分流量）。
pub struct WeightedPicker {
    cumulative: Vec<u64>,
}

impl WeightedPicker {
    pub fn new(weights: &[f64]) -> Self {
        let total: f64 = weights.iter().sum();
        let mut acc = 0u64;
        let cumulative = weights
            .iter()
            .map(|w| {
                acc += ((w / total) * 1e9).max(1.0) as u64;
                acc
            })
            .collect();
        WeightedPicker { cumulative }
    }
    #[inline]
    pub fn pick(&self, rng: &mut Rng) -> usize {
        let x = rng.below(*self.cumulative.last().unwrap());
        self.cumulative.partition_point(|&c| c <= x)
    }
}

/// 每個商品的「公允價」隨機漫步，每毫秒一步。所有 gateway 共用同一條路徑，
/// 讓不同 gateway 上的用戶對價格有一致的認知。
pub struct PricePath {
    steps: Vec<Vec<Price>>,
}

impl PricePath {
    pub fn new(symbols: &[SymbolSpec], millis: usize, seed: u64) -> Self {
        let steps = symbols
            .iter()
            .map(|s| {
                let mut rng = Rng::new(seed ^ (0x9E37 + s.id as u64 * 7919));
                let band = s.max_price - s.min_price;
                let (lo, hi) = (s.min_price + band / 5, s.max_price - band / 5);
                let step = (s.init_price / 5_000).max(1);
                let mut p = s.init_price;
                (0..millis)
                    .map(|_| {
                        p = (p + (rng.below(3) as Price - 1) * step).clamp(lo, hi);
                        p
                    })
                    .collect()
            })
            .collect();
        PricePath { steps }
    }

    #[inline]
    pub fn at(&self, symbol: SymbolId, elapsed_ns: u64) -> Price {
        let v = &self.steps[symbol as usize];
        v[((elapsed_ns / 1_000_000) as usize).min(v.len() - 1)]
    }
}
