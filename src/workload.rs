//! 可重現的合成委託流：大量掛單/撤單、少量吃單，近似真實交易所的訊息組成
//! （多數市場的撤單數量遠大於成交數量）。

use crate::types::*;

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

pub struct WorkloadConfig {
    pub seed: u64,
    pub mid: Price,
    pub min_price: Price,
    pub max_price: Price,
    /// 活躍掛單數上限；超過就強制撤單，讓簿的大小維持穩定。
    pub max_live: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            seed: 42,
            mid: 32_768,
            min_price: 0,
            max_price: 65_535,
            max_live: 100_000,
        }
    }
}

/// 約 50% 新掛單、40% 撤單、10% 主動 IOC 吃單。
pub fn generate(n: usize, cfg: &WorkloadConfig) -> Vec<Command> {
    let mut rng = Rng::new(cfg.seed);
    let mut mid = cfg.mid;
    let mut live: Vec<OrderId> = Vec::with_capacity(cfg.max_live + 1);
    let mut next_id: OrderId = 1;
    let mut cmds = Vec::with_capacity(n);

    for _ in 0..n {
        let r = rng.below(100);
        let cmd = if live.len() >= cfg.max_live || ((50..90).contains(&r) && !live.is_empty()) {
            let i = rng.below(live.len() as u64) as usize;
            Command::Cancel {
                id: live.swap_remove(i),
            }
        } else if r < 90 {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let off = 1 + rng.below(20) as Price;
            let price = match side {
                Side::Buy => mid - off,
                Side::Sell => mid + off,
            };
            let id = next_id;
            next_id += 1;
            live.push(id);
            Command::NewOrder {
                id,
                side,
                price,
                qty: 1 + rng.below(100),
                tif: TimeInForce::Gtc,
            }
        } else {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = match side {
                Side::Buy => mid + 5,
                Side::Sell => mid - 5,
            };
            let id = next_id;
            next_id += 1;
            Command::NewOrder {
                id,
                side,
                price,
                qty: 1 + rng.below(200),
                tif: TimeInForce::Ioc,
            }
        };
        cmds.push(cmd);

        if rng.below(16) == 0 {
            mid = (mid + rng.below(3) as Price - 1)
                .clamp(cfg.min_price + 1_000, cfg.max_price - 1_000);
        }
    }
    cmds
}
