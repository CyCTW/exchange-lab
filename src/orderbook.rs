//! 單執行緒、價格-時間優先（price-time priority / FIFO）的限價簿。
//!
//! 設計重點（對應 docs/03-matching-engine.md）：
//! - 價格層用「以 tick 為索引的陣列」，O(1) 定位價格層，不用樹。
//! - 每個價格層是一條以 `u32` 索引串起來的侵入式雙向鏈結串列（FIFO）。
//! - 委託單放在預先配置的 slab（`Vec<Order>` + free list），熱路徑上不做 heap 配置。
//! - 用 bitmap 記錄「哪些價格層非空」，最佳價格層清空時，
//!   以 `trailing_zeros` / `leading_zeros` 一次跳過 64 個空層找下一個最佳價。
//! - 沒有鎖、沒有原子操作：整個簿只被一條執行緒擁有。

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::types::*;

const NIL: u32 = u32::MAX;

/// 給 `u64` 訂單編號用的快速雜湊。預設的 SipHash 能抵抗 HashDoS，
/// 但對交易所自己分配的編號來說太慢。
#[derive(Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(8) ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    fn write_u64(&mut self, n: u64) {
        let x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = x ^ (x >> 32);
    }
}

type IdMap = HashMap<OrderId, u32, BuildHasherDefault<IdHasher>>;

#[derive(Clone, Copy, Debug)]
pub struct BookConfig {
    pub min_price: Price,
    pub max_price: Price,
    /// 簿上最多能同時掛多少張單（slab 大小）。
    pub max_orders: usize,
}

impl Default for BookConfig {
    fn default() -> Self {
        BookConfig {
            min_price: 0,
            max_price: 65_535,
            max_orders: 1 << 20,
        }
    }
}

#[derive(Clone, Copy)]
struct Order {
    id: OrderId,
    qty: Qty,
    level: u32,
    side: Side,
    prev: u32,
    next: u32,
}

#[derive(Clone, Copy)]
struct Level {
    head: u32,
    tail: u32,
    qty: Qty,
    count: u32,
}

const EMPTY_LEVEL: Level = Level {
    head: NIL,
    tail: NIL,
    qty: 0,
    count: 0,
};

struct LevelBitmap {
    words: Vec<u64>,
}

impl LevelBitmap {
    fn new(n: usize) -> Self {
        LevelBitmap {
            words: vec![0; n.div_ceil(64)],
        }
    }
    #[inline]
    fn set(&mut self, i: u32) {
        self.words[(i >> 6) as usize] |= 1 << (i & 63);
    }
    #[inline]
    fn clear(&mut self, i: u32) {
        self.words[(i >> 6) as usize] &= !(1 << (i & 63));
    }
    /// 回傳 >= i 的第一個非空層。
    fn next_at_or_above(&self, i: u32) -> u32 {
        let mut w = (i >> 6) as usize;
        if w >= self.words.len() {
            return NIL;
        }
        let mut bits = self.words[w] & (!0u64 << (i & 63));
        loop {
            if bits != 0 {
                return (w as u32) * 64 + bits.trailing_zeros();
            }
            w += 1;
            if w == self.words.len() {
                return NIL;
            }
            bits = self.words[w];
        }
    }
    /// 回傳 <= i 的第一個非空層。
    fn next_at_or_below(&self, i: u32) -> u32 {
        let mut w = (i >> 6) as usize;
        let b = i & 63;
        let mask = if b == 63 { !0 } else { (1u64 << (b + 1)) - 1 };
        let mut bits = self.words[w] & mask;
        loop {
            if bits != 0 {
                return (w as u32) * 64 + 63 - bits.leading_zeros();
            }
            if w == 0 {
                return NIL;
            }
            w -= 1;
            bits = self.words[w];
        }
    }
}

struct BookSide {
    levels: Vec<Level>,
    bitmap: LevelBitmap,
    /// 最佳價格層的索引；`NIL` 表示這一側是空的。
    best: u32,
}

impl BookSide {
    fn new(n: usize) -> Self {
        BookSide {
            levels: vec![EMPTY_LEVEL; n],
            bitmap: LevelBitmap::new(n),
            best: NIL,
        }
    }
}

pub struct OrderBook {
    min_price: Price,
    max_price: Price,
    max_orders: usize,
    bids: BookSide,
    asks: BookSide,
    orders: Vec<Order>,
    free: Vec<u32>,
    index: IdMap,
    /// 集合競價（call auction）模式：新委託只掛上簿、不撮合，直到 `uncross`。
    auction: bool,
}

impl OrderBook {
    pub fn new(cfg: BookConfig) -> Self {
        assert!(cfg.max_price >= cfg.min_price);
        assert!(cfg.max_orders < NIL as usize);
        let n = (cfg.max_price - cfg.min_price + 1) as usize;
        OrderBook {
            min_price: cfg.min_price,
            max_price: cfg.max_price,
            max_orders: cfg.max_orders,
            bids: BookSide::new(n),
            asks: BookSide::new(n),
            orders: Vec::with_capacity(cfg.max_orders),
            free: Vec::with_capacity(cfg.max_orders),
            index: IdMap::with_capacity_and_hasher(cfg.max_orders, Default::default()),
            auction: false,
        }
    }

    /// 處理一筆指令，透過 `out` 回呼同步輸出事件（不配置記憶體、不排隊）。
    #[inline]
    pub fn execute(&mut self, cmd: &Command, out: &mut impl FnMut(Event)) {
        match *cmd {
            Command::NewOrder {
                id,
                side,
                price,
                qty,
                tif,
            } => self.new_order(id, side, price, qty, tif, out),
            Command::Cancel { id } => self.cancel(id, out),
        }
    }

    fn new_order(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        out: &mut impl FnMut(Event),
    ) {
        let reject = |reason| Event::Rejected { id, reason };
        if qty == 0 {
            return out(reject(RejectReason::InvalidQty));
        }
        if price < self.min_price || price > self.max_price {
            return out(reject(RejectReason::PriceOutOfBand));
        }
        if self.index.contains_key(&id) {
            return out(reject(RejectReason::DuplicateId));
        }
        if self.auction && tif == TimeInForce::Ioc {
            return out(reject(RejectReason::InvalidPhase));
        }
        // 撮合只會釋放 slot，所以事先檢查容量是保守而安全的。
        if tif == TimeInForce::Gtc && self.free.is_empty() && self.orders.len() == self.max_orders {
            return out(reject(RejectReason::BookFull));
        }
        out(Event::Accepted { id });

        let idx = (price - self.min_price) as u32;
        let remaining = if self.auction {
            qty
        } else {
            self.match_order(id, side, idx, qty, out)
        };
        if remaining == 0 {
            return;
        }
        match tif {
            TimeInForce::Ioc => out(Event::Cancelled { id, remaining }),
            TimeInForce::Gtc => self.rest(id, side, idx, remaining),
        }
    }

    /// 對手方從最佳價開始逐層、層內依時間先後吃單，回傳未成交量。
    fn match_order(
        &mut self,
        taker: OrderId,
        taker_side: Side,
        limit: u32,
        mut remaining: Qty,
        out: &mut impl FnMut(Event),
    ) -> Qty {
        let OrderBook {
            min_price,
            bids,
            asks,
            orders,
            free,
            index,
            ..
        } = self;
        let book = match taker_side {
            Side::Buy => asks,
            Side::Sell => bids,
        };

        while remaining > 0 {
            let best = book.best;
            if best == NIL {
                break;
            }
            let crosses = match taker_side {
                Side::Buy => best <= limit,
                Side::Sell => best >= limit,
            };
            if !crosses {
                break;
            }

            let price = *min_price + best as Price;
            let level = &mut book.levels[best as usize];
            let mut slot = level.head;
            while remaining > 0 && slot != NIL {
                let maker = &mut orders[slot as usize];
                let fill = maker.qty.min(remaining);
                maker.qty -= fill;
                remaining -= fill;
                level.qty -= fill;
                out(Event::Trade {
                    taker,
                    maker: maker.id,
                    taker_side,
                    price,
                    qty: fill,
                });
                let next = maker.next;
                if maker.qty == 0 {
                    // 完全成交的 maker 一定是隊首，直接 pop。
                    index.remove(&maker.id);
                    level.head = next;
                    if next == NIL {
                        level.tail = NIL;
                    } else {
                        orders[next as usize].prev = NIL;
                    }
                    level.count -= 1;
                    free.push(slot);
                }
                slot = next;
            }

            if level.count == 0 {
                book.bitmap.clear(best);
                book.best = match taker_side {
                    Side::Buy if (best as usize) + 1 < book.levels.len() => {
                        book.bitmap.next_at_or_above(best + 1)
                    }
                    Side::Sell if best > 0 => book.bitmap.next_at_or_below(best - 1),
                    _ => NIL,
                };
            }
        }
        remaining
    }

    fn rest(&mut self, id: OrderId, side: Side, idx: u32, qty: Qty) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = &mut book.levels[idx as usize];
        let node = Order {
            id,
            qty,
            level: idx,
            side,
            prev: level.tail,
            next: NIL,
        };
        let slot = match self.free.pop() {
            Some(s) => {
                self.orders[s as usize] = node;
                s
            }
            None => {
                self.orders.push(node);
                (self.orders.len() - 1) as u32
            }
        };
        if level.tail == NIL {
            level.head = slot;
        } else {
            self.orders[level.tail as usize].next = slot;
        }
        level.tail = slot;
        level.qty += qty;
        level.count += 1;
        if level.count == 1 {
            book.bitmap.set(idx);
            let better = match side {
                Side::Buy => book.best == NIL || idx > book.best,
                Side::Sell => book.best == NIL || idx < book.best,
            };
            if better {
                book.best = idx;
            }
        }
        self.index.insert(id, slot);
    }

    fn cancel(&mut self, id: OrderId, out: &mut impl FnMut(Event)) {
        let Some(slot) = self.index.remove(&id) else {
            return out(Event::Rejected {
                id,
                reason: RejectReason::UnknownOrder,
            });
        };
        let o = self.orders[slot as usize];
        if o.prev != NIL {
            self.orders[o.prev as usize].next = o.next;
        }
        if o.next != NIL {
            self.orders[o.next as usize].prev = o.prev;
        }
        let book = match o.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = &mut book.levels[o.level as usize];
        if o.prev == NIL {
            level.head = o.next;
        }
        if o.next == NIL {
            level.tail = o.prev;
        }
        level.qty -= o.qty;
        level.count -= 1;
        self.free.push(slot);

        if level.count == 0 {
            book.bitmap.clear(o.level);
            if book.best == o.level {
                book.best = match o.side {
                    Side::Buy if o.level > 0 => book.bitmap.next_at_or_below(o.level - 1),
                    Side::Sell if (o.level as usize) + 1 < book.levels.len() => {
                        book.bitmap.next_at_or_above(o.level + 1)
                    }
                    _ => NIL,
                };
            }
        }
        out(Event::Cancelled {
            id,
            remaining: o.qty,
        });
    }

    pub fn set_auction(&mut self, on: bool) {
        debug_assert!(
            on || !self.is_crossed(),
            "leaving auction with a crossed book; uncross first"
        );
        self.auction = on;
    }

    pub fn in_auction(&self) -> bool {
        self.auction
    }

    pub fn contains(&self, id: OrderId) -> bool {
        self.index.contains_key(&id)
    }

    /// 掛單目前的剩餘數量；不在簿上則為 `None`。
    pub fn order_qty(&self, id: OrderId) -> Option<Qty> {
        self.index.get(&id).map(|&s| self.orders[s as usize].qty)
    }

    fn is_crossed(&self) -> bool {
        self.bids.best != NIL && self.asks.best != NIL && self.bids.best >= self.asks.best
    }

    /// 撤掉所有符合條件的委託（kill switch、斷線自動撤單）。依訂單編號排序後撤，確保結果確定。
    /// 這裡掃描整個索引；真實系統會另外維護「會員 → 委託」索引。
    pub fn cancel_matching(
        &mut self,
        pred: impl Fn(OrderId) -> bool,
        out: &mut impl FnMut(Event),
    ) -> usize {
        let mut ids: Vec<OrderId> = self.index.keys().copied().filter(|id| pred(*id)).collect();
        ids.sort_unstable();
        for &id in &ids {
            self.cancel(id, out);
        }
        ids.len()
    }

    /// 集合競價撮合（開盤、收盤、暫停後恢復）。
    ///
    /// 在交叉區間內選出單一競價價格：
    /// 可成交量最大 → 未成交量（買賣差）最小 → 最接近參考價 → 價格較低。
    /// 候選價格是交叉區間內的各價格層，以及落在區間內的參考價。
    /// 然後雙方依價格-時間優先，全部以該價格成交。回傳 (價格, 成交量)。
    pub fn uncross(
        &mut self,
        reference: Price,
        out: &mut impl FnMut(Event),
    ) -> Option<(Price, Qty)> {
        if !self.is_crossed() {
            return None;
        }
        let (bb, ba) = (self.bids.best, self.asks.best);
        // 交叉區間 [ba, bb] 內的價格層（遞增排序）。
        let mut bids = Vec::new();
        let mut i = bb;
        while i != NIL && i >= ba {
            bids.push((i, self.bids.levels[i as usize].qty));
            i = if i == 0 {
                NIL
            } else {
                self.bids.bitmap.next_at_or_below(i - 1)
            };
        }
        bids.reverse();
        let mut asks = Vec::new();
        let mut i = ba;
        while i != NIL && i <= bb {
            asks.push((i, self.asks.levels[i as usize].qty));
            i = if (i as usize) + 1 >= self.asks.levels.len() {
                NIL
            } else {
                self.asks.bitmap.next_at_or_above(i + 1)
            };
        }
        let ref_idx =
            (reference - self.min_price).clamp(0, (self.bids.levels.len() - 1) as Price) as u32;
        let mut cands: Vec<u32> = bids.iter().chain(&asks).map(|x| x.0).collect();
        // 參考價落在交叉區間內時也列為候選：條件都相同時就以參考價成交。
        if (ba..=bb).contains(&ref_idx) {
            cands.push(ref_idx);
        }
        cands.sort_unstable();
        cands.dedup();

        let mut buy_cum: Qty = bids.iter().map(|x| x.1).sum();
        let mut sell_cum: Qty = 0;
        let (mut bi, mut ai) = (0, 0);
        // (成交量, -買賣差, -距參考價, -價格) 取最大
        let mut best: Option<(Qty, i128, i64, i64, u32)> = None;
        for &p in &cands {
            while bi < bids.len() && bids[bi].0 < p {
                buy_cum -= bids[bi].1;
                bi += 1;
            }
            while ai < asks.len() && asks[ai].0 <= p {
                sell_cum += asks[ai].1;
                ai += 1;
            }
            let exec = buy_cum.min(sell_cum);
            let key = (
                exec,
                -((buy_cum as i128 - sell_cum as i128).abs()),
                -(p as i64 - ref_idx as i64).abs(),
                -(p as i64),
                p,
            );
            if best.is_none_or(|b| (key.0, key.1, key.2, key.3) > (b.0, b.1, b.2, b.3)) {
                best = Some(key);
            }
        }
        let (volume, .., p) = best?;
        let price = self.min_price + p as Price;
        let mut remaining = volume;
        while remaining > 0 {
            let b = self.orders[self.bids.levels[self.bids.best as usize].head as usize];
            let a = self.orders[self.asks.levels[self.asks.best as usize].head as usize];
            debug_assert!(self.bids.best >= p && self.asks.best <= p);
            let q = b.qty.min(a.qty).min(remaining);
            out(Event::Cross {
                buy: b.id,
                sell: a.id,
                price,
                qty: q,
            });
            self.reduce_head(Side::Buy, q);
            self.reduce_head(Side::Sell, q);
            remaining -= q;
        }
        debug_assert!(!self.is_crossed());
        Some((price, volume))
    }

    /// 減少最佳價層隊首委託的數量；歸零則移除，層空了就更新最佳價。
    fn reduce_head(&mut self, side: Side, q: Qty) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let best = book.best;
        let level = &mut book.levels[best as usize];
        let slot = level.head;
        let o = &mut self.orders[slot as usize];
        o.qty -= q;
        level.qty -= q;
        if o.qty > 0 {
            return;
        }
        let (id, next) = (o.id, o.next);
        self.index.remove(&id);
        level.head = next;
        if next == NIL {
            level.tail = NIL;
        } else {
            self.orders[next as usize].prev = NIL;
        }
        level.count -= 1;
        self.free.push(slot);
        if level.count == 0 {
            book.bitmap.clear(best);
            book.best = match side {
                Side::Buy if best > 0 => book.bitmap.next_at_or_below(best - 1),
                Side::Sell if (best as usize) + 1 < book.levels.len() => {
                    book.bitmap.next_at_or_above(best + 1)
                }
                _ => NIL,
            };
        }
    }

    fn side(&self, side: Side) -> &BookSide {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    /// 最佳價與該層總量。
    pub fn best(&self, side: Side) -> Option<(Price, Qty)> {
        let b = self.side(side);
        (b.best != NIL).then(|| {
            (
                self.min_price + b.best as Price,
                b.levels[b.best as usize].qty,
            )
        })
    }

    /// 前 `n` 檔價量（L2 行情）。
    pub fn depth(&self, side: Side, n: usize) -> Vec<(Price, Qty)> {
        let b = self.side(side);
        let mut v = Vec::with_capacity(n.min(64));
        let mut i = b.best;
        while i != NIL && v.len() < n {
            v.push((self.min_price + i as Price, b.levels[i as usize].qty));
            i = match side {
                Side::Buy if i > 0 => b.bitmap.next_at_or_below(i - 1),
                Side::Sell if (i as usize) + 1 < b.levels.len() => b.bitmap.next_at_or_above(i + 1),
                _ => NIL,
            };
        }
        v
    }

    pub fn order_count(&self) -> usize {
        self.index.len()
    }

    /// 依優先順序列出所有掛單（L3 快照）：(side, price, id, qty)。
    pub fn snapshot(&self) -> Vec<(Side, Price, OrderId, Qty)> {
        let mut v = Vec::with_capacity(self.order_count());
        for side in [Side::Buy, Side::Sell] {
            let b = self.side(side);
            for (price, _) in self.depth(side, usize::MAX) {
                let mut slot = b.levels[(price - self.min_price) as usize].head;
                while slot != NIL {
                    let o = &self.orders[slot as usize];
                    v.push((side, price, o.id, o.qty));
                    slot = o.next;
                }
            }
        }
        v
    }

    /// 整本簿狀態的指紋，用來驗證「重播 journal 後得到完全相同的狀態」。
    pub fn fingerprint(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for (side, price, id, qty) in self.snapshot() {
            for x in [side as u64, price as u64, id, qty] {
                h = (h ^ x).wrapping_mul(0x0000_0100_0000_01B3);
            }
        }
        h
    }
}
