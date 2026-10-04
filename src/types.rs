//! 交易所核心的基本型別。
//!
//! 價格一律用「tick 數」的整數表示（定點數），絕不用浮點：
//! 浮點有捨入誤差、比較不可靠，而且無法當陣列索引。

pub type OrderId = u64;
/// 以 tick 為單位的整數價格。
pub type Price = i64;
pub type Qty = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TimeInForce {
    /// Good-till-cancel：未成交部分掛在簿上。
    Gtc = 0,
    /// Immediate-or-cancel：能成交多少就成交多少，剩下的立即取消。
    Ioc = 1,
}

/// 撮合引擎的輸入。在真實系統中，這是 sequencer 排好序、寫進 journal 後的訊息。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    NewOrder {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
    },
    Cancel {
        id: OrderId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    InvalidQty,
    PriceOutOfBand,
    DuplicateId,
    UnknownOrder,
    BookFull,
}

/// 撮合引擎的輸出。下游（行情發布、回報、清算）都只消費這個事件流。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Accepted {
        id: OrderId,
    },
    Rejected {
        id: OrderId,
        reason: RejectReason,
    },
    Trade {
        taker: OrderId,
        maker: OrderId,
        taker_side: Side,
        price: Price,
        qty: Qty,
    },
    /// 主動取消，或 IOC 剩餘量被取消。
    Cancelled {
        id: OrderId,
        remaining: Qty,
    },
}
