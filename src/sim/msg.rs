//! 元件之間傳遞的訊息。全部是固定大小的 `Copy` 型別，可以直接放進 SPSC ring。
//!
//! `t` 一律是「這個請求預定送出的時間」（奈秒，見 `Clock`），沿著整條路徑原封不動地傳遞，
//! 讓最後收到回應的元件可以直接算出端到端延遲。

use super::model::SymbolId;
use crate::types::*;

/// Gateway → 風控分片。
#[derive(Clone, Copy, Debug)]
pub enum ToRisk {
    New {
        order_id: OrderId,
        symbol: SymbolId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        t: u64,
    },
    Cancel {
        order_id: OrderId,
        t: u64,
    },
    /// 這個 gateway 不會再送任何請求了。
    Shutdown,
}

/// 風控分片 → 撮合分片（已通過風控、資金已凍結）。
#[derive(Clone, Copy, Debug)]
pub enum ToMatcher {
    New {
        order_id: OrderId,
        symbol: SymbolId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        t: u64,
    },
    Cancel {
        order_id: OrderId,
        symbol: SymbolId,
        t: u64,
    },
    Shutdown,
}

/// 撮合分片 → 風控分片（結算回饋，送到訂單所屬用戶的風控分片）。
#[derive(Clone, Copy, Debug)]
pub enum Settle {
    Ack {
        order_id: OrderId,
        t: u64,
    },
    Fill {
        order_id: OrderId,
        price: Price,
        qty: Qty,
        maker: bool,
        t: u64,
    },
    /// 訂單結束，釋放剩餘的凍結資金。`response` 表示這是撤單請求的回應（而非 IOC 剩餘量取消）。
    Done {
        order_id: OrderId,
        remaining: Qty,
        response: bool,
        t: u64,
    },
    Rejected {
        order_id: OrderId,
        reason: RejectReason,
        t: u64,
    },
    CancelReject {
        order_id: OrderId,
        t: u64,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RejectCode {
    InsufficientBalance = 0,
    InvalidOrder = 1,
    PriceOutOfBand = 2,
    BookFull = 3,
    DuplicateId = 4,
}

impl RejectCode {
    pub const COUNT: usize = 5;
    pub const ALL: [RejectCode; Self::COUNT] = [
        RejectCode::InsufficientBalance,
        RejectCode::InvalidOrder,
        RejectCode::PriceOutOfBand,
        RejectCode::BookFull,
        RejectCode::DuplicateId,
    ];
    pub fn name(self) -> &'static str {
        match self {
            RejectCode::InsufficientBalance => "insufficient_balance",
            RejectCode::InvalidOrder => "invalid_order",
            RejectCode::PriceOutOfBand => "price_out_of_band",
            RejectCode::BookFull => "book_full",
            RejectCode::DuplicateId => "duplicate_id",
        }
    }
    pub fn from_matcher(r: RejectReason) -> RejectCode {
        match r {
            RejectReason::PriceOutOfBand => RejectCode::PriceOutOfBand,
            RejectReason::BookFull => RejectCode::BookFull,
            RejectReason::DuplicateId => RejectCode::DuplicateId,
            RejectReason::InvalidQty | RejectReason::UnknownOrder => RejectCode::InvalidOrder,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ExecKind {
    Ack,
    Rejected(RejectCode),
    Fill {
        price: Price,
        qty: Qty,
        leaves: Qty,
        maker: bool,
    },
    Done {
        remaining: Qty,
    },
    CancelRejected,
}

/// 風控分片 → Gateway（執行回報）。
#[derive(Clone, Copy, Debug)]
pub enum ToGateway {
    Report {
        order_id: OrderId,
        kind: ExecKind,
        /// 這是不是某個請求的「那一個」回應。每個請求恰好有一個回應。
        response: bool,
        t: u64,
    },
    Shutdown,
}

/// 撮合分片 → 行情發布。
#[derive(Clone, Copy, Debug)]
pub enum MdMsg {
    Trade {
        symbol: SymbolId,
        price: Price,
        qty: Qty,
        taker_side: Side,
        t: u64,
    },
    Top {
        symbol: SymbolId,
        bid: Option<(Price, Qty)>,
        ask: Option<(Price, Qty)>,
        t: u64,
    },
    Shutdown,
}
