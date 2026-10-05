//! 傳統交易所各元件之間的訊息。全部是固定大小的 `Copy` 型別。
//!
//! 核心是 [`SeqMsg`]：定序器（sequencer）蓋上全域序號的訊息。它同時是
//! 撮合分區的輸入、也是 journal 的內容——整個交易所的狀態都由這個序列唯一決定。

use std::io;

use super::model::*;
use crate::types::*;

/// 會員（經 gateway）送進交易所的請求。
#[derive(Clone, Copy, Debug)]
pub enum Request {
    New {
        order_id: OrderId,
        inst: InstrumentId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        t: u64,
    },
    Cancel {
        order_id: OrderId,
        inst: InstrumentId,
        t: u64,
    },
    /// 改單：撤掉舊單、以新編號掛新單（OUCH 的 Replace 語義，失去時間優先）。
    Replace {
        old_id: OrderId,
        new_id: OrderId,
        inst: InstrumentId,
        side: Side,
        price: Price,
        qty: Qty,
        t: u64,
    },
    /// 撤掉某會員的所有委託（kill switch / 斷線自動撤單）。
    MassCancel {
        member: MemberId,
        t: u64,
    },
    Shutdown,
}

/// 定序後的訊息內容。除了會員請求，也包含交易所自己的市場控制事件——
/// 它們一樣拿到序號、一樣寫進 journal，所以重播時會在完全相同的位置發生。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqBody {
    New {
        order_id: OrderId,
        inst: InstrumentId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        t: u64,
    },
    Cancel {
        order_id: OrderId,
        inst: InstrumentId,
        t: u64,
    },
    Replace {
        old_id: OrderId,
        new_id: OrderId,
        inst: InstrumentId,
        side: Side,
        price: Price,
        qty: Qty,
        t: u64,
    },
    MassCancel {
        member: MemberId,
        t: u64,
    },
    /// 全市場進入某個交易階段（進入連續交易 / 收盤時先做集合競價撮合）。
    Phase(Phase),
    /// 單一商品暫停交易（之後以集合競價恢復）。
    Halt(InstrumentId),
    Resume(InstrumentId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqMsg {
    pub seq: u64,
    /// 定序器蓋的時間戳。撮合引擎不讀時鐘，只用這個。
    pub ts: u64,
    pub body: SeqBody,
}

/// 定序器 → 撮合分區。
#[derive(Clone, Copy, Debug)]
pub enum ToPartition {
    Msg(SeqMsg),
    Shutdown,
}

/// 定序器 → journal 執行緒。
#[derive(Clone, Copy, Debug)]
pub enum ToJournal {
    Rec(SeqMsg),
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecKind {
    Ack,
    Rejected(RejectReason),
    Fill {
        side: Side,
        price: Price,
        qty: Qty,
        leaves: Qty,
    },
    Done {
        remaining: Qty,
    },
    CancelRejected,
}

/// 撮合分區 → gateway（執行回報）。
#[derive(Clone, Copy, Debug)]
pub enum ToGateway {
    Report {
        order_id: OrderId,
        kind: ExecKind,
        /// 這是不是某個請求的「那一個」回應。每個請求恰好一個回應。
        response: bool,
        t: u64,
    },
    Shutdown,
}

/// ITCH 風格的逐筆（L3）行情：客戶端只靠這些事件就能重建完整的限價簿。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MdBody {
    Add {
        inst: InstrumentId,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    Executed {
        inst: InstrumentId,
        id: OrderId,
        qty: Qty,
    },
    Deleted {
        inst: InstrumentId,
        id: OrderId,
    },
    Trade {
        inst: InstrumentId,
        price: Price,
        qty: Qty,
        auction: bool,
        t: u64,
    },
    AuctionResult {
        inst: InstrumentId,
        price: Price,
        qty: Qty,
        phase: Phase,
    },
    Phase(Phase),
    Halt(InstrumentId),
    Resume(InstrumentId),
}

/// 撮合分區 → 行情發布。每個分區是一個獨立的行情頻道，有自己的連續序號。
#[derive(Clone, Copy, Debug)]
pub enum MdMsg {
    Event { chan_seq: u64, body: MdBody },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Journal 編碼：每筆 64 bytes 固定長度，little-endian
// [0..8 seq][8..16 ts][16 tag][17 side][18 tif][19 phase][20..22 inst][22..24 member]
// [24..32 id1][32..40 id2][40..48 price][48..56 qty][56..64 t]

pub const RECORD_SIZE: usize = 64;

pub fn encode(m: &SeqMsg) -> [u8; RECORD_SIZE] {
    let mut r = [0u8; RECORD_SIZE];
    r[0..8].copy_from_slice(&m.seq.to_le_bytes());
    r[8..16].copy_from_slice(&m.ts.to_le_bytes());
    let mut put = |tag: u8,
                   side: u8,
                   tif: u8,
                   phase: u8,
                   inst: u16,
                   member: u16,
                   id1: u64,
                   id2: u64,
                   price: i64,
                   qty: u64,
                   t: u64| {
        r[16] = tag;
        r[17] = side;
        r[18] = tif;
        r[19] = phase;
        r[20..22].copy_from_slice(&inst.to_le_bytes());
        r[22..24].copy_from_slice(&member.to_le_bytes());
        r[24..32].copy_from_slice(&id1.to_le_bytes());
        r[32..40].copy_from_slice(&id2.to_le_bytes());
        r[40..48].copy_from_slice(&price.to_le_bytes());
        r[48..56].copy_from_slice(&qty.to_le_bytes());
        r[56..64].copy_from_slice(&t.to_le_bytes());
    };
    match m.body {
        SeqBody::New {
            order_id,
            inst,
            side,
            price,
            qty,
            tif,
            t,
        } => put(
            1, side as u8, tif as u8, 0, inst, 0, order_id, 0, price, qty, t,
        ),
        SeqBody::Cancel { order_id, inst, t } => put(2, 0, 0, 0, inst, 0, order_id, 0, 0, 0, t),
        SeqBody::Replace {
            old_id,
            new_id,
            inst,
            side,
            price,
            qty,
            t,
        } => put(3, side as u8, 0, 0, inst, 0, old_id, new_id, price, qty, t),
        SeqBody::MassCancel { member, t } => put(4, 0, 0, 0, 0, member, 0, 0, 0, 0, t),
        SeqBody::Phase(p) => put(5, 0, 0, p as u8, 0, 0, 0, 0, 0, 0, 0),
        SeqBody::Halt(i) => put(6, 0, 0, 0, i, 0, 0, 0, 0, 0, 0),
        SeqBody::Resume(i) => put(7, 0, 0, 0, i, 0, 0, 0, 0, 0, 0),
    }
    r
}

pub fn decode(r: &[u8; RECORD_SIZE]) -> io::Result<SeqMsg> {
    let u64_at = |i: usize| u64::from_le_bytes(r[i..i + 8].try_into().unwrap());
    let u16_at = |i: usize| u16::from_le_bytes(r[i..i + 2].try_into().unwrap());
    let side = if r[17] == 0 { Side::Buy } else { Side::Sell };
    let tif = if r[18] == 0 {
        TimeInForce::Gtc
    } else {
        TimeInForce::Ioc
    };
    let (inst, member, id1, id2, price, qty, t) = (
        u16_at(20),
        u16_at(22),
        u64_at(24),
        u64_at(32),
        u64_at(40) as i64,
        u64_at(48),
        u64_at(56),
    );
    let body = match r[16] {
        1 => SeqBody::New {
            order_id: id1,
            inst,
            side,
            price,
            qty,
            tif,
            t,
        },
        2 => SeqBody::Cancel {
            order_id: id1,
            inst,
            t,
        },
        3 => SeqBody::Replace {
            old_id: id1,
            new_id: id2,
            inst,
            side,
            price,
            qty,
            t,
        },
        4 => SeqBody::MassCancel { member, t },
        5 => SeqBody::Phase(Phase::from_u8(r[19])),
        6 => SeqBody::Halt(inst),
        7 => SeqBody::Resume(inst),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad journal tag",
            ))
        }
    };
    Ok(SeqMsg {
        seq: u64_at(0),
        ts: u64_at(8),
        body,
    })
}
