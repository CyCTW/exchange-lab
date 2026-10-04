//! 輸入 journal：把「已排序的指令流」以固定長度二進位紀錄追加寫入檔案。
//!
//! 撮合引擎是確定性狀態機（同樣的輸入序列 ⇒ 同樣的狀態與輸出），
//! 所以只要持久化「輸入」就能在當機後重播回完全相同的狀態，
//! 也能把同一份輸入流送到備援節點做 hot standby（見 docs/05-reliability.md）。
//!
//! 紀錄格式（40 bytes，little-endian）：
//! `seq:u64 | tag:u8 | side:u8 | tif:u8 | pad[5] | id:u64 | price:i64 | qty:u64`

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::types::*;

pub const RECORD_SIZE: usize = 40;

pub fn encode(seq: u64, cmd: &Command) -> [u8; RECORD_SIZE] {
    let mut r = [0u8; RECORD_SIZE];
    r[0..8].copy_from_slice(&seq.to_le_bytes());
    let (tag, side, tif, id, price, qty) = match *cmd {
        Command::NewOrder {
            id,
            side,
            price,
            qty,
            tif,
        } => (1u8, side as u8, tif as u8, id, price, qty),
        Command::Cancel { id } => (2u8, 0, 0, id, 0, 0),
    };
    r[8] = tag;
    r[9] = side;
    r[10] = tif;
    r[16..24].copy_from_slice(&id.to_le_bytes());
    r[24..32].copy_from_slice(&price.to_le_bytes());
    r[32..40].copy_from_slice(&qty.to_le_bytes());
    r
}

pub fn decode(r: &[u8; RECORD_SIZE]) -> io::Result<(u64, Command)> {
    let u64_at = |i: usize| u64::from_le_bytes(r[i..i + 8].try_into().unwrap());
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let seq = u64_at(0);
    let id = u64_at(16);
    let cmd = match r[8] {
        1 => Command::NewOrder {
            id,
            side: match r[9] {
                0 => Side::Buy,
                1 => Side::Sell,
                _ => return Err(bad("bad side")),
            },
            tif: match r[10] {
                0 => TimeInForce::Gtc,
                1 => TimeInForce::Ioc,
                _ => return Err(bad("bad tif")),
            },
            price: u64_at(24) as i64,
            qty: u64_at(32),
        },
        2 => Command::Cancel { id },
        _ => return Err(bad("bad tag")),
    };
    Ok((seq, cmd))
}

pub struct JournalWriter {
    w: BufWriter<File>,
}

impl JournalWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(JournalWriter {
            w: BufWriter::with_capacity(1 << 20, f),
        })
    }

    #[inline]
    pub fn append(&mut self, seq: u64, cmd: &Command) -> io::Result<()> {
        self.w.write_all(&encode(seq, cmd))
    }

    /// 寫進 OS page cache。程序崩潰不會掉資料，但機器斷電會。
    pub fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }

    /// 真正落盤。代價是數十微秒到數毫秒，所以真實系統會批次（group commit），
    /// 或改用「複製到另一台機器的記憶體」取代同步落盤。
    pub fn sync(&mut self) -> io::Result<()> {
        self.w.flush()?;
        self.w.get_ref().sync_data()
    }
}

pub struct JournalReader {
    r: BufReader<File>,
}

impl JournalReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(JournalReader {
            r: BufReader::with_capacity(1 << 20, File::open(path)?),
        })
    }
}

impl Iterator for JournalReader {
    type Item = io::Result<(u64, Command)>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; RECORD_SIZE];
        match self.r.read_exact(&mut buf) {
            Ok(()) => Some(decode(&buf)),
            // 結尾不完整的紀錄（寫到一半當機）直接忽略。
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}
