//! 定序器（sequencer）：整個交易所唯一決定「順序」的地方。
//!
//! - 輪詢所有 gateway 的輸入佇列，替每則請求蓋上全域單調遞增的序號與時間戳。
//! - 依交易日時程插入市場控制事件（開盤、收盤、暫停、恢復），它們也拿到序號。
//! - 每則定序後的訊息送到：journal（持久化）＋ 該商品所屬的撮合分區；
//!   全市場事件（階段變化、整批撤單）送到所有分區。
//!
//! 因為每個分區只從定序器的單一佇列讀取，它看到的就是全域序列中屬於自己的子序列，
//! 重播 journal 時只要用同樣的路由規則，就會得到完全相同的結果。

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use super::model::*;
use super::msg::*;
use crate::idle::{Clock, Idle};
use crate::ring::{Consumer, Producer};

pub struct SequencerIo {
    pub from_gw: Vec<Consumer<Request>>,
    pub to_part: Vec<Producer<ToPartition>>,
    pub to_journal: Option<Producer<ToJournal>>,
}

#[derive(Default, Debug, Clone)]
pub struct SequencerStats {
    pub sequenced: u64,
    pub control_events: u64,
    /// 一次輪詢中最多拿到幾則請求（反映排隊程度）。
    pub max_batch: usize,
}

pub struct Sequencer {
    placement: Vec<usize>,
    schedule: Vec<(u64, SeqBody)>,
    next_event: usize,
    io: SequencerIo,
    clock: Clock,
    idle: Idle,
    seq: u64,
    gw_shutdowns: usize,
    stats: SequencerStats,
}

/// 交易日時程 → 定序器在指定時間插入的控制事件。
pub fn schedule(cfg: &ExchangeConfig, start_ns: u64) -> Vec<(u64, SeqBody)> {
    let at = |f: f64| start_ns + cfg.at(f);
    let mut s = vec![
        (start_ns, SeqBody::Phase(Phase::PreOpen)),
        (at(OPEN_AT), SeqBody::Phase(Phase::Continuous)),
        (at(PRECLOSE_AT), SeqBody::Phase(Phase::PreClose)),
        (at(CLOSE_AT), SeqBody::Phase(Phase::Closed)),
    ];
    if cfg.halt {
        s.push((at(HALT_AT), SeqBody::Halt(0)));
        s.push((at(RESUME_AT), SeqBody::Resume(0)));
    }
    s.sort_by_key(|e| e.0);
    s
}

impl Sequencer {
    pub fn new(cfg: &ExchangeConfig, io: SequencerIo, clock: Clock, start_ns: u64) -> Self {
        Sequencer {
            placement: cfg.placement_map(),
            schedule: schedule(cfg, start_ns),
            next_event: 0,
            io,
            clock,
            idle: Idle::new(cfg.idle),
            seq: 0,
            gw_shutdowns: 0,
            stats: SequencerStats::default(),
        }
    }

    pub fn run(mut self) -> SequencerStats {
        let gateways = self.io.from_gw.len();
        while self.gw_shutdowns < gateways {
            let mut work = self.emit_due_events(self.clock.now_ns());
            for g in 0..gateways {
                let mut batch = 0;
                while batch < 256 {
                    let Some(r) = self.io.from_gw[g].try_pop() else {
                        break;
                    };
                    batch += 1;
                    self.on_request(r);
                }
                self.stats.max_batch = self.stats.max_batch.max(batch);
                work += batch;
            }
            if work == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
        // 所有 gateway 都結束了：補發剩下的時程事件，然後關閉下游。
        self.emit_due_events(u64::MAX);
        for p in 0..self.io.to_part.len() {
            push(&mut self.io.to_part[p], ToPartition::Shutdown);
        }
        if let Some(j) = self.io.to_journal.as_mut() {
            push(j, ToJournal::Shutdown);
        }
        self.stats
    }

    fn emit_due_events(&mut self, now: u64) -> usize {
        let mut n = 0;
        while self.next_event < self.schedule.len() && self.schedule[self.next_event].0 <= now {
            let body = self.schedule[self.next_event].1;
            self.next_event += 1;
            self.stats.control_events += 1;
            self.emit(body, None);
            n += 1;
        }
        n
    }

    fn on_request(&mut self, r: Request) {
        match r {
            Request::New {
                order_id,
                inst,
                side,
                price,
                qty,
                tif,
                t,
            } => self.emit(
                SeqBody::New {
                    order_id,
                    inst,
                    side,
                    price,
                    qty,
                    tif,
                    t,
                },
                Some(inst),
            ),
            Request::Cancel { order_id, inst, t } => {
                self.emit(SeqBody::Cancel { order_id, inst, t }, Some(inst))
            }
            Request::Replace {
                old_id,
                new_id,
                inst,
                side,
                price,
                qty,
                t,
            } => self.emit(
                SeqBody::Replace {
                    old_id,
                    new_id,
                    inst,
                    side,
                    price,
                    qty,
                    t,
                },
                Some(inst),
            ),
            Request::MassCancel { member, t } => self.emit(SeqBody::MassCancel { member, t }, None),
            Request::Shutdown => self.gw_shutdowns += 1,
        }
    }

    /// 蓋序號 → journal → 路由到分區（`None` = 廣播給所有分區）。
    fn emit(&mut self, body: SeqBody, inst: Option<InstrumentId>) {
        self.seq += 1;
        self.stats.sequenced += 1;
        let m = SeqMsg {
            seq: self.seq,
            ts: self.clock.now_ns(),
            body,
        };
        if let Some(j) = self.io.to_journal.as_mut() {
            push(j, ToJournal::Rec(m));
        }
        match inst {
            Some(i) => push(
                &mut self.io.to_part[self.placement[i as usize]],
                ToPartition::Msg(m),
            ),
            None => {
                for p in 0..self.io.to_part.len() {
                    push(&mut self.io.to_part[p], ToPartition::Msg(m));
                }
            }
        }
    }
}

/// 下游（分區、journal）一定會持續消化，所以單純等待即可。
#[inline]
fn push<T>(p: &mut Producer<T>, mut v: T) {
    while let Err(back) = p.try_push(v) {
        v = back;
        std::hint::spin_loop();
    }
}

#[derive(Default, Debug, Clone)]
pub struct JournalStats {
    pub records: u64,
    pub bytes: u64,
}

/// Journal 執行緒：和撮合分區「平行」消費定序後的訊息流，寫成固定長度紀錄。
/// 撮合不必等它（LMAX 的做法是讓撮合消費者以序號等待 journal；這裡為了簡單沒有加這個依賴，
/// 代價是當機時最後幾筆可能已撮合但尚未落盤——真實系統必須補上，見 docs/07）。
pub fn run_journal(
    path: &Path,
    mut rx: Consumer<ToJournal>,
    mut idle: Idle,
) -> io::Result<JournalStats> {
    let mut w = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut st = JournalStats::default();
    loop {
        match rx.try_pop() {
            Some(ToJournal::Rec(m)) => {
                idle.reset();
                w.write_all(&encode(&m))?;
                st.records += 1;
                st.bytes += RECORD_SIZE as u64;
            }
            Some(ToJournal::Shutdown) => break,
            None => idle.idle(),
        }
    }
    w.flush()?;
    Ok(st)
}
