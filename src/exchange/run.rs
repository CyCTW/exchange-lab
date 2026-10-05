//! 組裝傳統交易所的拓樸、啟動執行緒、收集結果，並做一致性檢查與 journal 重播驗證。
//!
//! ```text
//!  Gateway ×G ──(G SPSC)──▶ Sequencer ──(P SPSC)──▶ Partition ×P ──(P SPSC)──▶ MarketData
//!      ▲                        │                       │
//!      │                        └──(SPSC)──▶ Journal    │
//!      └──────────────────(P×G SPSC) 執行回報 ───────────┘
//! ```

use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::gateway::{Gateway, GatewayIo, GatewayStats, GwReject};
use super::mdata::{self, MdStats};
use super::model::*;
use super::msg::*;
use super::partition::{self, PartitionCore, PartitionIo, PartitionStats, Sink};
use super::sequencer::{self, JournalStats, Sequencer, SequencerIo, SequencerStats};
use crate::affinity::pin_current_thread;
use crate::histogram::Histogram;
use crate::idle::{Clock, Idle};
use crate::ring;
use crate::types::*;

pub type Book = Vec<(Side, Price, OrderId, Qty)>;

pub struct PartitionSummary {
    pub stats: PartitionStats,
    pub hash: u64,
    pub resting: usize,
    pub books: Vec<(InstrumentId, Book)>,
}

pub struct ReplayResult {
    pub records: u64,
    pub contiguous: bool,
    pub partitions: Vec<PartitionSummary>,
    pub elapsed: Duration,
}

pub struct ExchangeReport {
    pub cfg: ExchangeConfig,
    pub insts: Vec<Instrument>,
    pub placement: Vec<usize>,
    pub wall: Duration,
    pub gateways: Vec<GatewayStats>,
    pub sequencer: SequencerStats,
    pub journal: Option<JournalStats>,
    pub partitions: Vec<PartitionSummary>,
    pub md: MdStats,
    pub replay: Option<ReplayResult>,
    pub checks: Vec<(String, bool, String)>,
}

impl ExchangeReport {
    pub fn all_checks_pass(&self) -> bool {
        self.checks.iter().all(|c| c.1)
    }
}

fn summarize(core: &PartitionCore) -> PartitionSummary {
    PartitionSummary {
        hash: core.hash(),
        resting: core.resting(),
        books: core
            .stats
            .instruments
            .iter()
            .map(|&i| (i, core.snapshot(i).unwrap()))
            .collect(),
        stats: core.stats.clone(),
    }
}

pub fn run(cfg: ExchangeConfig) -> ExchangeReport {
    assert!(cfg.gateways > 0 && cfg.partitions > 0 && cfg.instruments > 0);
    assert!(cfg.members >= cfg.gateways && cfg.members < (1 << 16));
    let cfg = Arc::new(cfg);
    let insts = Arc::new(cfg.instruments_list());
    let placement = cfg.placement_map();
    let path = Arc::new(cfg.price_path(&insts));
    let cap = cfg.ring_capacity;
    let (g, p) = (cfg.gateways, cfg.partitions);

    let mut gw_to_seq = vec![];
    let mut seq_from_gw = vec![];
    for _ in 0..g {
        let (tx, rx) = ring::channel(cap);
        gw_to_seq.push(tx);
        seq_from_gw.push(rx);
    }
    let mut seq_to_part = vec![];
    let mut part_from_seq = vec![];
    let mut part_to_md = vec![];
    let mut md_from_part = vec![];
    for _ in 0..p {
        let (tx, rx) = ring::channel(cap);
        seq_to_part.push(tx);
        part_from_seq.push(rx);
        let (tx, rx) = ring::channel(cap);
        part_to_md.push(tx);
        md_from_part.push(rx);
    }
    // part_to_gw[p][g] 與 gw_from_part[g][p] 是同一條 ring 的兩端。
    let mut part_to_gw: Vec<Vec<_>> = (0..p).map(|_| vec![]).collect();
    let mut gw_from_part: Vec<Vec<_>> = (0..g).map(|_| vec![]).collect();
    for row in part_to_gw.iter_mut() {
        for col in gw_from_part.iter_mut() {
            let (tx, rx) = ring::channel(cap);
            row.push(tx);
            col.push(rx);
        }
    }

    let clock = Clock::new(Instant::now());
    let start_ns = 50_000_000; // 給所有執行緒 50ms 啟動
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut next_core = 1;
    let mut core = || {
        let c = next_core % cores;
        next_core += 1;
        c
    };
    let pin = cfg.pin;

    let md = {
        let (n, idle, c) = (cfg.instruments, cfg.idle, core());
        spawn("md", pin, c, move || {
            mdata::run(md_from_part, n, clock, idle)
        })
    };

    let (journal_tx, journal) = match cfg.journal.clone() {
        Some(path) => {
            let (tx, rx) = ring::channel(cap);
            let idle = Idle::new(cfg.idle);
            let c = core();
            let h = spawn("journal", pin, c, move || {
                sequencer::run_journal(&path, rx, idle).expect("journal")
            });
            (Some(tx), Some(h))
        }
        None => (None, None),
    };

    let mut parts = vec![];
    for (i, ((from_seq, to_gw), to_md)) in part_from_seq
        .into_iter()
        .zip(part_to_gw)
        .zip(part_to_md)
        .enumerate()
    {
        let core_ = PartitionCore::new(i, &cfg, &insts, &placement);
        let io = PartitionIo {
            from_seq,
            to_gw,
            to_md,
        };
        let idle = Idle::new(cfg.idle);
        let c = core();
        parts.push(spawn(format!("partition-{i}"), pin, c, move || {
            partition::run(core_, io, idle)
        }));
    }

    let seq = {
        let io = SequencerIo {
            from_gw: seq_from_gw,
            to_part: seq_to_part,
            to_journal: journal_tx,
        };
        let s = Sequencer::new(&cfg, io, clock, start_ns);
        let c = core();
        spawn("sequencer", pin, c, move || s.run())
    };

    let mut gws = vec![];
    for (i, (to_seq, from_part)) in gw_to_seq.into_iter().zip(gw_from_part).enumerate() {
        let gw = Gateway::new(
            i,
            cfg.clone(),
            insts.clone(),
            path.clone(),
            GatewayIo { to_seq, from_part },
            clock,
            start_ns,
        );
        let c = core();
        gws.push(spawn(format!("gateway-{i}"), pin, c, move || gw.run()));
    }

    let gateways: Vec<GatewayStats> = gws.into_iter().map(|h| h.join().unwrap()).collect();
    let sequencer = seq.join().unwrap();
    let cores_: Vec<PartitionCore> = parts.into_iter().map(|h| h.join().unwrap()).collect();
    let md = md.join().unwrap();
    let journal = journal.map(|h| h.join().unwrap());
    let wall = Duration::from_nanos(clock.now_ns().saturating_sub(start_ns));

    let partitions: Vec<PartitionSummary> = cores_.iter().map(summarize).collect();
    let cfg = Arc::try_unwrap(cfg).unwrap_or_else(|a| (*a).clone());
    let insts = Arc::try_unwrap(insts).unwrap_or_else(|a| (*a).clone());
    let replay = cfg
        .journal
        .as_ref()
        .map(|p| replay(&cfg, &insts, &placement, p).expect("replay journal"));

    let mut rep = ExchangeReport {
        cfg,
        insts,
        placement,
        wall,
        gateways,
        sequencer,
        journal,
        partitions,
        md,
        replay,
        checks: vec![],
    };
    rep.checks = verify(&rep);
    rep
}

fn spawn<T: Send + 'static>(
    name: impl Into<String>,
    pin: bool,
    core: usize,
    f: impl FnOnce() -> T + Send + 'static,
) -> thread::JoinHandle<T> {
    thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            if pin {
                pin_current_thread(core);
            }
            f()
        })
        .unwrap()
}

struct NullSink;

impl Sink for NullSink {
    fn report(&mut self, _: usize, _: ToGateway) {}
    fn md(&mut self, _: MdMsg) {}
}

/// 離線重播 journal：用全新的分區狀態機、同樣的路由規則，在單一執行緒上依序處理每一筆紀錄。
/// 若系統是確定性的，得到的輸出雜湊與最終簿必須和即時執行完全相同。
pub fn replay(
    cfg: &ExchangeConfig,
    insts: &[Instrument],
    placement: &[usize],
    path: &Path,
) -> io::Result<ReplayResult> {
    let t0 = Instant::now();
    let mut cores: Vec<PartitionCore> = (0..cfg.partitions)
        .map(|i| PartitionCore::new(i, cfg, insts, placement))
        .collect();
    let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut buf = [0u8; RECORD_SIZE];
    let (mut records, mut contiguous) = (0u64, true);
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let m = decode(&buf)?;
        records += 1;
        contiguous &= m.seq == records;
        let target = match m.body {
            SeqBody::New { inst, .. }
            | SeqBody::Cancel { inst, .. }
            | SeqBody::Replace { inst, .. } => Some(placement[inst as usize]),
            _ => None,
        };
        match target {
            Some(p) => cores[p].process(&m, &mut NullSink),
            None => {
                for c in cores.iter_mut() {
                    c.process(&m, &mut NullSink);
                }
            }
        }
    }
    Ok(ReplayResult {
        records,
        contiguous,
        partitions: cores.iter().map(summarize).collect(),
        elapsed: t0.elapsed(),
    })
}

/// 跨所有元件的一致性檢查。
fn verify(rep: &ExchangeReport) -> Vec<(String, bool, String)> {
    let mut checks = vec![];
    let mut add =
        |name: &str, ok: bool, detail: String| checks.push((name.to_string(), ok, detail));

    let req: u64 = rep
        .gateways
        .iter()
        .map(|g| g.sent_new + g.sent_cancel + g.sent_replace)
        .sum();
    let resp: u64 = rep.gateways.iter().map(|g| g.responses).sum();
    add(
        "每個請求恰好一個回應",
        req == resp,
        format!("請求 {req}，回應 {resp}"),
    );

    // 每筆成交都有一買一賣：所有會員在每個商品的淨部位總和為 0，淨現金流總和為 0。
    let n = rep.cfg.instruments;
    let mut pos = vec![0i64; n];
    let mut cash: i128 = 0;
    for g in &rep.gateways {
        for (i, x) in g.net_position.iter().enumerate() {
            pos[i] += x;
        }
        cash += g.net_cash;
    }
    let bad: Vec<_> = (0..n).filter(|&i| pos[i] != 0).collect();
    add(
        "部位與現金流守恆（淨部位、淨現金流加總為 0）",
        bad.is_empty() && cash == 0,
        format!("不為 0 的商品 {bad:?}，淨現金流 {cash}"),
    );

    let open: usize = rep.gateways.iter().map(|g| g.open_orders).sum();
    let resting: usize = rep.partitions.iter().map(|p| p.resting).sum();
    let credit = rep.gateways.iter().all(|g| g.credit_consistent);
    add(
        "gateway 未結委託 = 撮合簿掛單，信用占用一致",
        open == resting && credit,
        format!("gateway {open}，撮合簿 {resting}"),
    );

    let trades: u64 = rep
        .partitions
        .iter()
        .map(|p| p.stats.trades + p.stats.crosses)
        .sum();
    let fills: u64 = rep.gateways.iter().map(|g| g.fills).sum();
    let md_trades: u64 = rep.md.per_inst.iter().map(|s| s.trades).sum();
    add(
        "fill = 2 × 成交 = 2 × 行情成交",
        fills == 2 * trades && md_trades == trades,
        format!("成交 {trades}，fill {fills}，行情成交 {md_trades}"),
    );

    let mut mismatched = vec![];
    for p in &rep.partitions {
        for (inst, book) in &p.books {
            if &rep.md.books[*inst as usize] != book {
                mismatched.push(*inst);
            }
        }
    }
    add(
        "行情重建的簿 = 撮合簿（逐筆、含優先順序）",
        mismatched.is_empty() && rep.md.gaps == 0 && rep.md.bad_events == 0,
        format!(
            "{} 則行情，跳號 {}，無法套用 {}，不符商品 {:?}",
            rep.md.messages, rep.md.gaps, rep.md.bad_events, mismatched
        ),
    );

    let expect = [
        Phase::PreOpen,
        Phase::Continuous,
        Phase::PreClose,
        Phase::Closed,
    ];
    add(
        "交易階段依序發生",
        rep.md.phases_seen == expect,
        format!("{:?}", rep.md.phases_seen),
    );

    if let (Some(j), Some(r)) = (&rep.journal, &rep.replay) {
        let hashes_ok = rep
            .partitions
            .iter()
            .zip(&r.partitions)
            .all(|(a, b)| a.hash == b.hash && a.books == b.books);
        add(
            "journal 重播 → 輸出與最終簿完全相同（確定性）",
            j.records == rep.sequencer.sequenced && r.contiguous && hashes_ok,
            format!(
                "{} 筆紀錄（{:.1} MB），序號連續 {}，重播耗時 {:?}",
                r.records,
                j.bytes as f64 / 1e6,
                r.contiguous,
                r.elapsed
            ),
        );
    }
    checks
}

fn lat(h: &Histogram) -> String {
    let f = |ns: u64| {
        if ns >= 1_000_000 {
            format!("{:.1}ms", ns as f64 / 1e6)
        } else {
            format!("{:.1}µs", ns as f64 / 1e3)
        }
    };
    format!(
        "p50 {}  p90 {}  p99 {}  p99.9 {}  max {}  (n={})",
        f(h.percentile(50.0)),
        f(h.percentile(90.0)),
        f(h.percentile(99.0)),
        f(h.percentile(99.9)),
        f(h.max()),
        h.count()
    )
}

fn px(p: Price) -> String {
    format!("{}.{:02}", p / 100, p % 100)
}

impl ExchangeReport {
    pub fn render(&self) -> String {
        let c = &self.cfg;
        let mut o = String::new();
        let secs = self.wall.as_secs_f64();
        let threads = c.gateways + c.partitions + 2 + c.journal.is_some() as usize;
        let _ = writeln!(
            o,
            "== 拓樸 ==\n  gateway×{} → sequencer → partition×{} → md{}  （{} 條執行緒，{} 核心，idle={:?}，pin={}）",
            c.gateways,
            c.partitions,
            if c.journal.is_some() { "，sequencer → journal" } else { "" },
            threads,
            thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
            c.idle,
            c.pin
        );
        let _ = writeln!(
            o,
            "  會員 {}（做市商 {}、自營 {}、經紀商 {}），商品 {}，目標 {} 動作/秒，交易日壓縮為 {:?}，放置 {:?}",
            c.members,
            c.market_makers(),
            c.props(),
            c.members - c.market_makers() - c.props(),
            c.instruments,
            c.rate,
            c.duration,
            c.placement
        );

        let sum = |f: &dyn Fn(&GatewayStats) -> u64| self.gateways.iter().map(f).sum::<u64>();
        let (new, cancel, replace) = (
            sum(&|g| g.sent_new),
            sum(&|g| g.sent_cancel),
            sum(&|g| g.sent_replace),
        );
        let trades: u64 = self.partitions.iter().map(|p| p.stats.trades).sum();
        let crosses: u64 = self.partitions.iter().map(|p| p.stats.crosses).sum();
        let _ = writeln!(o, "\n== 流量 ==");
        let _ = writeln!(
            o,
            "  送進交易所 {} 則（新單 {}、改單 {}、撤單 {}）→ {:.0} 則/秒；定序 {} 則（含控制事件 {}）",
            new + cancel + replace,
            new,
            replace,
            cancel,
            (new + cancel + replace) as f64 / secs,
            self.sequencer.sequenced,
            self.sequencer.control_events
        );
        let _ = writeln!(o, "  成交：連續交易 {trades} 筆、集合競價 {crosses} 筆");
        let gw: Vec<_> = GwReject::ALL
            .iter()
            .map(|r| format!("{} {}", r.name(), sum(&|g| g.gw_rejects[*r as usize])))
            .collect();
        let _ = writeln!(o, "  gateway 事前風控擋下：{}", gw.join("、"));
        let mut rej: Vec<(String, u64)> = vec![];
        for g in &self.gateways {
            for (k, v) in &g.rejects {
                match rej.iter_mut().find(|x| &x.0 == k) {
                    Some(x) => x.1 += v,
                    None => rej.push((k.clone(), *v)),
                }
            }
        }
        rej.sort();
        let rej_s: Vec<_> = rej.iter().map(|(k, v)| format!("{k} {v}")).collect();
        let _ = writeln!(
            o,
            "  撮合回報：ack {}，fill {}，撤單/結束 {}，撤單失敗 {}，拒絕 [{}]",
            sum(&|g| g.acks),
            sum(&|g| g.fills),
            sum(&|g| g.done),
            sum(&|g| g.cancel_rejects),
            rej_s.join(", ")
        );
        let mass: u64 = self.partitions.iter().map(|p| p.stats.mass_cancelled).sum();
        if sum(&|g| g.sent_mass_cancel) > 0 {
            let _ = writeln!(
                o,
                "  斷線自動撤單：{} 次，撤掉 {} 張委託",
                sum(&|g| g.sent_mass_cancel),
                mass
            );
        }
        let behind = self
            .gateways
            .iter()
            .map(|g| g.max_behind_ns)
            .max()
            .unwrap_or(0);
        let _ = writeln!(
            o,
            "  gateway 最大排程落後 {:.1}ms；sequencer 單次輪詢最多 {} 則",
            behind as f64 / 1e6,
            self.sequencer.max_batch
        );

        let _ = writeln!(o, "\n== 延遲（從預定送出時間起算） ==");
        let mut all = Histogram::default();
        for g in &self.gateways {
            all.merge(&g.response_latency);
        }
        let _ = writeln!(
            o,
            "  請求→回應（gateway→sequencer→partition→gateway）  {}",
            lat(&all)
        );
        let _ = writeln!(
            o,
            "  請求→行情成交（gateway→sequencer→partition→md）    {}",
            lat(&self.md.trade_latency)
        );

        let _ = writeln!(o, "\n== 分區負載 ==");
        let total: u64 = self
            .partitions
            .iter()
            .map(|p| p.stats.msgs)
            .sum::<u64>()
            .max(1);
        for p in &self.partitions {
            let _ = writeln!(
                o,
                "  partition-{}  訊息 {:>9} ({:>4.1}%)  成交 {:>8}  競價成交 {:>6}  掛單 {:>6}  商品 {} 檔",
                p.stats.id,
                p.stats.msgs,
                100.0 * p.stats.msgs as f64 / total as f64,
                p.stats.trades,
                p.stats.crosses,
                p.resting,
                p.stats.instruments.len()
            );
        }

        let _ = writeln!(o, "\n== 熱門商品（前 8 檔） ==");
        let _ = writeln!(
            o,
            "  {:<6} {:>4}  {:>9} {:>10}  {:>18}  {:>18}  {:>18}",
            "代號", "分區", "成交筆數", "成交量", "開盤競價 價×量", "暫停後恢復", "收盤競價 價×量"
        );
        for s in self.insts.iter().take(8) {
            let m = &self.md.per_inst[s.id as usize];
            let f = |x: Option<(Price, Qty)>| {
                x.map(|(p, q)| format!("{}×{}", px(p), q))
                    .unwrap_or("-".into())
            };
            let _ = writeln!(
                o,
                "  {:<6} {:>4}  {:>9} {:>10}  {:>18}  {:>18}  {:>18}",
                s.symbol,
                self.placement[s.id as usize],
                m.trades,
                m.volume,
                f(m.open),
                f(m.reopen),
                f(m.close)
            );
        }

        let _ = writeln!(o, "\n== 一致性檢查 ==");
        for (name, ok, detail) in &self.checks {
            let _ = writeln!(
                o,
                "  [{}] {}  {}",
                if *ok { "OK" } else { "FAIL" },
                name,
                detail
            );
        }
        o
    }
}
