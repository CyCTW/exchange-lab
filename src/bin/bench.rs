//! 效能實驗。
//!
//! ```text
//! cargo run --release --bin bench -- [--n N] [--mode single|pipeline|all]
//!                                    [--rate MSGS_PER_SEC] [--pin] [--journal PATH]
//! ```
//!
//! - `single`：單執行緒直接呼叫撮合引擎，量純撮合的吞吐與每筆指令延遲。
//! - `pipeline`：gateway 執行緒 → SPSC ring → 撮合執行緒（可選寫 journal）→ SPSC ring → 發布執行緒，
//!   量端到端延遲。`--rate` 以固定速率送單，並從「預定送出時間」開始計時，
//!   避免 coordinated omission（系統卡住時壓測端也跟著停，導致尾延遲被嚴重低估）。

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

use exchange_lab::affinity::pin_current_thread;
use exchange_lab::histogram::Histogram;
use exchange_lab::journal::JournalWriter;
use exchange_lab::ring;
use exchange_lab::workload::{generate, WorkloadConfig};
use exchange_lab::*;

struct Args {
    n: usize,
    mode: String,
    rate: u64,
    pin: bool,
    journal: Option<String>,
}

fn parse_args() -> Args {
    let mut a = Args {
        n: 5_000_000,
        mode: "all".into(),
        rate: 0,
        pin: false,
        journal: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("missing value for {k}"));
        match k.as_str() {
            "--n" => a.n = val().parse().expect("--n"),
            "--mode" => a.mode = val(),
            "--rate" => a.rate = val().parse().expect("--rate"),
            "--pin" => a.pin = true,
            "--journal" => a.journal = Some(val()),
            other => panic!("unknown arg {other}"),
        }
    }
    a
}

fn new_book() -> OrderBook {
    OrderBook::new(BookConfig::default())
}

fn bench_single(cmds: &[Command]) {
    println!("== single-thread matching ==");

    // 吞吐：不在迴圈內計時，避免 Instant::now() 本身的開銷干擾。
    let mut book = new_book();
    let mut trades = 0u64;
    let mut events = 0u64;
    let t0 = Instant::now();
    for c in cmds {
        book.execute(c, &mut |e| {
            events += 1;
            if let Event::Trade { .. } = e {
                trades += 1;
            }
        });
    }
    let dt = t0.elapsed();
    println!(
        "throughput: {:.2} M cmds/s ({} cmds, {} events, {} trades, {} resting at end) in {:?}",
        cmds.len() as f64 / dt.as_secs_f64() / 1e6,
        cmds.len(),
        events,
        trades,
        book.order_count(),
        dt
    );

    // 每筆延遲（含約 15~25ns 的計時開銷）。
    let mut book = new_book();
    let mut h = Histogram::default();
    let mut sink = 0u64;
    for c in cmds {
        let t = Instant::now();
        book.execute(c, &mut |_| sink += 1);
        h.record(t.elapsed().as_nanos() as u64);
    }
    black_box(sink);
    println!("per-command latency: {}", h.summary("ns"));
}

#[derive(Clone, Copy)]
struct Inbound {
    cmd: Command,
    /// 預定送出時間（不是實際送出時間）——這就是 coordinated omission 的修正。
    t: Instant,
}

#[derive(Clone, Copy)]
struct Outbound {
    t: Instant,
    events: u32,
}

fn bench_pipeline(cmds: Vec<Command>, args: &Args) {
    println!(
        "== pipeline: gateway -> ring -> engine{} -> ring -> publisher (rate={}, pin={}) ==",
        if args.journal.is_some() {
            "+journal"
        } else {
            ""
        },
        if args.rate == 0 {
            "unthrottled".to_string()
        } else {
            format!("{}/s", args.rate)
        },
        args.pin
    );
    let n = cmds.len();
    let (mut in_tx, mut in_rx) = ring::channel::<Inbound>(1 << 16);
    let (mut out_tx, mut out_rx) = ring::channel::<Outbound>(1 << 16);
    let pin = args.pin;
    let rate = args.rate;
    let journal = args.journal.clone();

    let publisher = thread::spawn(move || {
        if pin {
            pin_current_thread(3);
        }
        let mut h = Histogram::default();
        let mut events = 0u64;
        for _ in 0..n {
            let o = out_rx.pop();
            h.record(o.t.elapsed().as_nanos() as u64);
            events += o.events as u64;
        }
        (h, events)
    });

    let engine = thread::spawn(move || {
        if pin {
            pin_current_thread(2);
        }
        let mut book = new_book();
        let mut jw = journal.map(|p| JournalWriter::create(p).expect("open journal"));
        for seq in 0..n as u64 {
            let m = in_rx.pop();
            if let Some(j) = jw.as_mut() {
                // 先寫 journal 再撮合：輸入一旦被處理就一定可重播。
                j.append(seq, &m.cmd).expect("journal write");
            }
            let mut events = 0u32;
            book.execute(&m.cmd, &mut |_| events += 1);
            out_tx.push(Outbound { t: m.t, events });
        }
        if let Some(j) = jw.as_mut() {
            j.flush().expect("journal flush");
        }
    });

    if pin {
        pin_current_thread(1);
    }
    // 有限速時預留 10ms 讓消費端先進入 spin。
    let start = Instant::now() + Duration::from_millis(if rate == 0 { 0 } else { 10 });
    let interval_ns = 1_000_000_000u64.checked_div(rate).unwrap_or(0);
    for (i, cmd) in cmds.into_iter().enumerate() {
        let t = if rate == 0 {
            Instant::now()
        } else {
            let due = start + Duration::from_nanos(i as u64 * interval_ns);
            while Instant::now() < due {
                std::hint::spin_loop();
            }
            due
        };
        in_tx.push(Inbound { cmd, t });
    }

    engine.join().unwrap();
    let (h, events) = publisher.join().unwrap();
    let elapsed = Instant::now().saturating_duration_since(start);
    println!(
        "throughput: {:.2} M cmds/s ({} cmds, {} events)",
        n as f64 / elapsed.as_secs_f64() / 1e6,
        n,
        events
    );
    println!("end-to-end latency: {}", h.summary("ns"));
}

fn main() {
    let args = parse_args();
    let threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!(
        "generating {} commands (cores available: {threads})",
        args.n
    );
    let cmds = generate(args.n, &WorkloadConfig::default());

    if args.mode == "single" || args.mode == "all" {
        bench_single(&cmds);
    }
    if args.mode == "pipeline" || args.mode == "all" {
        if threads < 3 {
            println!("warning: pipeline wants 3 cores for its busy-spinning threads; results will be noisy");
        }
        bench_pipeline(cmds, &args);
    }
}
