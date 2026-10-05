//! 傳統交易所模擬。
//!
//! ```text
//! cargo run --release --bin exchange -- [--gateways G] [--partitions P] [--members N]
//!     [--instruments N] [--rate ACTIONS_PER_SEC] [--secs N] [--placement modulo|balanced]
//!     [--zipf S] [--idle spin|backoff] [--pin] [--seed N]
//!     [--journal PATH | --no-journal] [--no-halt] [--no-disconnect]
//! ```
//!
//! 預設把 journal 寫到暫存目錄，結束時重播驗證後刪除；指定 `--journal PATH` 則保留檔案。

use std::path::PathBuf;
use std::time::Duration;

use exchange_lab::exchange::{self, ExchangeConfig, Placement};
use exchange_lab::idle::IdleKind;

fn main() {
    let mut cfg = ExchangeConfig::default();
    let mut journal: Option<PathBuf> = None;
    let mut no_journal = false;
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("missing value for {k}"));
        match k.as_str() {
            "--gateways" => cfg.gateways = val().parse().unwrap(),
            "--partitions" => cfg.partitions = val().parse().unwrap(),
            "--members" => cfg.members = val().parse().unwrap(),
            "--instruments" => cfg.instruments = val().parse().unwrap(),
            "--rate" => cfg.rate = val().parse().unwrap(),
            "--secs" => cfg.duration = Duration::from_secs_f64(val().parse().unwrap()),
            "--seed" => cfg.seed = val().parse().unwrap(),
            "--zipf" => cfg.zipf_s = val().parse().unwrap(),
            "--pin" => cfg.pin = true,
            "--journal" => journal = Some(val().into()),
            "--no-journal" => no_journal = true,
            "--no-halt" => cfg.halt = false,
            "--no-disconnect" => cfg.disconnect = false,
            "--placement" => {
                cfg.placement = match val().as_str() {
                    "modulo" => Placement::Modulo,
                    "balanced" => Placement::Balanced,
                    v => panic!("unknown placement {v}"),
                }
            }
            "--idle" => {
                cfg.idle = match val().as_str() {
                    "spin" => IdleKind::Spin,
                    "backoff" => IdleKind::Backoff,
                    v => panic!("unknown idle {v}"),
                }
            }
            other => panic!("unknown arg {other}"),
        }
    }
    let temp = journal.is_none() && !no_journal;
    if !no_journal {
        cfg.journal = Some(journal.unwrap_or_else(|| {
            std::env::temp_dir().join(format!("exchange-lab-{}.journal", std::process::id()))
        }));
    }
    let path = cfg.journal.clone();
    let report = exchange::run(cfg);
    print!("{}", report.render());
    if let (true, Some(p)) = (temp, path) {
        let _ = std::fs::remove_file(p);
    }
    if !report.all_checks_pass() {
        std::process::exit(1);
    }
}
