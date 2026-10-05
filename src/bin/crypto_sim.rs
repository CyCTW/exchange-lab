//! 多用戶、多商品交易所模擬。
//!
//! ```text
//! cargo run --release --bin crypto_sim -- [--gateways G] [--risk R] [--matchers M]
//!     [--users U] [--symbols S] [--rate ACTIONS_PER_SEC] [--secs N]
//!     [--placement modulo|balanced] [--zipf S] [--idle spin|backoff] [--pin] [--seed N]
//!     [--mm-limit MSGS_PER_SEC] [--user-limit MSGS_PER_SEC]
//! ```

use std::time::Duration;

use exchange_lab::crypto::{self as sim, Placement, SimConfig};
use exchange_lab::idle::IdleKind;

fn main() {
    let mut cfg = SimConfig::default();
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("missing value for {k}"));
        match k.as_str() {
            "--gateways" => cfg.gateways = val().parse().unwrap(),
            "--risk" => cfg.risk_shards = val().parse().unwrap(),
            "--matchers" => cfg.matchers = val().parse().unwrap(),
            "--users" => cfg.users = val().parse().unwrap(),
            "--symbols" => cfg.symbols = val().parse().unwrap(),
            "--rate" => cfg.rate = val().parse().unwrap(),
            "--secs" => cfg.duration = Duration::from_secs_f64(val().parse().unwrap()),
            "--seed" => cfg.seed = val().parse().unwrap(),
            "--zipf" => cfg.zipf_s = val().parse().unwrap(),
            "--pin" => cfg.pin = true,
            "--mm-limit" => cfg.mm_msgs_per_sec = val().parse().unwrap(),
            "--user-limit" => cfg.user_msgs_per_sec = val().parse().unwrap(),
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
    let report = sim::run(cfg);
    print!("{}", report.render());
    if !report.all_checks_pass() {
        std::process::exit(1);
    }
}
