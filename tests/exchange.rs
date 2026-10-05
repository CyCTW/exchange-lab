//! 傳統交易所端到端測試：不同拓樸下，所有一致性檢查（含 journal 重播的確定性）都必須成立。

use std::time::Duration;

use exchange_lab::exchange::{self, ExchangeConfig, Placement};
use exchange_lab::idle::IdleKind;

fn small(gateways: usize, partitions: usize, name: &str) -> ExchangeConfig {
    ExchangeConfig {
        gateways,
        partitions,
        members: 24,
        instruments: 12,
        rate: 40_000,
        duration: Duration::from_millis(600),
        idle: IdleKind::Backoff,
        ring_capacity: 256,
        max_orders_per_book: 1 << 14,
        journal: Some(std::env::temp_dir().join(format!(
            "exchange-lab-test-{}-{name}.journal",
            std::process::id()
        ))),
        ..Default::default()
    }
}

fn check(cfg: ExchangeConfig) -> exchange::ExchangeReport {
    let path = cfg.journal.clone();
    let rep = exchange::run(cfg);
    if let Some(p) = path {
        let _ = std::fs::remove_file(p);
    }
    let crosses: u64 = rep.partitions.iter().map(|p| p.stats.crosses).sum();
    let trades: u64 = rep.partitions.iter().map(|p| p.stats.trades).sum();
    assert!(
        trades > 0 && crosses > 0,
        "expected both continuous and auction trades:\n{}",
        rep.render()
    );
    assert!(rep.replay.is_some());
    assert!(rep.all_checks_pass(), "{}", rep.render());
    rep
}

#[test]
fn single_partition() {
    check(small(1, 1, "single"));
}

#[test]
fn multiple_gateways_and_partitions() {
    let rep = check(small(2, 3, "multi"));
    // 盤中暫停的商品有恢復競價、斷線的會員委託被撤掉
    assert!(rep.md.per_inst[0].halts == 1);
    assert!(
        rep.partitions
            .iter()
            .map(|p| p.stats.mass_cancelled)
            .sum::<u64>()
            > 0
    );
}

#[test]
fn tiny_queues_do_not_deadlock() {
    let mut cfg = small(3, 2, "tiny");
    cfg.ring_capacity = 4;
    cfg.placement = Placement::Modulo;
    check(cfg);
}

#[test]
fn pre_trade_risk_blocks_bad_orders_before_sequencing() {
    let mut cfg = small(2, 2, "risk");
    cfg.fat_finger_ppm = 50_000;
    cfg.credit_limit_broker = 50_000_000;
    cfg.member_msgs_per_sec = 200;
    let rep = check(cfg);
    let gw = |i: usize| rep.gateways.iter().map(|g| g.gw_rejects[i]).sum::<u64>();
    assert!(gw(0) > 0, "throttled");
    assert!(gw(1) > 0, "max qty");
    assert!(gw(2) > 0, "price collar");
    assert!(gw(3) > 0, "credit");
}

#[test]
fn replay_is_deterministic_across_runs() {
    let cfg = small(2, 2, "replay");
    let path = cfg.journal.clone().unwrap();
    let rep = exchange::run(cfg.clone());
    let insts = cfg.instruments_list();
    let placement = cfg.placement_map();
    let again = exchange::replay(&cfg, &insts, &placement, &path).unwrap();
    let _ = std::fs::remove_file(&path);
    for (a, b) in rep.replay.unwrap().partitions.iter().zip(&again.partitions) {
        assert_eq!(a.hash, b.hash);
        assert_eq!(a.books, b.books);
    }
}
