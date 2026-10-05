//! 端到端模擬：不同拓樸下，跨執行緒的一致性檢查（資產守恆、每個請求恰好一個回應等）都必須成立。

use std::time::Duration;

use exchange_lab::crypto::{self as sim, Placement, SimConfig};
use exchange_lab::idle::IdleKind;

fn small(gateways: usize, risk: usize, matchers: usize) -> SimConfig {
    SimConfig {
        gateways,
        risk_shards: risk,
        matchers,
        users: 600,
        symbols: 5,
        rate: 30_000,
        duration: Duration::from_millis(300),
        idle: IdleKind::Backoff,
        ring_capacity: 256, // 小佇列：逼出背壓路徑，驗證不會死結
        max_orders_per_book: 1 << 14,
        ..Default::default()
    }
}

fn check(cfg: SimConfig) {
    let label = format!("G{} R{} M{}", cfg.gateways, cfg.risk_shards, cfg.matchers);
    let rep = sim::run(cfg);
    let req: u64 = rep
        .gateways
        .iter()
        .map(|g| g.sent_new + g.sent_cancel)
        .sum();
    assert!(req > 1_000, "{label}: too little traffic ({req})");
    assert!(
        rep.matchers.iter().map(|m| m.trades).sum::<u64>() > 0,
        "{label}: no trades"
    );
    assert!(rep.all_checks_pass(), "{label}:\n{}", rep.render());
}

#[test]
fn single_shard_topology() {
    check(small(1, 1, 1));
}

#[test]
fn sharded_topology() {
    check(small(2, 2, 3));
}

#[test]
fn uneven_topology_with_balanced_placement() {
    let mut cfg = small(3, 2, 2);
    cfg.placement = Placement::Balanced;
    check(cfg);
}

#[test]
fn heavy_throttling_and_tiny_queues() {
    let mut cfg = small(2, 3, 2);
    cfg.ring_capacity = 4;
    cfg.mm_msgs_per_sec = 200;
    cfg.user_msgs_per_sec = 5;
    let rep = sim::run(cfg);
    assert!(rep.gateways.iter().map(|g| g.throttled).sum::<u64>() > 0);
    assert!(rep.all_checks_pass(), "{}", rep.render());
}

#[test]
fn balanced_placement_evens_out_zipf_load() {
    let cfg = SimConfig {
        symbols: 8,
        matchers: 2,
        placement: Placement::Balanced,
        ..Default::default()
    };
    let map = cfg.placement_map();
    let w = cfg.zipf_weights();
    let mut load = [0.0; 2];
    for (s, m) in map.iter().enumerate() {
        load[*m] += w[s];
    }
    let share = load[0] / (load[0] + load[1]);
    assert!((0.45..0.55).contains(&share), "{share}");
}
