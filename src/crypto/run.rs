//! 組裝拓樸、啟動所有執行緒、收集結果並做一致性檢查。
//!
//! ```text
//!  Gateway ×G ──(G×R SPSC)──▶ Risk ×R ──(R×M SPSC)──▶ Matcher ×M ──(M SPSC)──▶ MarketData
//!      ▲                       │  ▲                        │
//!      └──────(R×G SPSC)───────┘  └──────(M×R SPSC)────────┘
//!         執行回報                     結算回饋（成交/取消）
//! ```

use std::fmt::Write as _;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::gateway::{Gateway, GatewayIo, GatewayStats};
use super::matcher::{Matcher, MatcherIo, MatcherStats};
use super::mdata::{self, MdStats};
use super::model::*;
use super::risk::{RiskFinal, RiskIo, RiskShard};
use crate::affinity::pin_current_thread;
use crate::histogram::Histogram;
use crate::idle::Clock;
use crate::ring;

pub struct SimReport {
    pub cfg: SimConfig,
    pub symbols: Vec<SymbolSpec>,
    pub placement: Vec<usize>,
    pub wall: Duration,
    pub gateways: Vec<GatewayStats>,
    pub risks: Vec<RiskFinal>,
    pub matchers: Vec<MatcherStats>,
    pub md: MdStats,
    /// (檢查項目, 是否通過, 細節)
    pub checks: Vec<(String, bool, String)>,
}

impl SimReport {
    pub fn all_checks_pass(&self) -> bool {
        self.checks.iter().all(|c| c.1)
    }
}

type Matrix<T> = (Vec<Vec<ring::Producer<T>>>, Vec<Vec<ring::Consumer<T>>>);

fn matrix<T: Send>(rows: usize, cols: usize, cap: usize) -> Matrix<T> {
    let mut tx: Vec<Vec<_>> = (0..rows).map(|_| Vec::new()).collect();
    let mut rx: Vec<Vec<_>> = (0..cols).map(|_| Vec::new()).collect();
    for row in tx.iter_mut() {
        for col in rx.iter_mut() {
            let (p, c) = ring::channel(cap);
            row.push(p);
            col.push(c);
        }
    }
    // tx[row][col] 與 rx[col][row] 是同一條 ring 的兩端。
    (tx, rx)
}

pub fn run(cfg: SimConfig) -> SimReport {
    assert!(cfg.gateways > 0 && cfg.risk_shards > 0 && cfg.matchers > 0);
    assert!(cfg.symbols > 0 && cfg.users >= cfg.gateways);
    let cfg = Arc::new(cfg);
    let symbols = Arc::new(make_symbols(cfg.symbols));
    let placement = Arc::new(cfg.placement_map());
    let millis = cfg.duration.as_millis() as usize + 2_000;
    let path = Arc::new(price_path(&symbols, millis, cfg.seed));
    let cap = cfg.ring_capacity;
    let (g, r, m) = (cfg.gateways, cfg.risk_shards, cfg.matchers);

    let (gw_to_risk, mut risk_from_gw) = matrix(g, r, cap);
    let (risk_to_gw, mut gw_from_risk) = matrix(r, g, cap);
    let (risk_to_m, mut m_from_risk) = matrix(r, m, cap);
    let (m_to_risk, mut risk_from_m) = matrix(m, r, cap);
    let mut m_to_md = Vec::new();
    let mut md_from_m = Vec::new();
    for _ in 0..m {
        let (p, c) = ring::channel(cap);
        m_to_md.push(p);
        md_from_m.push(c);
    }

    let epoch = Instant::now();
    let clock = Clock::new(epoch);
    // 給所有執行緒 50ms 啟動，然後同時開始送單。
    let start_ns = 50_000_000;
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

    let md_handle = {
        let rx = std::mem::take(&mut md_from_m);
        let (n, idle, c) = (cfg.symbols, cfg.idle, core());
        thread::Builder::new()
            .name("md".into())
            .spawn(move || {
                if pin {
                    pin_current_thread(c);
                }
                mdata::run(rx, n, clock, idle)
            })
            .unwrap()
    };

    let mut m_handles = Vec::new();
    for (i, (to_risk, to_md)) in m_to_risk.into_iter().zip(m_to_md).enumerate() {
        let io = MatcherIo {
            from_risk: std::mem::take(&mut m_from_risk[i]),
            to_risk,
            to_md,
        };
        let matcher = Matcher::new(i, &cfg, &symbols, &placement, io);
        let c = core();
        m_handles.push(
            thread::Builder::new()
                .name(format!("matcher-{i}"))
                .spawn(move || {
                    if pin {
                        pin_current_thread(c);
                    }
                    matcher.run()
                })
                .unwrap(),
        );
    }

    let mut r_handles = Vec::new();
    for (i, (to_gw, to_m)) in risk_to_gw.into_iter().zip(risk_to_m).enumerate() {
        let io = RiskIo {
            from_gw: std::mem::take(&mut risk_from_gw[i]),
            from_m: std::mem::take(&mut risk_from_m[i]),
            to_m,
            to_gw,
        };
        let shard = RiskShard::new(i, cfg.clone(), symbols.clone(), placement.clone(), io);
        let c = core();
        r_handles.push(
            thread::Builder::new()
                .name(format!("risk-{i}"))
                .spawn(move || {
                    if pin {
                        pin_current_thread(c);
                    }
                    shard.run()
                })
                .unwrap(),
        );
    }

    let mut g_handles = Vec::new();
    for (i, to_risk) in gw_to_risk.into_iter().enumerate() {
        let io = GatewayIo {
            to_risk,
            from_risk: std::mem::take(&mut gw_from_risk[i]),
        };
        let gw = Gateway::new(i, cfg.clone(), &symbols, path.clone(), io, clock, start_ns);
        let c = core();
        g_handles.push(
            thread::Builder::new()
                .name(format!("gateway-{i}"))
                .spawn(move || {
                    if pin {
                        pin_current_thread(c);
                    }
                    gw.run()
                })
                .unwrap(),
        );
    }

    let gateways: Vec<_> = g_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let risks: Vec<_> = r_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let matchers: Vec<_> = m_handles.into_iter().map(|h| h.join().unwrap()).collect();
    let md = md_handle.join().unwrap();
    let wall = Duration::from_nanos(clock.now_ns().saturating_sub(start_ns));

    let cfg = Arc::try_unwrap(cfg).unwrap_or_else(|a| (*a).clone());
    let mut rep = SimReport {
        symbols: (*symbols).clone(),
        placement: (*placement).clone(),
        cfg,
        wall,
        gateways,
        risks,
        matchers,
        md,
        checks: vec![],
    };
    rep.checks = verify(&rep);
    rep
}

/// 跨所有執行緒的一致性檢查。只要任何一條訊息遺失、重複或處理錯誤，這裡就會失敗。
fn verify(rep: &SimReport) -> Vec<(String, bool, String)> {
    let cfg = &rep.cfg;
    let mut checks = vec![];

    // 1. 資產守恆：沒有手續費，成交是零和的，每種資產的總量（可用 + 凍結）必須不變。
    let n_assets = cfg.symbols + 1;
    let mut initial = vec![0i64; n_assets];
    for u in 0..cfg.users as UserId {
        for (a, total) in initial.iter_mut().enumerate() {
            *total += cfg.initial_balance(u, a as AssetId);
        }
    }
    let mut fin = vec![0i64; n_assets];
    for r in &rep.risks {
        for (i, b) in r.balances.iter().enumerate() {
            fin[i % r.n_assets] += b.available + b.locked;
        }
    }
    let diff: Vec<_> = (0..n_assets).filter(|&a| initial[a] != fin[a]).collect();
    checks.push((
        "資產守恆（每種資產 可用+凍結 總量不變）".into(),
        diff.is_empty(),
        if diff.is_empty() {
            format!("{n_assets} 種資產皆相符")
        } else {
            format!("不符的資產：{diff:?}")
        },
    ));

    // 2. 凍結金額 = 未成交訂單應凍結金額，且沒有負餘額。
    let ok = rep.risks.iter().all(|r| r.locks_consistent);
    checks.push((
        "凍結金額與未成交訂單一致、無負餘額".into(),
        ok,
        String::new(),
    ));

    // 3. 每個請求恰好一個回應。
    let req: u64 = rep
        .gateways
        .iter()
        .map(|g| g.sent_new + g.sent_cancel)
        .sum();
    let resp: u64 = rep.gateways.iter().map(|g| g.responses).sum();
    checks.push((
        "每個請求恰好一個回應".into(),
        req == resp,
        format!("請求 {req}，回應 {resp}"),
    ));

    // 4. 風控端未成交訂單數 = 撮合簿上的掛單數。
    let open: usize = rep.risks.iter().map(|r| r.open_orders).sum();
    let resting: usize = rep.matchers.iter().map(|m| m.resting_at_end).sum();
    checks.push((
        "風控未結訂單 = 撮合簿掛單".into(),
        open == resting,
        format!("風控 {open}，撮合簿 {resting}"),
    ));

    // 5. 每筆成交產生兩筆 fill，且行情看到的成交數相同。
    let trades: u64 = rep.matchers.iter().map(|m| m.trades).sum();
    let fills: u64 = rep.gateways.iter().map(|g| g.fills).sum();
    let md_trades: u64 = rep.md.per_symbol.iter().map(|s| s.trades).sum();
    checks.push((
        "fill = 2 × 成交 = 2 × 行情成交".into(),
        fills == 2 * trades && md_trades == trades,
        format!("成交 {trades}，fill {fills}，行情成交 {md_trades}"),
    ));
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

impl SimReport {
    pub fn render(&self) -> String {
        let c = &self.cfg;
        let mut o = String::new();
        let secs = self.wall.as_secs_f64();
        let threads = c.gateways + c.risk_shards + c.matchers + 1;
        let _ = writeln!(
            o,
            "== 拓樸 ==\n  gateway×{} → risk×{} → matcher×{} → md×1  （{} 條執行緒，{} 核心，idle={:?}，pin={}）",
            c.gateways,
            c.risk_shards,
            c.matchers,
            threads,
            thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
            c.idle,
            c.pin
        );
        let _ = writeln!(
            o,
            "  用戶 {}（做市商 {}），商品 {}，目標 {} 動作/秒，持續 {:?}，放置策略 {:?}",
            c.users,
            c.market_makers(),
            c.symbols,
            c.rate,
            c.duration,
            c.placement
        );

        let sum = |f: &dyn Fn(&GatewayStats) -> u64| self.gateways.iter().map(f).sum::<u64>();
        let (new, cancel) = (sum(&|g| g.sent_new), sum(&|g| g.sent_cancel));
        let trades: u64 = self.matchers.iter().map(|m| m.trades).sum();
        let _ = writeln!(o, "\n== 流量 ==");
        let _ = writeln!(
            o,
            "  請求 {}（新單 {}，撤單 {}）→ {:.0} 則/秒；成交 {}；被限流 {}",
            new + cancel,
            new,
            cancel,
            (new + cancel) as f64 / secs,
            trades,
            sum(&|g| g.throttled)
        );
        let mut rej = [0u64; super::msg::RejectCode::COUNT];
        for g in &self.gateways {
            for (i, x) in g.rejects.iter().enumerate() {
                rej[i] += x;
            }
        }
        let rej_s: Vec<_> = super::msg::RejectCode::ALL
            .iter()
            .filter(|r| rej[**r as usize] > 0)
            .map(|r| format!("{} {}", r.name(), rej[*r as usize]))
            .collect();
        let _ = writeln!(
            o,
            "  回報：ack {}，fill {}，完成/取消 {}，撤單失敗 {}，拒絕 [{}]",
            sum(&|g| g.acks),
            sum(&|g| g.fills),
            sum(&|g| g.done),
            sum(&|g| g.cancel_rejects),
            rej_s.join(", ")
        );
        let behind = self
            .gateways
            .iter()
            .map(|g| g.max_behind_ns)
            .max()
            .unwrap_or(0);
        let _ = writeln!(o, "  gateway 最大排程落後 {:.1}ms", behind as f64 / 1e6);

        let _ = writeln!(o, "\n== 延遲（從預定送出時間起算） ==");
        let mut all = Histogram::default();
        for g in &self.gateways {
            all.merge(&g.response_latency);
        }
        let _ = writeln!(o, "  請求→回應（4 跳往返）  {}", lat(&all));
        let _ = writeln!(
            o,
            "  請求→行情成交（3 跳）   {}",
            lat(&self.md.trade_latency)
        );

        let _ = writeln!(o, "\n== 分片負載 ==");
        for r in &self.risks {
            let _ = writeln!(
                o,
                "  risk-{}     用戶 {:>6}  請求 {:>9}  結算 {:>9}  餘額不足 {:>7}  未結訂單 {}",
                r.stats.id,
                r.stats.users,
                r.stats.requests,
                r.stats.settlements,
                r.stats.rejected_balance,
                r.open_orders
            );
        }
        let total_cmds: u64 = self.matchers.iter().map(|m| m.commands).sum::<u64>().max(1);
        for m in &self.matchers {
            let names: Vec<_> = m
                .symbols
                .iter()
                .map(|s| self.symbols[*s as usize].name.as_str())
                .collect();
            let _ = writeln!(
                o,
                "  matcher-{}  指令 {:>9} ({:>4.1}%)  成交 {:>8}  掛單 {:>6}  商品 {}",
                m.id,
                m.commands,
                100.0 * m.commands as f64 / total_cmds as f64,
                m.trades,
                m.resting_at_end,
                names.join(" ")
            );
        }

        let _ = writeln!(o, "\n== 商品 ==");
        for (s, md) in self.symbols.iter().zip(&self.md.per_symbol) {
            let fmt =
                |x: Option<(i64, u64)>| x.map(|(p, q)| format!("{p}×{q}")).unwrap_or("-".into());
            let _ = writeln!(
                o,
                "  {:<10} matcher-{}  成交 {:>8}  量 {:>9}  最新 {:>6}  買 {:>12}  賣 {:>12}",
                s.name,
                self.placement[s.id as usize],
                md.trades,
                md.volume,
                md.last_price.map(|p| p.to_string()).unwrap_or("-".into()),
                fmt(md.bid),
                fmt(md.ask)
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
