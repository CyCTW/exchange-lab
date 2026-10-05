use exchange_lab::journal::{JournalReader, JournalWriter};
use exchange_lab::ring;
use exchange_lab::workload::{generate, WorkloadConfig};
use exchange_lab::*;

fn book() -> OrderBook {
    OrderBook::new(BookConfig {
        min_price: 0,
        max_price: 1_000,
        max_orders: 1_000,
    })
}

fn run(b: &mut OrderBook, cmd: Command) -> Vec<Event> {
    let mut v = Vec::new();
    b.execute(&cmd, &mut |e| v.push(e));
    v
}

fn limit(id: OrderId, side: Side, price: Price, qty: Qty) -> Command {
    Command::NewOrder {
        id,
        side,
        price,
        qty,
        tif: TimeInForce::Gtc,
    }
}

fn ioc(id: OrderId, side: Side, price: Price, qty: Qty) -> Command {
    Command::NewOrder {
        id,
        side,
        price,
        qty,
        tif: TimeInForce::Ioc,
    }
}

fn trades(ev: &[Event]) -> Vec<(OrderId, Price, Qty)> {
    ev.iter()
        .filter_map(|e| match *e {
            Event::Trade {
                maker, price, qty, ..
            } => Some((maker, price, qty)),
            _ => None,
        })
        .collect()
}

#[test]
fn resting_orders_set_best_prices() {
    let mut b = book();
    run(&mut b, limit(1, Side::Buy, 99, 10));
    run(&mut b, limit(2, Side::Buy, 100, 5));
    run(&mut b, limit(3, Side::Sell, 102, 7));
    run(&mut b, limit(4, Side::Sell, 101, 3));
    assert_eq!(b.best(Side::Buy), Some((100, 5)));
    assert_eq!(b.best(Side::Sell), Some((101, 3)));
    assert_eq!(b.depth(Side::Buy, 10), vec![(100, 5), (99, 10)]);
    assert_eq!(b.depth(Side::Sell, 10), vec![(101, 3), (102, 7)]);
}

#[test]
fn price_then_time_priority() {
    let mut b = book();
    run(&mut b, limit(1, Side::Sell, 101, 5));
    run(&mut b, limit(2, Side::Sell, 100, 5));
    run(&mut b, limit(3, Side::Sell, 100, 5));
    let ev = run(&mut b, limit(10, Side::Buy, 101, 12));
    // 先吃較好的價格 100（依時間 2 再 3），再吃 101。
    assert_eq!(trades(&ev), vec![(2, 100, 5), (3, 100, 5), (1, 101, 2)]);
    assert_eq!(b.best(Side::Sell), Some((101, 3)));
    assert_eq!(b.best(Side::Buy), None);
}

#[test]
fn aggressor_remainder_rests_at_its_limit() {
    let mut b = book();
    run(&mut b, limit(1, Side::Buy, 100, 4));
    let ev = run(&mut b, limit(2, Side::Sell, 99, 10));
    assert_eq!(trades(&ev), vec![(1, 100, 4)]);
    assert_eq!(b.best(Side::Sell), Some((99, 6)));
    assert_eq!(b.best(Side::Buy), None);
}

#[test]
fn ioc_remainder_is_cancelled() {
    let mut b = book();
    run(&mut b, limit(1, Side::Sell, 100, 4));
    let ev = run(&mut b, ioc(2, Side::Buy, 100, 10));
    assert_eq!(trades(&ev), vec![(1, 100, 4)]);
    assert_eq!(
        ev.last(),
        Some(&Event::Cancelled {
            id: 2,
            remaining: 6
        })
    );
    assert_eq!(b.order_count(), 0);
}

#[test]
fn cancel_middle_of_queue_and_best_level() {
    let mut b = book();
    run(&mut b, limit(1, Side::Buy, 100, 1));
    run(&mut b, limit(2, Side::Buy, 100, 2));
    run(&mut b, limit(3, Side::Buy, 100, 3));
    run(&mut b, limit(4, Side::Buy, 37, 9));
    assert_eq!(
        run(&mut b, Command::Cancel { id: 2 }),
        vec![Event::Cancelled {
            id: 2,
            remaining: 2
        }]
    );
    assert_eq!(b.best(Side::Buy), Some((100, 4)));
    let ev = run(&mut b, ioc(9, Side::Sell, 100, 10));
    assert_eq!(trades(&ev), vec![(1, 100, 1), (3, 100, 3)]);
    // 最佳價清空後，透過 bitmap 往下跳到 37。
    assert_eq!(b.best(Side::Buy), Some((37, 9)));
    run(&mut b, Command::Cancel { id: 4 });
    assert_eq!(b.best(Side::Buy), None);
}

#[test]
fn rejects() {
    let mut b = book();
    let r = |id, reason| vec![Event::Rejected { id, reason }];
    assert_eq!(
        run(&mut b, limit(1, Side::Buy, 100, 0)),
        r(1, RejectReason::InvalidQty)
    );
    assert_eq!(
        run(&mut b, limit(1, Side::Buy, 5_000, 1)),
        r(1, RejectReason::PriceOutOfBand)
    );
    run(&mut b, limit(1, Side::Buy, 100, 1));
    assert_eq!(
        run(&mut b, limit(1, Side::Buy, 100, 1)),
        r(1, RejectReason::DuplicateId)
    );
    assert_eq!(
        run(&mut b, Command::Cancel { id: 77 }),
        r(77, RejectReason::UnknownOrder)
    );
}

#[test]
fn book_full() {
    let mut b = OrderBook::new(BookConfig {
        min_price: 0,
        max_price: 10,
        max_orders: 2,
    });
    run(&mut b, limit(1, Side::Buy, 1, 1));
    run(&mut b, limit(2, Side::Buy, 2, 1));
    assert_eq!(
        run(&mut b, limit(3, Side::Buy, 3, 1)),
        vec![Event::Rejected {
            id: 3,
            reason: RejectReason::BookFull
        }]
    );
    run(&mut b, Command::Cancel { id: 1 });
    run(&mut b, limit(3, Side::Buy, 3, 1));
    assert_eq!(b.order_count(), 2);
}

#[test]
fn bitmap_search_across_word_boundaries() {
    let mut b = book();
    for (id, p) in [(1, 0), (2, 63), (3, 64), (4, 129), (5, 1_000)] {
        run(&mut b, limit(id, Side::Sell, p, 1));
    }
    let prices: Vec<_> = b.depth(Side::Sell, 10).into_iter().map(|x| x.0).collect();
    assert_eq!(prices, vec![0, 63, 64, 129, 1_000]);
    let ev = run(&mut b, ioc(9, Side::Buy, 1_000, 5));
    assert_eq!(
        trades(&ev).iter().map(|t| t.1).collect::<Vec<_>>(),
        vec![0, 63, 64, 129, 1_000]
    );
    assert_eq!(b.best(Side::Sell), None);
}

/// 用一個樸素、明顯正確的實作當作 oracle，跑大量隨機指令比對輸出。
#[test]
fn matches_naive_reference_implementation() {
    #[derive(Clone)]
    struct Naive {
        // (side, price, seq, id, qty)
        orders: Vec<(Side, Price, u64, OrderId, Qty)>,
        seq: u64,
    }
    impl Naive {
        fn exec(&mut self, cmd: &Command, out: &mut Vec<Event>) {
            match *cmd {
                Command::Cancel { id } => match self.orders.iter().position(|o| o.3 == id) {
                    Some(i) => {
                        let o = self.orders.remove(i);
                        out.push(Event::Cancelled { id, remaining: o.4 });
                    }
                    None => out.push(Event::Rejected {
                        id,
                        reason: RejectReason::UnknownOrder,
                    }),
                },
                Command::NewOrder {
                    id,
                    side,
                    price,
                    mut qty,
                    tif,
                } => {
                    out.push(Event::Accepted { id });
                    loop {
                        let best = self
                            .orders
                            .iter()
                            .enumerate()
                            .filter(|(_, o)| o.0 != side)
                            .filter(|(_, o)| {
                                if side == Side::Buy {
                                    o.1 <= price
                                } else {
                                    o.1 >= price
                                }
                            })
                            .min_by_key(|(_, o)| (if side == Side::Buy { o.1 } else { -o.1 }, o.2))
                            .map(|(i, _)| i);
                        let Some(i) = best else { break };
                        if qty == 0 {
                            break;
                        }
                        let fill = qty.min(self.orders[i].4);
                        qty -= fill;
                        self.orders[i].4 -= fill;
                        out.push(Event::Trade {
                            taker: id,
                            maker: self.orders[i].3,
                            taker_side: side,
                            price: self.orders[i].1,
                            qty: fill,
                        });
                        if self.orders[i].4 == 0 {
                            self.orders.remove(i);
                        }
                    }
                    if qty > 0 {
                        match tif {
                            TimeInForce::Ioc => out.push(Event::Cancelled { id, remaining: qty }),
                            TimeInForce::Gtc => {
                                self.seq += 1;
                                self.orders.push((side, price, self.seq, id, qty));
                            }
                        }
                    }
                }
            }
        }
    }

    let cfg = WorkloadConfig {
        seed: 7,
        max_live: 300,
        ..Default::default()
    };
    let cmds = generate(20_000, &cfg);
    let mut fast = OrderBook::new(BookConfig::default());
    let mut naive = Naive {
        orders: vec![],
        seq: 0,
    };
    for (i, c) in cmds.iter().enumerate() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        fast.execute(c, &mut |e| a.push(e));
        naive.exec(c, &mut b);
        assert_eq!(a, b, "divergence at command #{i}: {c:?}");
    }
    assert_eq!(fast.order_count(), naive.orders.len());
}

#[test]
fn journal_replay_reproduces_identical_state() {
    let dir = std::env::temp_dir().join(format!("exchange-lab-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("replay.journal");
    let cmds = generate(50_000, &WorkloadConfig::default());

    let mut live = OrderBook::new(BookConfig::default());
    let mut live_events = Vec::new();
    let mut j = JournalWriter::create(&path).unwrap();
    for (seq, c) in cmds.iter().enumerate() {
        j.append(seq as u64, c).unwrap();
        live.execute(c, &mut |e| live_events.push(e));
    }
    j.sync().unwrap();

    let mut replica = OrderBook::new(BookConfig::default());
    let mut replica_events = Vec::new();
    for (expected_seq, rec) in JournalReader::open(&path).unwrap().enumerate() {
        let (seq, c) = rec.unwrap();
        assert_eq!(seq, expected_seq as u64);
        replica.execute(&c, &mut |e| replica_events.push(e));
    }
    assert_eq!(live_events, replica_events);
    assert_eq!(live.fingerprint(), replica.fingerprint());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn spsc_ring_preserves_order_across_threads() {
    let (mut tx, mut rx) = ring::channel::<u64>(64);
    let n = 1_000_000u64;
    let h = std::thread::spawn(move || {
        for i in 0..n {
            tx.push(i);
        }
    });
    for i in 0..n {
        assert_eq!(rx.pop(), i);
    }
    h.join().unwrap();
    assert!(rx.try_pop().is_none());
}

#[test]
fn spsc_ring_drops_unconsumed_items() {
    use std::sync::Arc;
    let marker = Arc::new(());
    {
        let (mut tx, mut rx) = ring::channel::<Arc<()>>(8);
        for _ in 0..5 {
            tx.try_push(marker.clone()).unwrap();
        }
        drop(rx.try_pop());
        assert_eq!(Arc::strong_count(&marker), 5);
    }
    assert_eq!(Arc::strong_count(&marker), 1);
}

fn crosses(ev: &[Event]) -> Vec<(OrderId, OrderId, Price, Qty)> {
    ev.iter()
        .filter_map(|e| match *e {
            Event::Cross {
                buy,
                sell,
                price,
                qty,
            } => Some((buy, sell, price, qty)),
            _ => None,
        })
        .collect()
}

#[test]
fn auction_mode_rests_crossing_orders_and_rejects_ioc() {
    let mut b = book();
    b.set_auction(true);
    run(&mut b, limit(1, Side::Buy, 105, 10));
    run(&mut b, limit(2, Side::Sell, 100, 4));
    assert_eq!(b.best(Side::Buy), Some((105, 10)));
    assert_eq!(b.best(Side::Sell), Some((100, 4)));
    assert_eq!(
        run(&mut b, ioc(3, Side::Buy, 110, 1)),
        vec![Event::Rejected {
            id: 3,
            reason: RejectReason::InvalidPhase
        }]
    );
}

#[test]
fn uncross_picks_max_volume_price_and_fills_in_priority_order() {
    let mut b = book();
    b.set_auction(true);
    // 買：102×5（id1）、101×5（id2）、100×5（id3）；賣：99×4（id4）、100×4（id5）、101×10（id6）
    run(&mut b, limit(1, Side::Buy, 102, 5));
    run(&mut b, limit(2, Side::Buy, 101, 5));
    run(&mut b, limit(3, Side::Buy, 100, 5));
    run(&mut b, limit(4, Side::Sell, 99, 4));
    run(&mut b, limit(5, Side::Sell, 100, 4));
    run(&mut b, limit(6, Side::Sell, 101, 10));
    // 價格 100：買 15、賣 8 → 8；價格 101：買 10、賣 18 → 10（最大）
    let mut ev = Vec::new();
    let r = b.uncross(100, &mut |e| ev.push(e));
    assert_eq!(r, Some((101, 10)));
    assert_eq!(
        crosses(&ev),
        vec![
            (1, 4, 101, 4),
            (1, 5, 101, 1),
            (2, 5, 101, 3),
            (2, 6, 101, 2)
        ]
    );
    b.set_auction(false);
    assert_eq!(b.best(Side::Buy), Some((100, 5)));
    assert_eq!(b.best(Side::Sell), Some((101, 8)));
}

#[test]
fn uncross_ties_use_reference_price_when_inside_range() {
    let mut sink = |_| {};
    // 90~110 任何價格成交量、買賣差都相同 → 用參考價
    let mut b = book();
    b.set_auction(true);
    run(&mut b, limit(1, Side::Buy, 110, 5));
    run(&mut b, limit(2, Side::Sell, 90, 5));
    assert_eq!(b.uncross(97, &mut sink), Some((97, 5)));
    // 參考價在區間外 → 取最接近參考價的那一端
    let mut b = book();
    b.set_auction(true);
    run(&mut b, limit(1, Side::Buy, 110, 5));
    run(&mut b, limit(2, Side::Sell, 90, 5));
    assert_eq!(b.uncross(200, &mut sink), Some((110, 5)));
}

#[test]
fn uncross_on_uncrossed_book_does_nothing() {
    let mut b = book();
    b.set_auction(true);
    run(&mut b, limit(1, Side::Buy, 99, 5));
    run(&mut b, limit(2, Side::Sell, 100, 5));
    assert_eq!(b.uncross(100, &mut |_| {}), None);
    assert_eq!(b.order_count(), 2);
}

/// 與暴力解比對：隨機掛單，檢查競價價格符合規則、成交量正確、撮合後不再交叉、數量守恆。
#[test]
fn uncross_matches_brute_force() {
    let mut rng = exchange_lab::workload::Rng::new(99);
    for round in 0..300 {
        let mut b = book();
        b.set_auction(true);
        let mut orders = vec![];
        for id in 1..=(2 + rng.below(40)) {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = 480 + rng.below(40) as Price;
            let qty = 1 + rng.below(50);
            orders.push((side, price, qty));
            run(&mut b, limit(id, side, price, qty));
        }
        let reference = 480 + rng.below(40) as Price;
        let total_before: Qty = orders.iter().map(|o| o.2).sum();

        // 暴力解：所有可能價格（含參考價）中依規則挑選
        let mut best: Option<(Qty, i64, i64, i64, Price)> = None;
        let prices: Vec<Price> = (470..=530).collect();
        let (bb, ba) = (
            b.best(Side::Buy).map(|x| x.0),
            b.best(Side::Sell).map(|x| x.0),
        );
        let crossed = matches!((bb, ba), (Some(x), Some(y)) if x >= y);
        for &p in &prices {
            let buy: Qty = orders
                .iter()
                .filter(|o| o.0 == Side::Buy && o.1 >= p)
                .map(|o| o.2)
                .sum();
            let sell: Qty = orders
                .iter()
                .filter(|o| o.0 == Side::Sell && o.1 <= p)
                .map(|o| o.2)
                .sum();
            let exec = buy.min(sell);
            let level = orders.iter().any(|o| o.1 == p) || p == reference;
            if exec == 0 || !level {
                continue;
            }
            let key = (
                exec,
                -((buy as i64 - sell as i64).abs()),
                -(p - reference).abs(),
                -p,
                p,
            );
            if best.is_none_or(|k| (key.0, key.1, key.2, key.3) > (k.0, k.1, k.2, k.3)) {
                best = Some(key);
            }
        }
        let mut ev = vec![];
        let got = b.uncross(reference, &mut |e| ev.push(e));
        let want = if crossed {
            best.map(|k| (k.4, k.0))
        } else {
            None
        };
        assert_eq!(got, want, "round {round}");
        b.set_auction(false);
        let after: Qty = b.snapshot().iter().map(|o| o.3).sum();
        let traded: Qty = crosses(&ev).iter().map(|c| c.3).sum();
        assert_eq!(after + 2 * traded, total_before, "round {round}");
        if let (Some((x, _)), Some((y, _))) = (b.best(Side::Buy), b.best(Side::Sell)) {
            assert!(x < y, "still crossed after uncross, round {round}");
        }
    }
}

#[test]
fn cancel_matching_removes_only_selected_orders() {
    let mut b = book();
    for id in 1..=10 {
        let side = if id % 2 == 0 { Side::Buy } else { Side::Sell };
        let price = if side == Side::Buy { 90 } else { 110 };
        run(&mut b, limit(id, side, price, 1));
    }
    let mut ev = vec![];
    let n = b.cancel_matching(|id| id <= 4, &mut |e| ev.push(e));
    assert_eq!(n, 4);
    assert_eq!(b.order_count(), 6);
    assert!(ev
        .iter()
        .all(|e| matches!(e, Event::Cancelled { id, .. } if *id <= 4)));
}
