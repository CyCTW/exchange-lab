//! 撮合分片執行緒：依「商品」分片，擁有分配給它的所有商品的限價簿。
//!
//! 只關心價格與數量，不知道任何帳戶餘額——資金在風控分片就已經凍結好了。
//! 成交結果拆成兩筆 Fill（taker、maker 各一筆），各自送回該用戶所屬的風控分片。

use super::model::*;
use super::msg::*;
use crate::idle::Idle;
use crate::orderbook::{BookConfig, OrderBook};
use crate::ring::{Consumer, Producer};
use crate::types::*;

pub struct MatcherIo {
    pub from_risk: Vec<Consumer<ToMatcher>>,
    pub to_risk: Vec<Producer<Settle>>,
    pub to_md: Producer<MdMsg>,
}

#[derive(Default, Debug)]
pub struct MatcherStats {
    pub id: usize,
    pub symbols: Vec<SymbolId>,
    pub commands: u64,
    pub trades: u64,
    pub resting_at_end: usize,
}

type Top = (Option<(Price, Qty)>, Option<(Price, Qty)>);

pub struct Matcher {
    risk_shards: usize,
    books: Vec<Option<OrderBook>>,
    last_top: Vec<Top>,
    io: MatcherIo,
    events: Vec<Event>,
    idle: Idle,
    stats: MatcherStats,
    shutdowns: usize,
}

impl Matcher {
    pub fn new(
        id: usize,
        cfg: &SimConfig,
        symbols: &[SymbolSpec],
        placement: &[usize],
        io: MatcherIo,
    ) -> Self {
        let mut owned = vec![];
        let books = symbols
            .iter()
            .map(|s| {
                (placement[s.id as usize] == id).then(|| {
                    owned.push(s.id);
                    OrderBook::new(BookConfig {
                        min_price: s.min_price,
                        max_price: s.max_price,
                        max_orders: cfg.max_orders_per_book,
                    })
                })
            })
            .collect();
        Matcher {
            risk_shards: cfg.risk_shards,
            books,
            last_top: vec![(None, None); symbols.len()],
            io,
            events: Vec::with_capacity(256),
            idle: Idle::new(cfg.idle),
            stats: MatcherStats {
                id,
                symbols: owned,
                ..Default::default()
            },
            shutdowns: 0,
        }
    }

    pub fn run(mut self) -> MatcherStats {
        while self.shutdowns < self.risk_shards {
            let mut work = 0;
            for r in 0..self.io.from_risk.len() {
                for _ in 0..256 {
                    let Some(m) = self.io.from_risk[r].try_pop() else {
                        break;
                    };
                    work += 1;
                    self.on_command(m);
                }
            }
            if work == 0 {
                self.idle.idle();
            } else {
                self.idle.reset();
            }
        }
        for r in 0..self.risk_shards {
            self.push_risk(r, Settle::Shutdown);
        }
        self.push_md(MdMsg::Shutdown);
        self.stats.resting_at_end = self.books.iter().flatten().map(|b| b.order_count()).sum();
        self.stats
    }

    fn on_command(&mut self, m: ToMatcher) {
        let (symbol, cmd, t, is_cancel) = match m {
            ToMatcher::New {
                order_id,
                symbol,
                side,
                price,
                qty,
                tif,
                t,
            } => (
                symbol,
                Command::NewOrder {
                    id: order_id,
                    side,
                    price,
                    qty,
                    tif,
                },
                t,
                false,
            ),
            ToMatcher::Cancel {
                order_id,
                symbol,
                t,
            } => (symbol, Command::Cancel { id: order_id }, t, true),
            ToMatcher::Shutdown => {
                self.shutdowns += 1;
                return;
            }
        };
        self.stats.commands += 1;

        let mut events = std::mem::take(&mut self.events);
        events.clear();
        let book = self.books[symbol as usize]
            .as_mut()
            .expect("command routed to the wrong matcher");
        book.execute(&cmd, &mut |e| events.push(e));
        let top = (book.best(Side::Buy), book.best(Side::Sell));

        for e in &events {
            match *e {
                Event::Accepted { id } => self.settle(id, Settle::Ack { order_id: id, t }),
                Event::Rejected { id, reason } => {
                    let s = if is_cancel {
                        Settle::CancelReject { order_id: id, t }
                    } else {
                        Settle::Rejected {
                            order_id: id,
                            reason,
                            t,
                        }
                    };
                    self.settle(id, s);
                }
                Event::Trade {
                    taker,
                    maker,
                    taker_side,
                    price,
                    qty,
                } => {
                    self.stats.trades += 1;
                    self.settle(
                        taker,
                        Settle::Fill {
                            order_id: taker,
                            price,
                            qty,
                            maker: false,
                            t,
                        },
                    );
                    self.settle(
                        maker,
                        Settle::Fill {
                            order_id: maker,
                            price,
                            qty,
                            maker: true,
                            t,
                        },
                    );
                    self.push_md(MdMsg::Trade {
                        symbol,
                        price,
                        qty,
                        taker_side,
                        t,
                    });
                }
                Event::Cross { .. } => unreachable!("no auctions in the crypto model"),
                Event::Cancelled { id, remaining } => self.settle(
                    id,
                    Settle::Done {
                        order_id: id,
                        remaining,
                        response: is_cancel,
                        t,
                    },
                ),
            }
        }
        self.events = events;

        if top != self.last_top[symbol as usize] {
            self.last_top[symbol as usize] = top;
            self.push_md(MdMsg::Top {
                symbol,
                bid: top.0,
                ask: top.1,
                t,
            });
        }
    }

    #[inline]
    fn settle(&mut self, order_id: OrderId, s: Settle) {
        let r = order_user(order_id) as usize % self.risk_shards;
        self.push_risk(r, s);
    }

    /// 風控分片在送不出訂單時仍會消化結算，所以這裡單純等待不會死結。
    fn push_risk(&mut self, r: usize, mut s: Settle) {
        while let Err(back) = self.io.to_risk[r].try_push(s) {
            s = back;
            std::hint::spin_loop();
        }
    }

    fn push_md(&mut self, mut m: MdMsg) {
        while let Err(back) = self.io.to_md.try_push(m) {
            m = back;
            std::hint::spin_loop();
        }
    }
}
