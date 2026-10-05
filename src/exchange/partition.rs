//! 撮合分區（partition）：擁有一組商品的限價簿，依序處理定序器送來的 [`SeqMsg`]。
//!
//! `PartitionCore` 是純粹的確定性狀態機：不讀時鐘、不做 I/O，輸出全部交給 [`Sink`]。
//! 同一份輸入序列 ⇒ 完全相同的輸出與狀態。所以同一份程式碼既跑在即時執行緒上，
//! 也能離線重播 journal 來驗證（見 `run::replay`）。

use std::collections::HashMap;

use super::model::*;
use super::msg::*;
use crate::idle::Idle;
use crate::orderbook::{BookConfig, OrderBook};
use crate::ring::{Consumer, Producer};
use crate::types::*;

pub trait Sink {
    fn report(&mut self, gateway: usize, m: ToGateway);
    fn md(&mut self, m: MdMsg);
}

#[derive(Default, Debug, Clone)]
pub struct PartitionStats {
    pub id: usize,
    pub instruments: Vec<InstrumentId>,
    pub msgs: u64,
    pub trades: u64,
    pub crosses: u64,
    pub rejects: HashMap<String, u64>,
    pub mass_cancelled: u64,
    /// (商品, 階段, 競價價格, 成交量)
    pub auctions: Vec<(InstrumentId, Phase, Price, Qty)>,
}

pub struct PartitionCore {
    gateways: usize,
    books: Vec<Option<OrderBook>>,
    /// 最近成交價（集合競價的參考價）。
    last: Vec<Price>,
    halted: Vec<bool>,
    phase: Phase,
    events: Vec<Event>,
    chan_seq: u64,
    /// 所有輸出（回報 + 行情）的滾動雜湊，用來比對即時執行與重播。
    hash: u64,
    pub stats: PartitionStats,
}

impl PartitionCore {
    pub fn new(id: usize, cfg: &ExchangeConfig, insts: &[Instrument], placement: &[usize]) -> Self {
        let mut owned = vec![];
        let books = insts
            .iter()
            .map(|s| {
                (placement[s.id as usize] == id).then(|| {
                    owned.push(s.id);
                    let mut b = OrderBook::new(BookConfig {
                        min_price: s.lower_limit,
                        max_price: s.upper_limit,
                        max_orders: cfg.max_orders_per_book,
                    });
                    b.set_auction(true); // 開盤前是集合競價累積期
                    b
                })
            })
            .collect();
        PartitionCore {
            gateways: cfg.gateways,
            books,
            last: insts.iter().map(|s| s.reference).collect(),
            halted: vec![false; insts.len()],
            phase: Phase::PreOpen,
            events: Vec::with_capacity(256),
            chan_seq: 0,
            hash: 0xcbf2_9ce4_8422_2325,
            stats: PartitionStats {
                id,
                instruments: owned,
                ..Default::default()
            },
        }
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn resting(&self) -> usize {
        self.books.iter().flatten().map(|b| b.order_count()).sum()
    }

    pub fn snapshot(&self, inst: InstrumentId) -> Option<Vec<(Side, Price, OrderId, Qty)>> {
        self.books[inst as usize].as_ref().map(|b| b.snapshot())
    }

    #[inline]
    fn mix(&mut self, x: u64) {
        self.hash = (self.hash ^ x).wrapping_mul(0x0000_0100_0000_01B3);
    }

    fn report(
        &mut self,
        sink: &mut impl Sink,
        order_id: OrderId,
        kind: ExecKind,
        response: bool,
        t: u64,
    ) {
        self.mix(order_id);
        match kind {
            ExecKind::Ack => self.mix(1),
            ExecKind::Rejected(r) => self.mix(2 << 8 | r as u64),
            ExecKind::Fill {
                side,
                price,
                qty,
                leaves,
            } => {
                self.mix(3 << 8 | side as u64);
                self.mix(price as u64);
                self.mix(qty);
                self.mix(leaves);
            }
            ExecKind::Done { remaining } => {
                self.mix(4);
                self.mix(remaining);
            }
            ExecKind::CancelRejected => self.mix(5),
        }
        self.mix(response as u64);
        self.mix(t);
        let g = member_of(order_id) as usize % self.gateways;
        sink.report(
            g,
            ToGateway::Report {
                order_id,
                kind,
                response,
                t,
            },
        );
    }

    fn md(&mut self, sink: &mut impl Sink, body: MdBody) {
        self.chan_seq += 1;
        self.mix(self.chan_seq);
        let words: [u64; 5] = match body {
            MdBody::Add {
                inst,
                id,
                side,
                price,
                qty,
            } => [1 << 32 | inst as u64, id, side as u64, price as u64, qty],
            MdBody::Executed { inst, id, qty } => [2 << 32 | inst as u64, id, qty, 0, 0],
            MdBody::Deleted { inst, id } => [3 << 32 | inst as u64, id, 0, 0, 0],
            MdBody::Trade {
                inst,
                price,
                qty,
                auction,
                t,
            } => [4 << 32 | inst as u64, price as u64, qty, auction as u64, t],
            MdBody::AuctionResult {
                inst,
                price,
                qty,
                phase,
            } => [5 << 32 | inst as u64, price as u64, qty, phase as u64, 0],
            MdBody::Phase(p) => [6 << 32 | p as u64, 0, 0, 0, 0],
            MdBody::Halt(i) => [7 << 32 | i as u64, 0, 0, 0, 0],
            MdBody::Resume(i) => [8 << 32 | i as u64, 0, 0, 0, 0],
        };
        for w in words {
            self.mix(w);
        }
        sink.md(MdMsg::Event {
            chan_seq: self.chan_seq,
            body,
        });
    }

    fn reject(&mut self, sink: &mut impl Sink, order_id: OrderId, reason: RejectReason, t: u64) {
        *self.stats.rejects.entry(format!("{reason:?}")).or_default() += 1;
        self.report(sink, order_id, ExecKind::Rejected(reason), true, t);
    }

    pub fn process(&mut self, m: &SeqMsg, sink: &mut impl Sink) {
        self.stats.msgs += 1;
        match m.body {
            SeqBody::New {
                order_id,
                inst,
                side,
                price,
                qty,
                tif,
                t,
            } => {
                if self.phase == Phase::Closed {
                    return self.reject(sink, order_id, RejectReason::InvalidPhase, t);
                }
                self.new_order(sink, inst, order_id, side, price, qty, tif, t);
            }
            SeqBody::Cancel { order_id, inst, t } => self.cancel(sink, inst, order_id, true, t),
            SeqBody::Replace {
                old_id,
                new_id,
                inst,
                side,
                price,
                qty,
                t,
            } => {
                let exists = self.book(inst).contains(old_id);
                if self.phase == Phase::Closed {
                    return self.reject(sink, new_id, RejectReason::InvalidPhase, t);
                }
                if !exists {
                    return self.reject(sink, new_id, RejectReason::UnknownOrder, t);
                }
                self.cancel(sink, inst, old_id, false, t);
                self.new_order(sink, inst, new_id, side, price, qty, TimeInForce::Gtc, t);
            }
            SeqBody::MassCancel { member, t } => {
                for inst in self.stats.instruments.clone() {
                    let mut events = std::mem::take(&mut self.events);
                    events.clear();
                    let book = self.books[inst as usize].as_mut().unwrap();
                    book.cancel_matching(|id| member_of(id) == member, &mut |e| events.push(e));
                    for e in &events {
                        if let Event::Cancelled { id, remaining } = *e {
                            self.stats.mass_cancelled += 1;
                            self.report(sink, id, ExecKind::Done { remaining }, false, t);
                            self.md(sink, MdBody::Deleted { inst, id });
                        }
                    }
                    self.events = events;
                }
            }
            SeqBody::Phase(p) => self.set_phase(sink, p),
            SeqBody::Halt(inst) => {
                if self.books[inst as usize].is_some() {
                    self.halted[inst as usize] = true;
                    self.book(inst).set_auction(true);
                    self.md(sink, MdBody::Halt(inst));
                }
            }
            SeqBody::Resume(inst) => {
                if self.books[inst as usize].is_some() {
                    self.halted[inst as usize] = false;
                    if self.phase == Phase::Continuous {
                        self.auction(sink, inst, Phase::Continuous);
                        self.book(inst).set_auction(false);
                    }
                    self.md(sink, MdBody::Resume(inst));
                }
            }
        }
    }

    fn book(&mut self, inst: InstrumentId) -> &mut OrderBook {
        self.books[inst as usize]
            .as_mut()
            .expect("message routed to the wrong partition")
    }

    #[allow(clippy::too_many_arguments)]
    fn new_order(
        &mut self,
        sink: &mut impl Sink,
        inst: InstrumentId,
        order_id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        t: u64,
    ) {
        let mut events = std::mem::take(&mut self.events);
        events.clear();
        let book = self.book(inst);
        book.execute(
            &Command::NewOrder {
                id: order_id,
                side,
                price,
                qty,
                tif,
            },
            &mut |e| events.push(e),
        );
        let mut leaves = qty;
        for e in &events {
            match *e {
                Event::Accepted { id } => self.report(sink, id, ExecKind::Ack, true, t),
                Event::Rejected { id, reason } => self.reject(sink, id, reason, t),
                Event::Trade {
                    taker,
                    maker,
                    taker_side,
                    price,
                    qty,
                } => {
                    self.stats.trades += 1;
                    leaves -= qty;
                    self.last[inst as usize] = price;
                    // 同一張 maker 在一次撮合中最多成交一次，撮合後查到的剩餘量就是它的 leaves。
                    let maker_leaves = self.book(inst).order_qty(maker).unwrap_or(0);
                    self.report(
                        sink,
                        taker,
                        ExecKind::Fill {
                            side: taker_side,
                            price,
                            qty,
                            leaves,
                        },
                        false,
                        t,
                    );
                    self.report(
                        sink,
                        maker,
                        ExecKind::Fill {
                            side: taker_side.opposite(),
                            price,
                            qty,
                            leaves: maker_leaves,
                        },
                        false,
                        t,
                    );
                    self.md(
                        sink,
                        MdBody::Trade {
                            inst,
                            price,
                            qty,
                            auction: false,
                            t,
                        },
                    );
                    self.md(
                        sink,
                        MdBody::Executed {
                            inst,
                            id: maker,
                            qty,
                        },
                    );
                }
                Event::Cancelled { id, remaining } => {
                    self.report(sink, id, ExecKind::Done { remaining }, false, t)
                }
                Event::Cross { .. } => unreachable!("crosses only happen in uncross"),
            }
        }
        self.events = events;
        if let Some(q) = self.book(inst).order_qty(order_id) {
            self.md(
                sink,
                MdBody::Add {
                    inst,
                    id: order_id,
                    side,
                    price,
                    qty: q,
                },
            );
        }
    }

    fn cancel(
        &mut self,
        sink: &mut impl Sink,
        inst: InstrumentId,
        order_id: OrderId,
        response: bool,
        t: u64,
    ) {
        let mut events = std::mem::take(&mut self.events);
        events.clear();
        self.book(inst)
            .execute(&Command::Cancel { id: order_id }, &mut |e| events.push(e));
        for e in &events {
            match *e {
                Event::Cancelled { id, remaining } => {
                    self.report(sink, id, ExecKind::Done { remaining }, response, t);
                    self.md(sink, MdBody::Deleted { inst, id });
                }
                Event::Rejected { id, .. } => {
                    self.report(sink, id, ExecKind::CancelRejected, response, t)
                }
                _ => unreachable!(),
            }
        }
        self.events = events;
    }

    fn set_phase(&mut self, sink: &mut impl Sink, p: Phase) {
        let prev = self.phase;
        self.phase = p;
        for inst in self.stats.instruments.clone() {
            match p {
                Phase::PreOpen | Phase::PreClose => self.book(inst).set_auction(true),
                Phase::Continuous => {
                    // 開盤集合競價。暫停中的商品維持累積，等恢復時再撮合。
                    if !self.halted[inst as usize] {
                        self.auction(sink, inst, Phase::PreOpen);
                        self.book(inst).set_auction(false);
                    }
                }
                Phase::Closed => {
                    if prev.is_call() || self.book(inst).in_auction() {
                        self.auction(sink, inst, Phase::Closed);
                    }
                    self.book(inst).set_auction(true); // 收盤後不再撮合
                }
            }
        }
        self.md(sink, MdBody::Phase(p));
    }

    /// 集合競價撮合，並把成交拆成雙方的回報與行情。
    fn auction(&mut self, sink: &mut impl Sink, inst: InstrumentId, kind: Phase) {
        let mut events = std::mem::take(&mut self.events);
        events.clear();
        let reference = self.last[inst as usize];
        let result = self.book(inst).uncross(reference, &mut |e| events.push(e));
        let Some((price, volume)) = result else {
            self.events = events;
            return;
        };
        self.last[inst as usize] = price;
        self.stats.auctions.push((inst, kind, price, volume));

        // 同一張單可能和多張對手單成交：由後往前累加，算出每筆成交當下的 leaves。
        let mut later: HashMap<OrderId, Qty> = HashMap::new();
        let mut leaves = vec![(0, 0); events.len()];
        for (k, e) in events.iter().enumerate().rev() {
            if let Event::Cross { buy, sell, qty, .. } = *e {
                let b = self.book(inst).order_qty(buy).unwrap_or(0)
                    + later.get(&buy).copied().unwrap_or(0);
                let s = self.book(inst).order_qty(sell).unwrap_or(0)
                    + later.get(&sell).copied().unwrap_or(0);
                leaves[k] = (b, s);
                *later.entry(buy).or_default() += qty;
                *later.entry(sell).or_default() += qty;
            }
        }
        for (k, e) in events.iter().enumerate() {
            let Event::Cross {
                buy,
                sell,
                price,
                qty,
            } = *e
            else {
                continue;
            };
            self.stats.crosses += 1;
            let (bl, sl) = leaves[k];
            self.report(
                sink,
                buy,
                ExecKind::Fill {
                    side: Side::Buy,
                    price,
                    qty,
                    leaves: bl,
                },
                false,
                0,
            );
            self.report(
                sink,
                sell,
                ExecKind::Fill {
                    side: Side::Sell,
                    price,
                    qty,
                    leaves: sl,
                },
                false,
                0,
            );
            self.md(
                sink,
                MdBody::Trade {
                    inst,
                    price,
                    qty,
                    auction: true,
                    t: 0,
                },
            );
            self.md(sink, MdBody::Executed { inst, id: buy, qty });
            self.md(
                sink,
                MdBody::Executed {
                    inst,
                    id: sell,
                    qty,
                },
            );
        }
        self.events = events;
        self.md(
            sink,
            MdBody::AuctionResult {
                inst,
                price,
                qty: volume,
                phase: kind,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// 即時執行緒

pub struct PartitionIo {
    pub from_seq: Consumer<ToPartition>,
    pub to_gw: Vec<Producer<ToGateway>>,
    pub to_md: Producer<MdMsg>,
}

struct LiveSink<'a> {
    io: &'a mut PartitionIo,
}

impl Sink for LiveSink<'_> {
    /// Gateway 送不出請求時仍會消化回報，所以這裡單純等待不會死結。
    #[inline]
    fn report(&mut self, g: usize, mut m: ToGateway) {
        while let Err(back) = self.io.to_gw[g].try_push(m) {
            m = back;
            std::hint::spin_loop();
        }
    }
    #[inline]
    fn md(&mut self, mut m: MdMsg) {
        while let Err(back) = self.io.to_md.try_push(m) {
            m = back;
            std::hint::spin_loop();
        }
    }
}

pub fn run(mut core: PartitionCore, mut io: PartitionIo, mut idle: Idle) -> PartitionCore {
    loop {
        match io.from_seq.try_pop() {
            Some(ToPartition::Msg(m)) => {
                idle.reset();
                core.process(&m, &mut LiveSink { io: &mut io });
            }
            Some(ToPartition::Shutdown) => break,
            None => idle.idle(),
        }
    }
    let mut sink = LiveSink { io: &mut io };
    for g in 0..core.gateways {
        sink.report(g, ToGateway::Shutdown);
    }
    sink.md(MdMsg::Shutdown);
    core
}
