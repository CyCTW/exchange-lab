//! 沒有工作時要怎麼等（對應 Disruptor 的 WaitStrategy / Aeron 的 IdleStrategy）。

use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleKind {
    /// 一直忙等。延遲最低，但每條執行緒都要獨佔一顆核心。
    Spin,
    /// 先忙等、再 yield、最後短暫 sleep。執行緒數多於核心數時必須用這個，
    /// 否則忙等的執行緒會搶走真正有工作的執行緒的 CPU 時間。
    Backoff,
}

pub struct Idle {
    kind: IdleKind,
    n: u32,
}

impl Idle {
    pub fn new(kind: IdleKind) -> Self {
        Idle { kind, n: 0 }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.n = 0;
    }

    #[inline]
    pub fn idle(&mut self) {
        match self.kind {
            IdleKind::Spin => std::hint::spin_loop(),
            IdleKind::Backoff => {
                if self.n < 200 {
                    std::hint::spin_loop();
                } else if self.n < 300 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_micros(50));
                }
                self.n = self.n.saturating_add(1);
            }
        }
    }
}

/// 所有元件共用的時鐘：自模擬開始以來的奈秒數。
#[derive(Clone, Copy)]
pub struct Clock(Instant);

impl Clock {
    pub fn new(epoch: Instant) -> Self {
        Clock(epoch)
    }
    #[inline]
    pub fn now_ns(&self) -> u64 {
        self.0.elapsed().as_nanos() as u64
    }
}
