//! 有界、無鎖的單生產者/單消費者（SPSC）環形佇列。
//!
//! 這是 LMAX Disruptor / Aeron IPC / Chronicle Queue 背後同一個核心想法的最小版本：
//! - 容量是 2 的冪，用 `& mask` 取代取餘數。
//! - head（消費者寫）與 tail（生產者寫）各自放在獨立的 cache line，避免 false sharing。
//! - 每一端快取對方的索引，只有在「看起來滿了 / 空了」時才去讀對方的原子變數，
//!   大幅減少 cache line 在核心之間來回搬移（cache coherence traffic）。
//! - 只需要 Acquire/Release，不需要 CAS 或鎖。

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 128 bytes：x86 的相鄰 cache line prefetcher 會成對抓 64-byte line。
#[repr(align(128))]
struct CachePadded<T>(T);

struct Ring<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// 下一個要讀的位置（只由消費者寫入）。
    head: CachePadded<AtomicUsize>,
    /// 下一個要寫的位置（只由生產者寫入）。
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Sync for Ring<T> {}
unsafe impl<T: Send> Send for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for i in head..tail {
            unsafe { (*self.buf[i & self.mask].get()).assume_init_drop() };
        }
    }
}

pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    tail: usize,
    cached_head: usize,
}

pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    head: usize,
    cached_tail: usize,
}

/// 建立容量為 `capacity`（會進位到 2 的冪）的 SPSC 通道。
pub fn channel<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let cap = capacity.max(2).next_power_of_two();
    let buf = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let ring = Arc::new(Ring {
        buf,
        mask: cap - 1,
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
    });
    (
        Producer {
            ring: ring.clone(),
            tail: 0,
            cached_head: 0,
        },
        Consumer {
            ring,
            head: 0,
            cached_tail: 0,
        },
    )
}

impl<T> Producer<T> {
    #[inline]
    pub fn try_push(&mut self, v: T) -> Result<(), T> {
        let cap = self.ring.mask + 1;
        if self.tail - self.cached_head == cap {
            self.cached_head = self.ring.head.0.load(Ordering::Acquire);
            if self.tail - self.cached_head == cap {
                return Err(v);
            }
        }
        unsafe { (*self.ring.buf[self.tail & self.ring.mask].get()).write(v) };
        self.tail += 1;
        self.ring.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// 忙等（busy-spin）直到推入成功。低延遲系統寧可燒一顆核心也不讓執行緒睡著。
    #[inline]
    pub fn push(&mut self, mut v: T) {
        loop {
            match self.try_push(v) {
                Ok(()) => return,
                Err(back) => {
                    v = back;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

impl<T> Consumer<T> {
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            self.cached_tail = self.ring.tail.0.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                return None;
            }
        }
        let v = unsafe { (*self.ring.buf[self.head & self.ring.mask].get()).assume_init_read() };
        self.head += 1;
        self.ring.head.0.store(self.head, Ordering::Release);
        Some(v)
    }

    #[inline]
    pub fn pop(&mut self) -> T {
        loop {
            if let Some(v) = self.try_pop() {
                return v;
            }
            std::hint::spin_loop();
        }
    }
}
