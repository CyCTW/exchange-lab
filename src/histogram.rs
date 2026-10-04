//! 簡化版 HdrHistogram：對數-線性分桶，固定記憶體、O(1) 紀錄，誤差約 6%。
//!
//! 延遲一定要看分佈（p99 / p99.9 / max），不能只看平均值。

pub struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
    sum: u128,
}

const SUB: u32 = 16;

#[inline]
fn bucket(v: u64) -> usize {
    if v < 2 * SUB as u64 {
        return v as usize;
    }
    let msb = 63 - v.leading_zeros();
    let shift = msb - 4;
    let top = (v >> shift) as u32; // [16, 31]
    (2 * SUB + (shift - 1) * SUB + (top - SUB)) as usize
}

/// 桶的上界（回報百分位時偏保守）。
fn bucket_upper(i: usize) -> u64 {
    let i = i as u32;
    if i < 2 * SUB {
        return i as u64;
    }
    let shift = (i - 2 * SUB) / SUB + 1;
    let top = (i - 2 * SUB) % SUB + SUB;
    let upper = ((top as u128 + 1) << shift) - 1;
    upper.min(u64::MAX as u128) as u64
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            counts: vec![0; 1024],
            total: 0,
            max: 0,
            sum: 0,
        }
    }
}

impl Histogram {
    #[inline]
    pub fn record(&mut self, v: u64) {
        self.counts[bucket(v)] += 1;
        self.total += 1;
        self.max = self.max.max(v);
        self.sum += v as u128;
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
        self.sum += other.sum;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn mean(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.sum as f64 / self.total as f64
        }
    }

    pub fn percentile(&self, p: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = ((p / 100.0) * self.total as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= target {
                return bucket_upper(i).min(self.max);
            }
        }
        self.max
    }

    pub fn summary(&self, unit: &str) -> String {
        format!(
            "n={} mean={:.0}{u} p50={}{u} p90={}{u} p99={}{u} p99.9={}{u} p99.99={}{u} max={}{u}",
            self.total,
            self.mean(),
            self.percentile(50.0),
            self.percentile(90.0),
            self.percentile(99.0),
            self.percentile(99.9),
            self.percentile(99.99),
            self.max,
            u = unit
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_bounded() {
        let mut prev = 0;
        for v in (0..1_000_000u64).chain([u64::MAX / 2, u64::MAX]) {
            let b = bucket(v);
            assert!(b >= prev && b < 1024, "v={v} b={b}");
            assert!(bucket_upper(b) >= v);
            prev = b;
        }
    }

    #[test]
    fn percentiles() {
        let mut h = Histogram::default();
        for v in 1..=1000 {
            h.record(v);
        }
        let p50 = h.percentile(50.0);
        assert!((500..=540).contains(&p50), "{p50}");
        assert_eq!(h.percentile(100.0), 1000);
    }
}
