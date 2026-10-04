//! 把執行緒釘在指定 CPU 核心上（Linux `sched_setaffinity`）。
//!
//! 搭配開機參數 `isolcpus=` / `nohz_full=`，讓熱路徑執行緒獨佔一顆核心，
//! 不被排程器搬移、不被其他行程或中斷打擾（見 docs/04-system-tuning.md）。

#[cfg(target_os = "linux")]
pub fn pin_current_thread(cpu: usize) -> bool {
    extern "C" {
        fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const u64) -> i32;
    }
    let mut set = [0u64; 16]; // 1024 bits，等同 glibc 的 cpu_set_t
    if cpu >= set.len() * 64 {
        return false;
    }
    set[cpu / 64] |= 1 << (cpu % 64);
    unsafe { sched_setaffinity(0, std::mem::size_of_val(&set), set.as_ptr()) == 0 }
}

#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread(_cpu: usize) -> bool {
    false
}
