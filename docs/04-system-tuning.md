# 04 · 硬體、作業系統與網路

撮合本身只要數十奈秒，但一個沒調校的 Linux 主機隨時可能讓你的執行緒停頓數百微秒到數毫秒。
低延遲系統的大量工作是在 **消除抖動（jitter）**：讓 p99.9、max 接近 p50。

本 lab 在一般雲端 VM 上的量測結果就是活生生的例子：p50 約 1 µs，p99 卻到毫秒級（見 [06](06-measurement.md)）。

## CPU：獨佔、綁核、不睡覺

| 手段 | 作法 | 消除的問題 |
|---|---|---|
| 隔離核心 | 開機參數 `isolcpus=2-7`（或 cgroup cpuset） | 排程器不再把其他行程放到這些核心 |
| 關閉 tick | `nohz_full=2-7` | 單一可執行行程時不再每毫秒被 timer 中斷 |
| 移走 RCU 回呼 | `rcu_nocbs=2-7` | 核心內部的 RCU 工作不在隔離核心上跑 |
| 中斷綁核 | 設定 `/proc/irq/*/smp_affinity`，停用 `irqbalance` | 硬體中斷不打擾熱路徑核心 |
| 綁定執行緒 | `sched_setaffinity` / `taskset`（本 lab：`src/affinity.rs`、`--pin`） | 執行緒不被搬移，cache 一直是熱的 |
| 忙等 | 執行緒 busy-spin（`spin_loop` / x86 `pause`），不用 futex/condvar | 喚醒延遲（數 µs）與 cache 被洗掉 |
| 關閉深層 C-state | BIOS 設定或 `intel_idle.max_cstate=0 processor.max_cstate=1` | 核心從深度睡眠醒來要數十 µs |
| 固定頻率 | `performance` governor；評估是否關閉 turbo | 頻率切換造成的停頓與不可預測性 |
| 處理超執行緒 | 關閉 SMT，或確保熱路徑核心的 sibling 閒置 | 兩個硬體執行緒共用 L1/L2 與執行單元 |

## 記憶體

- **預先配置 + 預先觸碰（prefault）**：啟動時把所有 buffer 寫過一遍，避免執行中的 page fault。
- **`mlockall(MCL_CURRENT | MCL_FUTURE)`**、關閉 swap：記憶體永不被換出。
- **Huge pages**（2MB/1GB，經 hugetlbfs）：減少 TLB miss。但 **關閉 Transparent Huge Pages** 的自動合併（`khugepaged` 可能造成停頓）。
- **NUMA**：熱路徑執行緒、它的記憶體、以及網卡要在 **同一個 NUMA node**（`numactl --cpunodebind --membind`）。跨 socket 存取延遲明顯較高。
- **避免 false sharing**：不同執行緒寫的變數放在不同 cache line（本 lab ring 的 head/tail 各自 128-byte 對齊）。

## 語言執行環境

- **C++ / Rust**：沒有 GC，但仍要避免熱路徑配置；注意 `std::map`、`std::string`、`std::function` 的隱藏配置。
- **Java**（LMAX、許多交易系統）：穩態零配置、物件池、off-heap 記憶體（Agrona、Chronicle）、用 `-XX:+AlwaysPreTouch` 預先觸碰 heap、JIT 暖機；或使用低停頓 GC（如 Azul 的 C4、ZGC）。
- 通用：**熱路徑上不做 log 格式化**——只把二進位紀錄丟進 ring，讓另一條執行緒去格式化寫檔。

## 網路：繞過 kernel

一般 socket 收一個封包要經過：網卡中斷 → kernel 網路堆疊 → socket buffer → syscall 複製到 user space。每一步都有延遲和抖動。

| 技術 | 說明 |
|---|---|
| **OpenOnload**（AMD/Solarflare） | user space TCP/UDP 堆疊，透過 `LD_PRELOAD` 取代 socket API，程式幾乎不用改 |
| **ef_vi / TCPDirect** | 更底層的 API，直接收送乙太網路 frame，延遲更低但要自己處理協定 |
| **NVIDIA VMA / XLIO** | Mellanox/NVIDIA 網卡的類似方案 |
| **DPDK** | 通用的 user space 封包處理框架，poll-mode driver |
| **RDMA / RoCE** | 機房內複製資料到另一台主機的記憶體，常用於 journal 複製 |
| `SO_BUSY_POLL` | 不換掉 kernel 堆疊時的折衷：讓 socket 忙等網卡佇列 |

### 硬體

- **低延遲網卡**（Solarflare/AMD X2/X3 系列、NVIDIA ConnectX、Cisco/前 Exablaze）與 **硬體時間戳**。
- **交換器**：cut-through 交換器（數百 ns）；**L1 交換器**（Arista 7130 / 前 Metamako，約 5 ns）用於行情扇出與量測。
- **FPGA**：交易端最常用於 tick-to-trade；交易所端則可用在行情封包編碼、gateway 協定處理、pre-trade 風控等「規則固定、需要確定性延遲」的環節。
- **主機共置**：會員把伺服器放在交易所機房，**等長線纜** 確保公平。

## 時間同步

- 每台機器用 **PTP（IEEE 1588）** 配合 GPS 授時與支援硬體時間戳的網卡同步，達到次微秒精度。
- 監管要求：歐盟 MiFID II RTS 25 要求高頻交易者與低延遲交易所的時鐘與 UTC 誤差在 **100 µs** 以內、時間戳精度到 **1 µs**。
- 熱路徑內部計時可以用 TSC（`rdtsc`，需確認 invariant TSC），比 `clock_gettime` 更便宜。
- **撮合引擎本身不讀時鐘**：時間戳由 sequencer 蓋在輸入訊息上，確保重播結果一致（見 [05](05-reliability.md)）。

## 檢查清單

```text
[ ] BIOS：關閉 C-states、省電、（評估）SMT/turbo；開啟效能模式
[ ] 開機參數：isolcpus / nohz_full / rcu_nocbs / 關閉 THP 自動合併
[ ] 停用 irqbalance，中斷綁到非熱路徑核心；網卡中斷綁到同 NUMA node
[ ] 熱路徑執行緒綁核、busy-spin；其他執行緒放在 housekeeping 核心
[ ] mlockall、hugepages、prefault；熱路徑零配置
[ ] kernel bypass 網路；硬體時間戳；PTP 同步
[ ] 持續量測 p99.9 / max，並用 perf / ftrace 追每一個 outlier
```
