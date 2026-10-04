# 06 · 如何量測，以及本 lab 的實驗結果

## 量測原則

1. **看分佈，不看平均**：報 p50 / p90 / p99 / p99.9 / p99.99 / max。交易所的競爭力在尾端——客戶在乎的是「最慢的那幾筆」。
2. **用直方圖**：HdrHistogram（本 lab 的 `src/histogram.rs` 是簡化版）固定記憶體、O(1) 紀錄，可以在熱路徑上用。
3. **避免 coordinated omission**（Gil Tene, *How NOT to Measure Latency*）：
   如果壓測端「等上一筆回來才送下一筆」，系統卡住 10ms 時壓測端也停了，那 10ms 內 **本來應該送出** 的數千筆請求的延遲全部沒被記到，尾延遲被嚴重低估。
   修正方法：**以固定速率排程，從「預定送出時間」開始計時**（本 lab `--rate` 就是這樣做）。
4. **區分吞吐量與延遲**：不限速灌滿系統量到的是最大吞吐，此時延遲由排隊決定（見下方 Little's law），不代表正常負載下的延遲。
5. **量測本身有成本**：`Instant::now()`（`clock_gettime` vDSO）約 15–25 ns；奈秒級的量測要扣掉這個。生產環境常用 TSC 或網卡硬體時間戳。
6. **在接近生產的環境量**：隔離核心、綁核、關閉省電；雲端 VM 的數字只能看相對趨勢。

## 實驗設定

```bash
cargo test --release                                   # 正確性（含與樸素實作的對照、journal 重播）
cargo run --release --bin bench -- --mode single       # 單執行緒撮合
cargo run --release --bin bench -- --mode pipeline     # 三執行緒管線，不限速
cargo run --release --bin bench -- --mode pipeline --rate 1000000 --pin
cargo run --release --bin bench -- --mode pipeline --rate 1000000 --pin --journal /tmp/x.journal
```

工作負載（`src/workload.rs`）：約 50% 新掛單、40% 撤單（部分撤的是已成交的單，會被拒絕——真實市場也常見）、10% IOC 吃單；簿上約維持數萬到十萬張掛單，價格在中間價附近隨機漂移。

管線模式：

```text
gateway 執行緒 ──SPSC ring(64K)──▶ engine 執行緒 [journal] ──SPSC ring(64K)──▶ publisher 執行緒
 （依速率送單，記下預定時間）       （撮合，計算事件數）                       （記錄 now - 預定時間）
```

## 結果（2026-10，4 vCPU 雲端 VM，Intel Xeon @ 2.8GHz，未做 isolcpus/BIOS 調校）

### 單執行緒撮合

| 指標 | 數值 |
|---|---|
| 吞吐量 | **約 1,100 萬筆指令/秒**（500 萬筆、713 萬事件、195 萬筆成交，0.45 秒） |
| 每筆延遲 p50 / p90 / p99 | 167 ns / 399 ns / 863 ns |
| p99.9 / p99.99 / max | 2.9 µs / 37 µs / 2.1 ms |

- 平均每筆約 90 ns（1 / 11M），與 p50 167 ns 的差距主要是計時開銷。
- p99.99 與 max 遠大於 p99：這些不是撮合本身慢，而是 **作業系統/虛擬化打斷**（中斷、排程、hypervisor 搶佔）。這正是 [04](04-system-tuning.md) 那些調校要消除的。

### 管線（3 執行緒）

| 情境 | 吞吐量 | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| 不限速 | 3.8 M/s | 16 ms | 22 ms | 42 ms | 50 ms |
| 100k/s，綁核 | 0.1 M/s | 1.3 µs | 12 µs | 2.1 ms | 21 ms |
| 500k/s，綁核 | 0.5 M/s | 1.2 µs | 16 µs | 3.0 ms | 8 ms |
| 1M/s，綁核 | 1.0 M/s | 1.1 µs | 15 µs | 2.5 ms | 3.9 ms |
| 1M/s，綁核 + journal（page cache） | 1.0 M/s | 1.3 µs | 90 µs | 3.4 ms | 6.9 ms |

### 解讀

1. **管線吞吐（3.8M/s）低於單執行緒（11M/s）**：每則訊息都要跨兩次核心，cache line 在核心間搬移的成本（約數十到上百 ns）超過撮合本身。
   改善方向：消費端 **批次處理**（一次讀走 ring 中所有可讀訊息、只更新一次 head）——這正是 Disruptor 在高負載下吞吐反而提升的「batching effect」。
2. **不限速時延遲 ~16 ms 完全是排隊**：Little's law，延遲 ≈ 佇列長度 / 吞吐 = 65,536 / 3.8M ≈ 17 ms，和量到的 p50 吻合。
   這說明：**壓垮系統時量到的延遲，量的是你的佇列有多長，不是系統有多快**。真實交易所會在 gateway 做限流與背壓，而不是讓佇列無限累積。
3. **正常負載下 p50 約 1.1–1.3 µs**（三執行緒、兩次跨核、含計時開銷），符合「跨核傳遞 + 撮合」的預期數量級。
4. **p99 到毫秒級是環境造成的**：這台 VM 只有 4 個 vCPU，三條 busy-spin 執行緒加上 OS 本身已經把核心用滿，而且沒有 isolcpus、會被 hypervisor 搶佔。
   在調校過的實體機上，同樣的程式 p99 通常可以壓到個位數微秒。這也是為什麼交易所不會（或很晚才會）把撮合放在一般的共享雲端主機上。
5. **Journal 讓 p90 從 15 µs 升到 90 µs**：即使只是寫進 page cache，`write` syscall 與偶發的 page cache 回寫也會干擾熱路徑。
   改善方向：撮合執行緒不直接寫檔，改由 **獨立的 journal 執行緒** 從輸入 ring 平行消費（LMAX 做法），或把 journal 換成網路複製。

## 下一步實驗建議

- [ ] 消費端批次處理（batch drain），比較管線吞吐與延遲
- [ ] journal 改成獨立消費者執行緒，撮合只在 journal 序號之後處理（Disruptor 依賴圖）
- [ ] 交易所自派委託編號，以 slot 索引取代雜湊表
- [ ] 加入行情發布：把事件編碼成 ITCH 風格訊息，經 UDP multicast 發送，並實作序號補洞
- [ ] 依商品分片，多個撮合執行緒，觀察擴充性
- [ ] 實作 primary/standby：standby 經 TCP/UDP 吃同一份 journal，驗證 failover 後狀態一致
- [ ] 在實體機上套用 [04](04-system-tuning.md) 的調校清單，量化每一項對 p99.9 的影響
- [ ] `perf stat -e cache-misses,branch-misses` 分析撮合熱路徑
