# exchange-lab

研究「高頻、低延遲、高流量的交易所是如何實作的」：一份研究筆記，加上一個可以實際跑、實際量的 Rust 原型。

## 一句話結論

> 現代低延遲交易所都收斂到同一個模式：**二進位協定 → 定序 → journal/複製 → 單執行緒、全記憶體、確定性的撮合引擎（依商品分片）→ 事件流 → UDP multicast 行情**。
> 撮合本身只要數十奈秒；真正的工程挑戰是把網路、序列化、持久化、作業系統抖動壓到最小，並讓尾延遲穩定。

## 研究筆記

| 章節 | 內容 |
|---|---|
| [01 · 全貌](docs/01-overview.md) | 系統元件、設計原則、延遲預算、一筆委託的生命週期 |
| [02 · 案例研究](docs/02-case-studies.md) | LMAX、Nasdaq INET、CME Globex、SBE、Aeron、LSE、Eurex、IEX、加密交易所 |
| [03 · 撮合引擎與資料結構](docs/03-matching-engine.md) | 價格-時間優先、價格層資料結構比較、slab、為什麼單執行緒 |
| [04 · 硬體、作業系統與網路](docs/04-system-tuning.md) | 綁核/隔離、記憶體、kernel bypass、FPGA、時間同步 |
| [05 · 可靠性](docs/05-reliability.md) | 確定性狀態機複製、sequencer、journal、快照、failover、行情補洞 |
| [06 · 量測與實驗結果](docs/06-measurement.md) | coordinated omission、本原型的效能數字與解讀、下一步實驗 |
| [07 · 多用戶多商品模擬與架構](docs/07-simulation-architecture.md) | 依用戶分片的風控 × 依商品分片的撮合、資金凍結/結算、循環拓樸死結、熱門商品、實驗結果 |
| [參考資料](docs/references.md) | 文章、演講、協定規格 |

## 原型

```text
src/
  types.rs       指令與事件（整數 tick 價格）
  orderbook.rs   單執行緒限價簿：tick 陣列 + 非空層 bitmap + 侵入式 FIFO + slab，熱路徑零配置
  ring.rs        無鎖 SPSC 環形佇列（cache line padding、快取對方索引）
  journal.rs     固定長度二進位輸入 journal，可重播
  histogram.rs   簡化版 HdrHistogram
  affinity.rs    執行緒綁核
  workload.rs    可重現的合成委託流
  bin/bench.rs   單執行緒與三執行緒管線的效能實驗（含 coordinated omission 修正）
  sim/           多用戶、多商品交易所模擬（見 docs/07）
    model.rs       商品、用戶類型、分片規則、價格路徑
    gateway.rs     模擬用戶行為 + 指派訂單編號、限流、收回報
    risk.rs        依用戶分片：餘額凍結 / 結算 / 解凍
    matcher.rs     依商品分片：每個商品一本 OrderBook
    mdata.rs       行情彙整
    run.rs         組裝拓樸、跨執行緒一致性檢查、報表
  bin/sim.rs     模擬器 CLI
tests/
  matching.rs    撮合規則、與樸素實作對照、journal 重播一致性、ring 正確性
  sim.rs         多種拓樸（含極小佇列）下的端到端一致性
```

```bash
cargo test --release
cargo run --release --bin bench -- --mode single
cargo run --release --bin bench -- --mode pipeline --rate 1000000 --pin [--journal /tmp/x.journal]
cargo run --release --bin sim -- --gateways 2 --risk 2 --matchers 2 --users 10000 --symbols 8 --rate 200000 --secs 5
```

無外部相依套件。

### 初步結果（4 vCPU 雲端 VM，未調校）

- 單執行緒撮合：**約 1,100 萬筆指令/秒**，每筆 p50 167 ns、p99 863 ns
- 三執行緒管線 @ 1M msg/s：端到端 p50 約 1.1 µs；p99 達毫秒級，來自 VM/OS 抖動——正好說明 [04](docs/04-system-tuning.md) 的調校為何必要

- 多用戶多商品模擬（1 gateway / 1 風控 / 1 撮合，每條執行緒獨佔核心）：請求→回應的 4 跳往返 p50 約 2 µs；執行緒數超過核心數時 p50 惡化到數百 µs

詳細數字與分析見 [06](docs/06-measurement.md)、[07](docs/07-simulation-architecture.md)。
