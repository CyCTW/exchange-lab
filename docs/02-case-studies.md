# 02 · 案例研究

> 以下整理自公開資料（技術文章、演講、協定規格）。數字與細節會隨版本演進，引用前請以 [references](references.md) 中的原始出處為準。
> 標示「待查證」的項目是我記得有公開來源、但本文未逐字核對的說法。

## LMAX Exchange —— Disruptor 與「單執行緒業務邏輯」

LMAX 是英國的外匯/差價合約交易所，最有名的是 2011 年 Martin Fowler 撰文介紹的架構，以及開源的 **Disruptor**。

- **Business Logic Processor (BLP)**：所有撮合/業務邏輯在 **一條 Java 執行緒** 上、完全在記憶體中執行，公開數字為每秒 600 萬筆委託。
- **Input Disruptor**：網路收到的訊息放進環形緩衝區，多個消費者 **平行** 處理同一批訊息：
  - journaler：把輸入寫到磁碟
  - replicator：把輸入送到備援節點
  - unmarshaller：反序列化
  
  BLP 只處理「已經 journal 且已經複製」的訊息（消費者之間用序號表達依賴關係，而不是用鎖）。
- **Output Disruptor**：BLP 產生的事件交給 marshaller/網路發送。
- **事件溯源**：狀態不寫資料庫；當機時從最近的快照 + 重播 journal 恢復。
- **Mechanical sympathy**：了解硬體（cache line、false sharing、記憶體屏障）來寫軟體——例如 ring buffer 序號做 cache line padding、預先配置 ring 中所有 entry 以避免 GC。
- 啟示：**Java 也可以做低延遲**，前提是熱路徑零配置、避免 GC、了解 JIT 與記憶體模型。

本 lab 的 [`src/ring.rs`](../src/ring.rs) 就是 Disruptor 核心想法的最小 SPSC 版本。

## Nasdaq INET —— 協定家族的標竿

Nasdaq 的撮合平台源自 Island ECN / INET，後來成為 Nasdaq 自家及授權給全球多家交易所的技術基礎。它定義了一整套被廣泛模仿的協定：

| 協定 | 用途 | 特點 |
|---|---|---|
| **OUCH** | 下單（order entry） | 極簡二進位、固定欄位；只做下單/改單/刪單 |
| **ITCH**（TotalView-ITCH） | 行情 | L3 逐筆委託行情：每筆新增/成交/刪除都廣播，讓客戶端自行重建完整限價簿 |
| **SoupBinTCP** | OUCH 的 session 層 | 在 TCP 上加入登入、心跳、序號，可依序號重連續傳 |
| **MoldUDP64** | ITCH 的傳輸層 | UDP multicast 封包帶 session + 序號；漏封包時向 retransmission server 請求補發 |

啟示：
- **行情只發增量事件，不發整本簿**；客戶端自己維護狀態。這讓交易所的行情發布非常輕量。
- 把 **session 層（可靠性、序號）與應用層（委託語義）分離**，各自可以獨立最佳化。

## CME Globex —— SBE、A/B feed、多種撮合演算法

- **iLink 3**：下單協定，使用 **SBE（Simple Binary Encoding）** 在 TCP 上傳輸（取代較早的 FIX 文字格式）。
- **MDP 3.0**：行情協定，SBE over **UDP multicast**：
  - **A/B 兩路** 相同的 feed 走不同網路路徑，客戶端做 arbitration（取先到的、用序號去重）。
  - **Incremental feed**（增量）+ **Market Recovery feed**（定期快照）+ TCP replay：漏資料時可以先套快照再接增量。
- **撮合演算法不只 FIFO**：CME 支援多種配置，如 FIFO、Pro-Rata（依掛單量比例分配）、以及結合 top-order 優先、做市商（LMM）分配的混合規則。利率類期貨常見 pro-rata 變體。
- CME 已宣布與 Google Cloud 合作，規劃把撮合遷到專屬的雲端區域（待查證最新進度）——這是傳統上「必須自建機房」觀念的轉變。

啟示：**撮合規則是產品設計的一部分**，引擎要能依商品切換分配演算法；行情一定要設計「漏了怎麼補」。

## SBE —— 為什麼不是 Protobuf / JSON

SBE 由 Real Logic（Martin Thompson 等人）設計，FIX Trading Community 標準化，CME 採用：

- 欄位在固定偏移量，**直接在 buffer 上讀寫（flyweight）**，不需要「解碼成物件」這一步，也就沒有配置。
- 依序存取，對 CPU prefetcher 友善。
- 固定長度的區塊 + 可重複群組 + 可變長度欄位放最後。

本 lab 的 [`src/journal.rs`](../src/journal.rs) 用 40-byte 固定長度紀錄，就是同樣的思路。

## Aeron / Aeron Cluster —— 把「LMAX 架構」產品化

Aeron 同樣出自 Real Logic（現屬 Adaptive），是許多新一代交易系統的基礎設施：

- **Aeron Transport**：可靠的 UDP unicast / multicast，以及同機器的 **共享記憶體 IPC**。
- **Aeron Archive**：把訊息流錄下來、之後可重播。
- **Aeron Cluster**：以 **Raft** 共識協定複製的 **確定性狀態機**：
  - Leader 定序輸入、寫 log，複製到多數節點後才交給狀態機（你的撮合引擎）處理。
  - 每個節點跑同一份撮合程式，吃同一份 log，得到同樣的狀態。
  - 支援快照，新節點/重啟節點用「快照 + log 尾巴」追上。
- 等於把 LMAX 的「journal + replicator + BLP」變成現成框架。公開分享中有多家數位資產交易所與金融機構採用（例如 Coinbase 的新一代交易所，待查證）。

## LSE：TradElect → Millennium Exchange（反面教材）

倫敦證交所在 2000 年代使用以 .NET/Windows 建構的 TradElect，曾發生重大當機事故，且延遲在當時競爭者中偏高。
2009 年收購 MillenniumIT 後，2011 年改用以 Linux/C++ 建構的 Millennium Exchange 平台，延遲大幅下降。

啟示：技術棧本身不是一切，但 **對延遲與可預測性缺乏控制力的平台（GC 停頓、OS 排程不可控）在高頻市場中會被淘汰**。

## Eurex / Xetra T7

Deutsche Börse 的 T7 平台：下單用 **ETI（Enhanced Trading Interface）**，行情提供 **EOBI（Enhanced Order Book Interface，逐筆委託）** 等多種 feed。
Eurex 在回報中提供多個內部處理時間戳，讓會員能看到委託在 gateway/撮合各段花了多少時間——**透明的延遲量測** 也是現代交易所的服務之一。

## IEX —— 刻意變慢的交易所

IEX 在入口放了約 **350 µs 的「speed bump」**（一圈盤繞的光纖），並用「crumbling quote」訊號保護掛單者。
它說明了另一個面向：**低延遲交易所的設計同時也是市場結構/公平性設計**。相關做法還有：
- 主機共置（colocation）提供 **等長線纜**，讓所有會員到撮合引擎的物理距離相同。
- 學界提出的 **Frequent Batch Auctions**（Budish, Cramton, Shim, 2015）：以極短間隔的集合競價取代連續撮合，消除「速度軍備競賽」。
- 部分外匯平台曾加入隨機延遲或最小停留時間。

## 加密貨幣交易所 —— 同樣的核心，不同的限制

| 面向 | 傳統交易所 | 加密交易所 |
|---|---|---|
| 接入 | 主機共置、專線、二進位協定 | 公網 REST/WebSocket（JSON）為主，大客戶有 VIP 專線或雲端同區接入 |
| 部署 | 自建機房 | 多數在公有雲（大型交易所常見於 AWS 東京區域） |
| 交易時間 | 有收盤，可以每晚重啟、做快照 | 7×24，**不能停機**：升級、快照都要線上完成 |
| 風控 | 會員/結算會員有信用額度，事後結算 | **下單前就要凍結餘額**；永續合約要即時算保證金、強平 |
| 價格範圍 | 有漲跌幅，tick 陣列很合適 | 價格範圍大、精度高，tick 陣列要搭配動態平移或稀疏結構 |

加密交易所特有的難題是 **跨商品共享的帳戶餘額/保證金**：撮合可以依交易對分片，但同一帳戶在多個交易對下單時，餘額檢查必須一致。常見解法：
- 在撮合前加一個依「帳戶」分片的 **風控/餘額引擎**（同樣是單執行緒確定性狀態機），先凍結資金再送去撮合；成交後再回寫。
- 或在 gateway 端預先分配額度給各個撮合分片。

## 總結：大家都收斂到同一個模式

```text
  二進位協定 → 定序 → [journal ∥ 複製] → 單執行緒確定性撮合（依商品分片）→ 事件流 → multicast 行情 / 回報
```

差異主要在：用什麼語言（C++ / Java / Rust）、用什麼複製機制（primary-backup / Raft）、網路怎麼 bypass kernel、哪些部分丟給 FPGA。
