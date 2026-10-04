# 參考資料

> 建議閱讀順序：先看 ★ 標記的項目。

## 架構與案例

- ★ Martin Fowler, *The LMAX Architecture* (2011) — https://martinfowler.com/articles/lmax.html
- ★ LMAX Disruptor 技術論文與原始碼 — https://lmax-exchange.github.io/disruptor/
- ★ Brian Nigito, *How to Build an Exchange*（Jane Street Tech Talk, 2017）— 定序器架構、確定性狀態機、multicast 的完整講解（YouTube 可搜尋）
- Martin Thompson 的部落格 *Mechanical Sympathy* — https://mechanical-sympathy.blogspot.com/
- Aeron（傳輸、Archive、Cluster）— https://github.com/real-logic/aeron ；Aeron Cluster 文件位於其 wiki
- Agrona（低延遲資料結構）— https://github.com/real-logic/agrona
- Chronicle Queue — https://github.com/OpenHFT/Chronicle-Queue
- exchange-core（開源 Java 撮合引擎，LMAX Disruptor 架構；風控依用戶分片、撮合依商品分片，與 [07](07-simulation-architecture.md) 的設計相近）— https://github.com/exchange-core/exchange-core

## 協定規格

- Nasdaq OUCH / TotalView-ITCH / SoupBinTCP / MoldUDP64 — Nasdaq Trader 網站的 Technical Specifications 頁面
- CME iLink 3 與 MDP 3.0 — CME Group Client Systems Wiki
- Simple Binary Encoding (SBE) — https://github.com/real-logic/simple-binary-encoding ；FIX Trading Community 標準
- Eurex/Xetra T7 ETI、EOBI — Deutsche Börse 會員技術文件

## 量測

- ★ Gil Tene, *How NOT to Measure Latency*（演講，多個版本可在 YouTube/InfoQ 找到）— coordinated omission
- HdrHistogram — http://hdrhistogram.org/

## 系統調校

- Red Hat, *Low Latency Performance Tuning for RHEL* / *Real Time Tuning Guide*
- Linux kernel 文件：`Documentation/admin-guide/kernel-parameters.txt`（isolcpus、nohz_full、rcu_nocbs）
- Erik Rigtorp 的低延遲調校筆記與 SPSC queue 實作 — https://rigtorp.se/

## 市場結構

- Budish, Cramton, Shim, *The High-Frequency Trading Arms Race: Frequent Batch Auctions as a Market Design Response*, QJE 2015
- Michael Lewis, *Flash Boys*（2014）— IEX 與 speed bump 的背景（科普讀物）
- 歐盟 MiFID II RTS 25（時鐘同步要求）
