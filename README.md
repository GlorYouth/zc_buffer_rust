# zc_buffer_rust: 基于 Bytes 的高性能 Rust 零拷贝缓冲库

`zc_buffer_rust` 是一个使用 Rust 语言编写的高性能零拷贝缓冲库，它基于 `bytes` crate，旨在提供高效的异步数据处理能力。该库通过精心设计的 `Manager` Actor模型、`Agent` 提交机制以及 `Handle` 交互句柄，实现了数据的预留、分块提交、自动分组合并与最终化处理，特别适用于需要高吞吐量和低延迟的流数据处理场景。

## 核心组件

该库主要由以下几个核心模块和结构组成：

* **`Manager` (管理器):**
    * 作为核心 Actor，负责管理缓冲区的分配、预留、数据分组以及最终的数据合并与分发。
    * 通过 MPSC 通道接收来自 `Handle` 的请求，并异步处理。
    * 维护分组状态，当分组达到预设的提交大小时会自动密封并处理。
    * 提供 `finalize` 机制，用于处理所有剩余的分组，并报告处理失败的分组信息。

* **`ZeroCopyHandle` (操作句柄):**
    * 用户与 `Manager` 交互的入口，封装了异步通信逻辑。
    * 允许用户请求预留写入空间 (`reserve_writer`)，并返回一个 `SubmitAgent`。
    * 提供 `finalize` 方法来触发 `Manager` 的最终化处理流程。
    * 可以被克隆，允许多个写入者共享同一个 `Manager` 连接。

* **`SubmitAgent`, `SingleAgent`, `ChunkAgent` (提交代理):**
    * `SubmitAgent`: 代表一个已成功获取的预留写入凭证，用户需将其转换为 `SingleAgent` 或 `ChunkAgent`。
    * `SingleAgent`: 用于执行单次完整数据提交。
    * `ChunkAgent`: 用于执行分块数据提交，允许多次添加数据块，最后通过 `commit` 方法统一提交。
    * 这些 Agent 在 `Drop` 时会自动通知 `Manager` 未完成的预留，确保资源状态的一致性。

* **`types.rs` (核心类型定义):**
    * 定义了如 `ReservationId`, `AbsoluteOffset`, `GroupId` 等基础类型。
    * 定义了请求结构体 (如 `ReserveRequest`, `SubmitBytesRequest`) 和回复通道类型。
    * 定义了数据结构如 `FailedReservationInfo`, `FailedGroupData`, `SuccessfulGroupData`, `GroupState` 等，用于描述预留、分组和处理结果的状态。

* **`error.rs` (错误处理):**
    * 定义了 `ManagerError` (Manager 内部错误) 和 `BufferError` (用户与 Handle 交互时可能遇到的顶层错误)。

## 主要特性

* **零拷贝 (Zero-Copy Inspired):** 基于 `Bytes` 类型，旨在减少不必要的数据拷贝，提高性能。
* **异步操作:** 所有与 `Manager` 的交互都是异步的，基于 `tokio` 运行时。
* **数据预留与提交:**
    * 客户端首先预留指定大小的空间。
    * 可以通过 `SingleAgent` 一次性提交完整数据，或通过 `ChunkAgent` 分块提交数据。
* **自动分组合并:**
    * `Manager` 会将逻辑上相邻或相关的预留聚合到分组 (`GroupState`) 中。
    * 当分组的总大小达到预设的 `min_group_commit_size` 阈值时，分组会被密封。
    * 密封且所有预留都已提交或处理失败的分组，其数据会被合并并通过特定通道发送给数据消费者。
* **失败处理与 Finalize:**
    * Agent 在 Drop 时会自动通知 `Manager` 其未完成的预留。
    * `Manager` 记录失败的预留信息。
    * `finalize` 过程会处理所有剩余的分组，将成功合并的数据发送出去，并将无法成功处理的分组信息（包括已提交的数据块和失败的预留详情）通过失败数据通道发送给专门的消费者。
* **清晰的错误处理:** 定义了详细的错误类型，方便问题定位。

## 使用示例

项目中的 `src/main.rs` 文件提供了一个详细的示例，展示了如何使用该库：

1.  **启动 Manager Actor**。
2.  **启动成功数据和失败数据的消费者任务**，分别监听来自 Manager 的数据。
3.  **启动多个并发写入者任务**：
    * 使用 `handle.reserve_writer()` 请求预留。
    * 将返回的 `SubmitAgent` 转换为 `SingleAgent` 或 `ChunkAgent`。
    * 通过 Agent 提交数据。
    * 示例中包含了成功提交、预留后不提交 (测试 Agent Drop 导致的失败处理)、以及在 Agent 层进行大小校验失败等多种场景。
4.  **等待所有写入者完成**。
5.  **调用 `handle.finalize()`** 来处理所有未完成的预留和分组，并获取最终报告。
6.  **等待消费者任务结束并验证结果**。

## 如何运行基准测试

项目包含了一个基准测试，位于 `benches/zc_buffer_benchmark.rs`。 你可以使用 Criterion 来运行它：

```bash
cargo bench