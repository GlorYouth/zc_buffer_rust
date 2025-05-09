# zc_buffer_rust: High-Performance Rust Zero-Copy Inspired Buffer Library based on Bytes

`zc_buffer_rust` is a high-performance zero-copy inspired buffer library written in Rust, based on the `bytes` crate. It
aims to provide efficient asynchronous data processing capabilities. Through a well-designed `Manager` Actor model,
`Agent` submission mechanism, and `Handle` interaction interface, the library implements data reservation, chunked
submission, automatic group merging, and finalization processing. It is particularly suitable for stream data processing
scenarios requiring high throughput and low latency.

## Core Components

The library mainly consists of the following core modules and structures:

* **`Manager`**:
  * Acts as the core Actor, responsible for managing buffer allocation, reservations, data grouping, and final data
    merging and dispatch.
  * Receives requests from `Handle` via MPSC channels and processes them asynchronously.
  * Maintains group state and automatically seals and processes groups when they reach a predefined commit size.
  * Provides a `finalize` mechanism to process all remaining groups and report information about failed groups.

* **`ZeroCopyHandle` (Operations Handle)**:
  * The entry point for users to interact with the `Manager`, encapsulating asynchronous communication logic.
  * Allows users to request write space reservations (`reserve_writer`), returning a `SubmitAgent`.
  * Provides a `finalize` method to trigger the `Manager`'s finalization process.
  * It is cloneable, allowing multiple writers to share the same `Manager` connection.

* **`SubmitAgent`, `SingleAgent`, `ChunkAgent` (Submission Agents)**:
  * `SubmitAgent`: Represents a successfully acquired write reservation credential; users need to convert it to either
    `SingleAgent` or `ChunkAgent`.
  * `SingleAgent`: Used for executing single, complete data submissions.
  * `ChunkAgent`: Used for executing chunked data submissions, allowing multiple data chunks to be added and then
    committed together via the `commit` method.
  * These Agents automatically notify the `Manager` of unfinished reservations upon `Drop`, ensuring resource state
    consistency.

* **`types.rs` (Core Type Definitions)**:
  * Defines basic types such as `ReservationId`, `AbsoluteOffset`, `GroupId`.
  * Defines request structures (e.g., `ReserveRequest`, `SubmitBytesRequest`) and reply channel types.
  * Defines data structures like `FailedReservationInfo`, `FailedGroupData`, `SuccessfulGroupData`, `GroupState`, etc.,
    to describe the state of reservations, groups, and processing results.

* **`error.rs` (Error Handling)**:
  * Defines `ManagerError` (internal Manager errors) and `BufferError` (top-level errors users might encounter when
    interacting with the Handle).

## Main Features

* **Zero-Copy Inspired**: Based on the `Bytes` type, aiming to reduce unnecessary data copies and improve performance.
* **Asynchronous Operations**: All interactions with the `Manager` are asynchronous, based on the `tokio` runtime.
* **Data Reservation and Submission**:
  * Clients first reserve space of a specified size.
  * Data can be submitted in a single go via `SingleAgent` or in chunks via `ChunkAgent`.
* **Automatic Group Merging**:
  * The `Manager` aggregates logically adjacent or related reservations into groups (`GroupState`).
  * When a group's total size reaches the preset `min_group_commit_size` threshold, the group is sealed.
  * Data from sealed groups where all reservations have been committed or have failed is merged and sent to data
    consumers via specific channels.
* **Failure Handling and Finalize**:
  * Agents automatically notify the `Manager` of their unfinished reservations upon `Drop`.
  * The `Manager` records information about failed reservations.
  * The `finalize` process handles all remaining groups, sending successfully merged data out and dispatching
    information about unsuccessfully processed groups (including committed data chunks and details of failed
    reservations) to a dedicated consumer via a failure data channel.
* **Clear Error Handling**: Defines detailed error types for easy problem diagnosis.

## Usage Example

The `src/main.rs` file in the project provides a detailed example of how to use the library:

1. **Start the Manager Actor**.
2. **Start consumer tasks for successful and failed data**, listening for data from the Manager respectively.
3. **Start multiple concurrent writer tasks**:

* Request reservations using `handle.reserve_writer()`.
* Convert the returned `SubmitAgent` into a `SingleAgent` or `ChunkAgent`.
* Submit data through the Agent.
* The example includes scenarios such as successful submissions, reservations that are not submitted (to test failure
  handling due to Agent Drop), and agent-level size validation failures.

4. **Wait for all writers to complete**.
5. **Call `handle.finalize()`** to process all unfinished reservations and groups and to obtain a final report.
6. **Wait for consumer tasks to finish and verify the results**.

## How to Run Benchmarks

The project includes a benchmark test located in `benches/zc_buffer_benchmark.rs`. You can run it using Criterion:

```bash
cargo bench
```

---

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
  * 定义了数据结构如 `FailedReservationInfo`, `FailedGroupData`, `SuccessfulGroupData`, `GroupState`
    等，用于描述预留、分组和处理结果的状态。

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
```