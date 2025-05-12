//! 定义 `ZeroCopyHandle`，这是用户与 `ManagerActor` 交互的主要接口。
//!
//! `ZeroCopyHandle` 封装了与 `ManagerActor` 进行异步通信的细节，
//! 提供方法来请求预留空间 (`reserve_writer`) 和触发最终化操作 (`finalize`)。
//! 它还被 `Agent` (如 `ChunkAgent`, `SingleAgent`) 内部使用来发送提交请求和失败信息。
//!
//! 这个句柄是 `Clone` 的，允许多个任务或线程共享对同一个 `ManagerActor` 的访问，
//! 从而可以并发地写入数据。

use crate::{
    agent::SubmitAgent, // 引入 `SubmitAgent`，作为 `reserve_writer` 成功后的返回值类型
    error::BufferError, // 引入顶层错误类型 `BufferError`
    types::{
        // 引入与请求、回复和状态相关的类型
        FailedInfoRequest,       // Agent drop 时发送的失败信息请求
        FailedReservationInfo, // 失败预留的具体信息结构体
        FinalizeResult,        // `finalize` 操作的结果报告
        Request,               // 发送给 Manager 的请求枚举
        ReservationId,         // 预留 ID 类型别名 (未使用，但保持引入可能有用)
        ReserveRequest,        // 预留请求的具体结构体
    },
};
use bytes::Bytes;
// 引入 `Bytes` 用于处理数据块
use std::num::NonZeroUsize;
// 用于确保预留大小非零
use tokio::sync::{mpsc, oneshot};
// 引入 Tokio 的 MPSC 和 Oneshot 通道，用于异步通信
use crate::types::SubmitParams;
// 引入 `SubmitParams`，主要在 Agent 内部使用
use tracing::{debug, error, info, trace, warn};
// 引入 `tracing` 日志宏

/// 与 `ManagerActor` 交互的句柄。
///
/// 这个结构体提供了一个异步接口，用于向关联的 `ManagerActor` 发送请求。
/// 它是 `Clone` 的，克隆操作成本很低（只复制内部的 MPSC 发送端），
/// 因此可以方便地在多个任务之间共享。
#[derive(Clone, Debug)] // 实现 Clone 和 Debug trait
pub struct ZeroCopyHandle {
    /// 用于向 `ManagerActor` 发送 `Request` 枚举的 MPSC 通道发送端。
    request_tx: mpsc::Sender<Request>,
}

impl ZeroCopyHandle {
    /// 创建一个新的 `ZeroCopyHandle` 实例。
    ///
    /// 这个方法是 `pub(crate)`，意味着它只能在 `zc_buffer` crate 内部被调用，
    /// 主要是在 `ManagerActor::spawn` 函数中创建初始的 Handle 时使用。
    ///
    /// # Arguments
    ///
    /// * `request_tx`: 一个 `mpsc::Sender<Request>`，用于将请求发送给 `ManagerActor` 的事件循环。
    ///
    /// # Returns
    ///
    /// 返回新创建的 `ZeroCopyHandle`。
    pub(crate) fn new(request_tx: mpsc::Sender<Request>) -> Self {
        Self { request_tx }
    }

    /// 异步请求在缓冲区中预留一块指定大小的写入空间。
    ///
    /// 这个方法向 `ManagerActor` 发送一个 `Request::Reserve` 消息。
    /// 如果 `ManagerActor` 成功处理了请求（例如，缓冲区有足够空间），
    /// 它会返回一个 `SubmitAgent` 实例。
    ///
    /// `SubmitAgent` 是一个临时的凭证对象，它本身不能直接用于提交数据。
    /// 用户必须调用 `SubmitAgent` 的 `into_single_agent()` 或 `into_chunk_agent()` 方法
    /// 将其转换为可以实际提交数据的 `SingleAgent` 或 `ChunkAgent`。
    ///
    /// 如果预留失败（例如 `ManagerActor` 已经停止、通道关闭或内部发生错误），
    /// 则返回一个 `BufferError`。
    ///
    /// # Arguments
    ///
    /// * `size`: 希望预留的空间大小（字节数），必须大于 0 (`NonZeroUsize`)。
    ///
    /// # Returns
    ///
    /// * `Ok(SubmitAgent)`: 预留成功，返回一个 `SubmitAgent` 凭证。
    /// * `Err(BufferError)`: 预留失败。错误类型包含了失败的具体原因，
    ///   可能是发送请求失败 (`SendRequestError`)、接收回复失败 (`ReceiveReplyError`)，
    ///   或者 `ManagerActor` 报告的业务逻辑错误 (`ManagerError`)。
    pub async fn reserve_writer(&self, size: NonZeroUsize) -> Result<SubmitAgent, BufferError> {
        // 1. 创建 Oneshot 通道：用于 Manager 向此 Handle 发送预留结果。
        let (reply_tx, reply_rx) = oneshot::channel();
        let requested_size = size.get(); // 获取 usize 类型的大小，主要用于日志记录

        // 2. 构建 Reserve 请求消息
        let request = Request::Reserve(ReserveRequest { size, reply_tx });

        // 3. 发送请求到 ManagerActor
        //    使用 `?` 操作符处理发送错误：如果 `send` 返回错误，将其转换为
        //    `BufferError::SendRequestError` 并立即返回。
        self.request_tx.send(request).await.map_err(|e| {
            error!("(Handle) 发送 Reserve 请求失败: {}", e);
            BufferError::SendRequestError(e)
        })?;
        trace!("(Handle) Reserve 请求已发送 (size: {})", requested_size);

        // 4. 等待 ManagerActor 通过 Oneshot 通道发回回复
        match reply_rx.await {
            // 4.1 Oneshot 通道成功接收到消息
            Ok(manager_result) => {
                match manager_result {
                    // 4.1.1 Manager 回复预留成功，包含 ID、偏移量和组 ID
                    Ok((id, offset, group_id)) => {
                        debug!(
                            "(Handle) 收到 Reserve 回复: ID={}, Offset={}, GroupId={}",
                            id, offset, group_id
                        );
                        // 使用获取的信息创建 SubmitAgent
                        let agent = SubmitAgent {
                            id,
                            group_id,
                            offset,
                            size: requested_size,
                            handle: self.clone(), // Agent 需要 Handle 来提交数据或失败信息
                            consumed: false,     // 初始状态为未消费
                        };
                        Ok(agent)
                    }
                    // 4.1.2 Manager 回复预留失败（业务逻辑错误）
                    Err(manager_err) => {
                        error!(
                            "(Handle) Reserve 请求失败 (Manager Error): {:?}",
                            manager_err
                        );
                        // 将 ManagerError 包装进 BufferError 返回
                        Err(BufferError::ManagerError(manager_err))
                    }
                }
            }
            // 4.2 Oneshot 通道接收失败 (通常意味着 Manager 在回复前已停止)
            Err(recv_error) => {
                error!("(Handle) 接收 Reserve 回复失败: {}", recv_error);
                // 将 RecvError 包装进 BufferError 返回
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    /// 异步请求 `ManagerActor` 执行最终化 (`Finalize`) 操作。
    ///
    /// Finalize 是一个清理和报告阶段。调用此方法会向 `ManagerActor` 发送
    /// 一个 `Request::Finalize` 消息。`ManagerActor` 收到后会：
    /// 1. 停止接受新的 `Reserve` 和 `SubmitBytes` 请求。
    /// 2. 处理所有当前进行中和已完成的分组。
    /// 3. 将成功处理的分组数据发送到 `completed_data_tx` 通道。
    /// 4. 将处理失败的分组信息 (`FailedGroupData`) 发送到 `failed_data_tx` 通道。
    /// 5. 向调用此方法的 Handle 回复一个 `Option<FinalizeResult>`。
    ///
    /// 此方法会一直等待 `ManagerActor` 完成上述操作并返回结果。
    /// 返回的 `FinalizeResult` 结构体 **只包含** 那些处理失败 **且** 未能成功通过
    /// `failed_data_tx` 发送出去的分组信息。这种情况可能发生在 `failed_data_tx`
    /// 通道关闭或缓冲区已满时。
    ///
    /// **注意：** 调用 `finalize` 会消耗 (`consume`) 这个 `ZeroCopyHandle` 实例。
    /// 这是为了防止在发送 `Finalize` 请求后，再使用同一个 Handle 发送其他请求。
    /// 如果需要保留 Handle，可以先 `clone()` 一个副本再调用 `finalize`。
    ///
    /// # Returns
    ///
    /// * `Ok(FinalizeResult)`: Finalize 操作成功完成。返回的 `FinalizeResult` 包含
    ///   未能发送的失败分组信息。如果所有失败分组都成功发送，则 `FinalizeResult` 为空。
    ///   如果 `ManagerActor` 在完成 finalize 时没有返回报告（可能是因为它已经在 finalize 过程中，
    ///   或者没有失败分组需要报告），也会返回一个空的 `FinalizeResult`。
    /// * `Err(BufferError)`: 发送 `Finalize` 请求失败 (`SendRequestError`) 或
    ///   等待接收回复失败 (`ReceiveReplyError`)。
    pub async fn finalize(self) -> Result<FinalizeResult, BufferError> {
        // 1. 创建 Oneshot 通道用于接收 Finalize 结果
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::Finalize { reply_tx };

        // 2. 发送 Finalize 请求
        //    注意这里处理错误的方式略有不同：如果发送失败，直接返回错误，
        //    因为无法继续等待回复。
        if let Err(e) = self.request_tx.send(request).await {
            error!("(Handle) 发送 Finalize 请求失败: {}", e);
            return Err(BufferError::SendRequestError(e));
        }
        trace!("(Handle) Finalize 请求已发送，等待 Manager 完成...");

        // 3. 等待 Manager 通过 Oneshot 通道发回回复 (Option<FinalizeResult>)
        match reply_rx.await {
            // 3.1 成功接收到 Manager 的回复
            Ok(option_finalize_result) => {
                match option_finalize_result {
                    // 3.1.1 Manager 返回了包含失败报告的 Some(FinalizeResult)
                    Some(finalize_result) => {
                        info!(
                            "(Handle) 收到 Finalize 结果报告 (含 {} 个需报告的失败组)",
                            finalize_result.failed_len()
                        );
                        Ok(finalize_result)
                    }
                    // 3.1.2 Manager 返回了 None
                    None => {
                        warn!("(Handle) Finalize 完成，但 Manager 未返回具体报告 (可能无失败组需报告或 Manager 已关闭)");
                        // 即使 Manager 返回 None，也认为 finalize 操作逻辑上完成了。
                        // 返回一个空的 FinalizeResult 表示没有需要调用者处理的、未能发送的失败信息。
                        Ok(FinalizeResult::default())
                    }
                }
            }
            // 3.2 Oneshot 通道接收失败 (Manager 可能已停止)
            Err(recv_error) => {
                error!("(Handle) 接收 Finalize 回复失败: {}", recv_error);
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    // --- 内部方法 (Internal Methods, pub(crate)) ---
    // 这些方法主要由 Agent 调用，用于将数据提交或失败信息发送给 Manager。

    /// (内部) 发送单块数据提交请求。
    /// 由 `SingleAgent::commit` 调用。
    pub(crate) async fn submit_single_bytes_internal(
        &self,
        params: SubmitParams, // 包含预留 ID、组 ID、偏移量
        data: Bytes,          // 要提交的数据块
    ) -> Result<(), BufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let reservation_id = params.res_id; // 用于日志

        // 使用 SubmitParams 的辅助方法构建请求
        let request = params.into_single_request(data, reply_tx);

        // 发送请求
        self.request_tx.send(request).await.map_err(|e| {
            error!(
                "(Handle) 发送 Single Submit 请求失败 (ResID {}): {}",
                reservation_id, e
            );
            BufferError::SendRequestError(e)
        })?;
        trace!("(Handle) Single Submit 请求已发送 (ResID {})", reservation_id);

        // 等待回复
        match reply_rx.await {
            Ok(Ok(())) => {
                // Manager 确认提交成功
                debug!("(Handle) Single Submit 成功 (ResID {})", reservation_id);
                Ok(())
            }
            Ok(Err(manager_err)) => {
                // Manager 报告提交失败
                error!(
                    "(Handle) Single Submit 失败 (ResID {}, Manager Error): {:?}",
                    reservation_id, manager_err
                );
                Err(BufferError::ManagerError(manager_err))
            }
            Err(recv_error) => {
                // 接收回复失败
                error!(
                    "(Handle) 接收 Single Submit 回复失败 (ResID {}): {}",
                    reservation_id, recv_error
                );
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    /// (内部) 发送分块数据提交请求。
    /// 由 `ChunkAgent::commit` 调用。
    pub(crate) async fn submit_chunked_bytes_internal(
        &self,
        params: SubmitParams, // 包含预留 ID、组 ID、偏移量
        chunks: Vec<Bytes>,   // 要提交的数据块列表
    ) -> Result<(), BufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let reservation_id = params.res_id; // 用于日志

        // 构建请求
        let request = params.into_chunked_request(chunks, reply_tx);

        // 发送请求
        self.request_tx.send(request).await.map_err(|e| {
            error!(
                "(Handle) 发送 Chunked Submit 请求失败 (ResID {}): {}",
                reservation_id, e
            );
            BufferError::SendRequestError(e)
        })?;
        trace!("(Handle) Chunked Submit 请求已发送 (ResID {})", reservation_id);

        // 等待回复 (与 Single Submit 逻辑相同)
        match reply_rx.await {
            Ok(Ok(())) => {
                debug!("(Handle) Chunked Submit 成功 (ResID {})", reservation_id);
                Ok(())
            }
            Ok(Err(manager_err)) => {
                error!(
                    "(Handle) Chunked Submit 失败 (ResID {}, Manager Error): {:?}",
                    reservation_id, manager_err
                );
                Err(BufferError::ManagerError(manager_err))
            }
            Err(recv_error) => {
                error!(
                    "(Handle) 接收 Chunked Submit 回复失败 (ResID {}): {}",
                    reservation_id, recv_error
                );
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    /// (内部) 同步地（尽力而为）发送预留失败信息给 Manager。
    ///
    /// 由 `Agent` (如 `SubmitAgent`, `SingleAgent`, `ChunkAgent`) 在 `Drop` 时调用，
    /// 如果预留没有被成功提交，则需要通知 Manager 这个预留失败了。
    ///
    /// 这个方法使用 `try_send`，而不是 `send().await`，因为 `Drop` 不能是 `async` 的。
    /// 这意味着如果请求通道已满，这个失败信息可能会被丢弃。
    /// Manager 的 `Finalize` 逻辑需要能够处理这种情况（即某些失败的预留可能没有收到明确的 `FailedInfoRequest`）。
    ///
    /// # Arguments
    ///
    /// * `failed_info`: 包含失败预留信息的结构体。
    pub(crate) fn send_failed_info(&self, failed_info: FailedReservationInfo) {
        let reservation_id = failed_info.id; // 用于日志
        let request = Request::FailedInfo(FailedInfoRequest { info: failed_info });

        // 尝试发送，不等待
        match self.request_tx.try_send(request) {
            Ok(_) => {
                trace!(
                    "(Handle) 已发送 FailedInfo (ResID {}) (try_send 成功)",
                    reservation_id
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // 通道已满，消息被丢弃
                warn!(
                    "(Handle) 发送 FailedInfo 失败 (ResID {}): 通道已满，信息可能丢失",
                    reservation_id
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // 通道已关闭 (Manager 可能已停止)
                warn!(
                    "(Handle) 发送 FailedInfo 失败 (ResID {}): 通道已关闭 (Manager 可能已停止)",
                    reservation_id
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! 包含 `handle` 模块中 `ZeroCopyHandle` 相关功能的单元测试。
    use super::*;
    // 导入父模块（`handle.rs`）中的所有公开项
    use crate::types::{
        AbsoluteOffset, CommitType, FailedReservationInfo, FinalizeResult, GroupId, Request,
    };
    // 导入测试中需要用到的类型
    use crate::ManagerError;
    // 导入 Manager 错误类型，用于模拟 Manager 的错误回复
    use std::num::NonZeroUsize;
    use tokio::sync::mpsc;
    // 导入 Tokio 相关组件

    /// 测试 `ZeroCopyHandle::new` 函数。
    /// 验证是否可以成功创建一个 Handle 实例。
    /// 由于内部字段是私有的，主要通过 `clone` 和 `Debug` 输出来间接验证。
    #[test]
    fn test_zero_copy_handle_new() {
        let (request_tx, _request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);
        // 检查克隆是否能成功
        let _cloned_handle = handle.clone();
        // 检查 Debug 输出是否包含类型名称
        assert!(format!("{:?}", handle).contains("ZeroCopyHandle"));
    }

    /// 测试 `reserve_writer` 方法在 Manager 成功响应时的行为。
    /// 模拟 Manager 接收到 Reserve 请求后，返回成功的预留信息。
    /// 验证 Handle 是否能正确接收并解析回复，返回有效的 `SubmitAgent`。
    #[tokio::test]
    async fn test_reserve_writer_success() {
        // 创建 MPSC 通道模拟 Handle 和 Manager 之间的通信
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        // 启动一个后台任务模拟 Manager Actor
        tokio::spawn(async move {
            // 等待接收请求
            if let Some(request) = request_rx.recv().await {
                // 检查是否是 Reserve 请求
                if let Request::Reserve(req) = request {
                    let res_id: ReservationId = 1;
                    let offset: AbsoluteOffset = 100;
                    let group_id: GroupId = 0;
                    // 通过 oneshot 通道发送成功的回复
                    req.reply_tx.send(Ok((res_id, offset, group_id))).unwrap();
                } else {
                    panic!("模拟 Manager 期望收到 Reserve 请求");
                }
            } else {
                panic!("模拟 Manager 未收到请求");
            }
        });

        // 调用 handle 的 reserve_writer
        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(submit_agent) => {
                // 验证返回的 SubmitAgent 的字段是否符合预期
                assert_eq!(submit_agent.id, 1);
                assert_eq!(submit_agent.offset, 100);
                assert_eq!(submit_agent.size, 50);
                assert!(!submit_agent.consumed); // 初始应为未消费
            }
            Err(e) => panic!("reserve_writer 应该成功, 但返回错误: {:?}", e),
        }
    }

    /// 测试 `reserve_writer` 方法在 Manager 返回业务错误时的行为。
    /// 模拟 Manager 接收到 Reserve 请求后，返回一个 `ManagerError`。
    /// 验证 Handle 是否能正确接收错误并将其包装在 `BufferError::ManagerError` 中返回。
    #[tokio::test]
    async fn test_reserve_writer_manager_error() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::Reserve(req)) = request_rx.recv().await {
                // 模拟 Manager 返回一个内部错误
                req.reply_tx
                    .send(Err(ManagerError::Internal("测试错误".to_string())))
                    .unwrap();
            }
        });

        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(_) => panic!("reserve_writer 应该失败"),
            // 验证返回的错误是否是预期的 ManagerError
            Err(BufferError::ManagerError(ManagerError::Internal(msg))) => {
                assert_eq!(msg, "测试错误");
            }
            Err(e) => panic!("期望 ManagerError::Internal, 但得到 {:?}", e),
        }
    }

    /// 测试 `reserve_writer` 方法在请求通道关闭时的行为。
    /// 通过 `drop` 接收端来模拟通道关闭。
    /// 验证 Handle 在发送请求时是否能检测到通道关闭，并返回 `BufferError::SendRequestError`。
    #[tokio::test]
    async fn test_reserve_writer_send_error() {
        let (request_tx, request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);
        drop(request_rx); // 关闭接收端，这将导致发送失败

        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(_) => panic!("reserve_writer 应该因为通道关闭而失败"),
            // 验证返回的错误是否是 SendRequestError
            Err(BufferError::SendRequestError(_)) => {
                // 测试通过
            }
            Err(e) => panic!("期望 SendRequestError, 但得到 {:?}", e),
        }
    }

    /// 测试 `finalize` 方法在 Manager 成功响应并返回报告时的行为。
    /// 模拟 Manager 接收到 Finalize 请求后，成功完成并返回一个空的 `FinalizeResult`。
    /// 验证 Handle 能正确接收并返回该结果。
    #[tokio::test]
    async fn test_finalize_success() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx); // 注意：finalize 会消耗 handle

        tokio::spawn(async move {
            if let Some(Request::Finalize { reply_tx }) = request_rx.recv().await {
                // 模拟成功 finalize 并返回默认（空）报告
                let finalize_result = FinalizeResult::default();
                reply_tx.send(Some(finalize_result)).unwrap();
            }
        });

        match handle.finalize().await {
            Ok(report) => {
                // 验证返回的报告是否为空
                assert!(report.is_empty());
            }
            Err(e) => panic!("finalize 应该成功, 但返回错误: {:?}", e),
        }
    }

    /// 测试 `finalize` 方法在 Manager 成功响应但不返回报告 (`None`) 时的行为。
    /// 模拟 Manager 接收到 Finalize 请求后，完成操作但通过 oneshot 发送 `None`。
    /// 验证 Handle 是否会将 `None` 解释为成功完成，并返回一个默认的空 `FinalizeResult`。
    #[tokio::test]
    async fn test_finalize_success_none_report() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::Finalize { reply_tx }) = request_rx.recv().await {
                // 模拟 Manager 返回 None
                reply_tx.send(None).unwrap();
            }
        });

        match handle.finalize().await {
            Ok(report) => {
                // 验证即使 Manager 返回 None，handle 也返回一个空的报告
                assert!(report.is_empty(), "报告应该为空，因为 Manager 返回了 None");
            }
            Err(e) => panic!("finalize 应该成功（即使报告为 None）, 但返回错误: {:?}", e),
        }
    }

    /// 测试内部方法 `submit_single_bytes_internal` 在成功时的行为。
    /// 模拟 Manager 接收 SubmitBytes 请求并回复成功。
    /// 验证 Handle 能正确发送请求并处理成功回复。
    #[tokio::test]
    async fn test_submit_single_bytes_internal_success() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = request_rx.recv().await {
                // 验证收到的请求内容是否符合预期
                assert_eq!(req.reservation_id, 1);
                assert_eq!(req.absolute_offset, 100);
                matches!(req.data, CommitType::Single(_)); // 检查是否是 Single 类型
                // 发送成功回复
                req.reply_tx.send(Ok(())).unwrap();
            }
        });

        // 调用内部提交方法
        let result = handle
            .submit_single_bytes_internal(
                SubmitParams { // 模拟 Agent 提供的参数
                    res_id: 1,
                    group_id: 0,
                    offset: 100,
                },
                Bytes::from_static(b"test"), // 模拟提交的数据
            )
            .await;
        // 验证结果是 Ok
        assert!(result.is_ok());
    }

    /// 测试内部方法 `submit_chunked_bytes_internal` 在 Manager 返回错误时的行为。
    /// 模拟 Manager 接收 SubmitBytes 请求并回复一个 `AlreadyCommitted` 错误。
    /// 验证 Handle 能正确发送请求并处理错误回复。
    #[tokio::test]
    async fn test_submit_chunked_bytes_internal_error() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = request_rx.recv().await {
                // 模拟返回 AlreadyCommitted 错误
                req.reply_tx
                    .send(Err(ManagerError::AlreadyCommitted(req.reservation_id)))
                    .unwrap();
            }
        });

        let chunks = vec![Bytes::from_static(b"chunk1")];
        let result = handle
            .submit_chunked_bytes_internal(
                SubmitParams {
                    res_id: 2,
                    group_id: 0,
                    offset: 200,
                },
                chunks,
            )
            .await;
        // 验证返回的错误是预期的 AlreadyCommitted
        match result {
            Err(BufferError::ManagerError(ManagerError::AlreadyCommitted(res_id))) => {
                assert_eq!(res_id, 2);
            }
            _ => panic!("期望 AlreadyCommitted 错误, 得到 {:?}", result),
        }
    }

    /// 测试内部方法 `send_failed_info` 的基本功能。
    /// 这个方法使用 `try_send`，所以测试需要验证消息是否真的被发送到了通道中。
    #[tokio::test]
    async fn test_send_failed_info() {
        // 创建一个有足够缓冲的通道
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(10);
        let handle = ZeroCopyHandle::new(request_tx);

        let failed_info = FailedReservationInfo {
            id: 123,
            group_id: 0,
            offset: 456,
            size: 789,
        };

        // 调用 send_failed_info
        handle.send_failed_info(failed_info.clone());

        // 尝试从通道接收消息，并设置超时以防万一发送失败
        match tokio::time::timeout(std::time::Duration::from_millis(100), request_rx.recv()).await {
            Ok(Some(Request::FailedInfo(req))) => {
                // 验证接收到的请求类型和内容是否正确
                assert_eq!(req.info, failed_info);
            }
            Ok(Some(_)) => panic!("收到了非 FailedInfo 类型的请求"),
            Ok(None) => panic!("通道意外关闭"),
            Err(_) => panic!("在规定时间内未收到 FailedInfo 请求 (try_send 可能失败或未发送)"),
        }
    }

    /// 测试 `send_failed_info` 在通道已满或已关闭时的行为。
    /// 由于 `send_failed_info` 使用 `try_send` 并且不返回错误，
    /// 这个测试主要关注调用该方法不会导致 panic，并间接验证其容错性。
    /// （注意：我们无法直接断言内部是否打印了警告日志）。
    #[tokio::test]
    async fn test_send_failed_info_channel_full_or_closed() {
        // --- 情况 1: 通道已满 ---
        // 创建一个容量为 1 的通道
        let (request_tx_full, mut request_rx_full) = mpsc::channel::<Request>(1);
        let handle_full = ZeroCopyHandle::new(request_tx_full);

        // 先发送一个消息填满通道
        let (dummy_reply_tx, _) = oneshot::channel();
        handle_full
            .request_tx
            .send(Request::Finalize { reply_tx: dummy_reply_tx })
            .await
            .unwrap();

        // 准备失败信息
        let failed_info_full = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 1,
            size: 1,
        };
        // 此时调用 send_failed_info，try_send 应该失败（因为通道已满）
        handle_full.send_failed_info(failed_info_full.clone());
        // 我们期望这个调用不会 panic，并且内部会记录一个警告（无法直接测试日志）

        // 确认通道中确实只有一个消息（之前发送的 Finalize）
        assert!(request_rx_full.recv().await.is_some());
        // 再尝试接收应该是 None 或超时
        assert!(tokio::time::timeout(std::time::Duration::from_millis(10), request_rx_full.recv()).await.is_err() || request_rx_full.recv().await.is_none());

        // --- 情况 2: 通道已关闭 ---
        let (request_tx_closed, request_rx_closed) = mpsc::channel::<Request>(1);
        let handle_closed = ZeroCopyHandle::new(request_tx_closed);
        drop(request_rx_closed); // 关闭接收端

        let failed_info_closed = FailedReservationInfo {
            id: 2,
            group_id: 0,
            offset: 2,
            size: 2,
        };
        // 调用 send_failed_info，try_send 应该失败（因为通道已关闭）
        handle_closed.send_failed_info(failed_info_closed);
        // 同样，期望这个调用不会 panic，并且内部会记录一个警告。
    }
}
