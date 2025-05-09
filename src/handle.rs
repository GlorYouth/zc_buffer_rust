//! 定义用户与 Manager Actor 交互的句柄 (`ZeroCopyHandle`)。
//! Defines the handle for users to interact with the Manager Actor (`ZeroCopyHandle`).
//! Handle 封装了与 Manager 的异步通信逻辑。
//! Handle encapsulates the asynchronous communication logic with the Manager.

use crate::{
    agent::SubmitAgent, // 引入初始凭证 Agent
    // Import initial credential Agent
    error::BufferError, // 引入错误类型
    // Import error types
    types::{
        FailedInfoRequest, FailedReservationInfo, FinalizeResult, Request, ReservationId,
        ReserveRequest,
    },
};
use bytes::Bytes;
use std::num::NonZeroUsize;
use tokio::sync::{mpsc, oneshot};
// 引入 tokio 的通道
// Import tokio channels
use crate::types::SubmitParams;
use tracing::{debug, error, info, trace, warn};

/// 用于与 Manager Actor 交互的句柄。
/// Handle for interacting with the Manager Actor.
/// 它是 Clone 的，允许多个写入者共享同一个 Manager 连接。
/// It is Clone-able, allowing multiple writers to share the same Manager connection.
#[derive(Clone, Debug)] // 添加 Debug trait
                        // Add Debug trait
pub struct ZeroCopyHandle {
    /// 用于向 Manager 发送请求的 MPSC 通道发送端。
    /// MPSC channel sender for sending requests to the Manager.
    request_tx: mpsc::Sender<Request>,
}

impl ZeroCopyHandle {
    /// 创建一个新的 ZeroCopyHandle (内部使用)。
    /// Create a new ZeroCopyHandle (internal use).
    ///
    /// # Arguments
    ///
    /// * `request_tx` - 连接到 Manager 的请求通道发送端。
    /// * `request_tx` - Request channel sender connected to the Manager.
    ///
    /// # Returns
    ///
    /// 一个新的 `ZeroCopyHandle` 实例。
    /// A new `ZeroCopyHandle` instance.
    pub(crate) fn new(request_tx: mpsc::Sender<Request>) -> Self {
        Self { request_tx }
    }

    /// 向 Manager 请求预留一块指定大小的写入空间。
    /// Requests a reservation of a specified size from the Manager.
    ///
    /// 成功时返回一个 `SubmitAgent`，它代表了预留的凭证。
    /// When successful, returns a `SubmitAgent` representing the reservation credential.
    /// 用户需要使用 `SubmitAgent` 的 `into_single_agent` 或 `into_chunk_agent`
    /// Users need to use `SubmitAgent`'s `into_single_agent` or `into_chunk_agent`
    /// 来获取具体的提交代理。
    /// to get the specific submission agent.
    ///
    /// # Arguments
    ///
    /// * `size` - 请求预留的大小 (必须大于 0)。
    /// * `size` - Size of the requested reservation (must be greater than 0).
    ///
    /// # Returns
    ///
    /// * `Ok(SubmitAgent)` - 预留成功，返回提交代理凭证。
    /// * `Ok(SubmitAgent)` - Reservation successful, returns the submission agent credential.
    /// * `Err(BufferError)` - 预留失败 (例如 Manager 关闭或内部错误)。
    /// * `Err(BufferError)` - Reservation failed (e.g., Manager closed or internal error).
    pub async fn reserve_writer(&self, size: NonZeroUsize) -> Result<SubmitAgent, BufferError> {
        // 创建一个 oneshot 通道用于接收 Manager 的回复
        // Create a new oneshot channel to receive the Manager's reply
        let (reply_tx, reply_rx) = oneshot::channel();
        let requested_size = size.get(); // 获取 usize 值
                                         // Get the usize value

        // 构建 Reserve 请求
        // Builds the Reserve request
        let request = Request::Reserve(ReserveRequest { size, reply_tx });

        // 发送请求到 Manager
        // Send the request to the Manager
        self.request_tx
            .send(request)
            .await
            .map_err(BufferError::SendRequestError)?; // 发送失败则包装错误并返回
                                                      // If sending fails, wrap the error and return
        trace!("(Handle) Reserve 请求已发送 (size: {})", requested_size);
        // trace!("(Handle) Reserve request sent (size: {})", requested_size);

        // 等待 Manager 的回复
        // Wait for the Manager's reply
        match reply_rx.await {
            Ok(Ok((id, offset, group_id))) => {
                // Manager 回复成功
                // Manager replied successfully
                debug!(
                    "(Handle) 收到 Reserve 回复: ID={}, Offset={}, GroupId={}",
                    id, offset, group_id
                );
                // 创建 SubmitAgent 凭证
                // Create SubmitAgent credential
                let agent = SubmitAgent {
                    id,
                    group_id,
                    offset,
                    size: requested_size,
                    handle: self.clone(), // 克隆 Handle 供 Agent 使用
                    // Clone Handle for Agent use
                    consumed: false, // 初始未消费
                                     // Initially not consumed
                };
                Ok(agent)
            }
            Ok(Err(manager_err)) => {
                // Manager 回复了一个业务错误
                // Manager replied with a business error
                error!(
                    "(Handle) Reserve 请求失败 (Manager Error): {:?}",
                    manager_err
                );
                Err(BufferError::ManagerError(manager_err))
            }
            Err(recv_error) => {
                //接收 Manager 回复失败 (oneshot 通道关闭)
                //Failed to receive Manager reply (oneshot channel closed)
                error!("(Handle) 接收 Reserve 回复失败: {}", recv_error);
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    /// 请求 Manager 执行 Finalize 操作。
    /// Request the Manager to perform a Finalize operation.
    ///
    /// Finalize 会处理所有剩余的分组，并将失败分组信息发送到失败数据通道。
    /// Finalize processes all remaining groups and sends failed group information to the failure data channel.
    /// 此方法会等待 Manager 完成 Finalize 操作，并返回一个 `FinalizeResult`，
    /// This method waits for the Manager to complete the Finalize operation and returns a `FinalizeResult`,
    /// 其中包含那些 **未能** 通过失败通道发送出去的失败分组信息 (通常在通道关闭或阻塞时发生)。
    /// which contains failed group information that could **not** be sent through the failure channel. (Typically occurs when the channel is closed or blocked.)
    ///
    /// 注意：调用此方法会消耗 Handle。
    /// Note: Calling this method consumes the Handle.
    ///
    /// # Returns
    ///
    /// * `Ok(FinalizeResult)` - Finalize 操作完成，返回报告。
    /// * `Ok(FinalizeResult)` - Finalize operation completed, returns the report.
    /// * `Err(BufferError)` - 发送 Finalize 请求或接收回复失败。
    /// * `Err(BufferError)` - Failed to send Finalize request or receive a reply.
    pub async fn finalize(self) -> Result<FinalizeResult, BufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = Request::Finalize { reply_tx };

        // 发送 Finalize 请求
        // Send Finalize request
        if let Err(e) = self.request_tx.send(request).await {
            error!("(Handle) 发送 Finalize 请求失败: {}", e);
            // 直接返回发送错误，因为无法继续
            // Directly return the error sent, as we cannot continue
            return Err(BufferError::SendRequestError(e));
        }
        trace!("(Handle) Finalize 请求已发送，等待 Manager 完成...");
        // trace!("(Handle) Finalize request sent, waiting for Manager to complete...");

        // 等待 Manager 的回复 (Option<FinalizeResult>)
        // Wait for Manager's reply (Option<FinalizeResult>)
        match reply_rx.await {
            Ok(Some(finalize_result)) => {
                // Manager 成功执行 finalize 并返回了结果报告
                // Manager successfully executed finalize and returned a result report
                info!(
                    "(Handle) 收到 Finalize 结果报告 (含 {} 个需报告的失败组)",
                    finalize_result.failed_len()
                );
                // info!("(Handle) Received Finalize result report (containing {} failed groups to report)", finalize_result.failed_len());
                Ok(finalize_result)
            }
            Ok(None) => {
                // Manager 完成了 Finalize，但没有返回报告
                // Manager completed Finalize, but didn't return a report
                // 这可能是因为 finalize_internal 发现已经在 finalize，或者没有失败组需要报告
                // This may be because finalize_internal found it was already finalizing, or there were no failed groups to report
                warn!("(Handle) Finalize 完成，但 Manager 未返回具体报告 (可能无失败组需报告或 Manager 已关闭)");
                // warn!("(Handle) Finalize completed, but Manager didn't return a specific report (possibly no failed groups to report or Manager is closed)");
                // 在这种情况下，返回一个空的 FinalizeResult 是合理的
                // In this case, returning an empty FinalizeResult is reasonable
                Ok(FinalizeResult::default()) // 返回空的报告
                                              // Return an empty report
            }
            Err(recv_error) => {
                // oneshot 通道接收错误 (Manager 可能已崩溃)
                // oneshot channel receive error (Manager may have crashed)
                error!("(Handle) 接收 Finalize 回复失败: {}", recv_error);
                // error!("(Handle) Failed to receive Finalize reply: {}", recv_error);
                Err(BufferError::ReceiveReplyError(recv_error))
            }
        }
    }

    // --- 以下是供 Agent 使用的内部辅助方法 ---
    // --- The following are internal helper methods for Agent use ---

    /// (内部) 发送单块提交请求给 Manager (由 SingleAgent 调用)
    /// (Internal) Send a single block submission request to the Manager (called by SingleAgent)
    pub(crate) async fn submit_single_bytes_internal(
        &self,
        params: SubmitParams,
        data: Bytes,
    ) -> Result<(), BufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = params.into_single_request(data, reply_tx);

        self.request_tx
            .send(request)
            .await
            .map_err(BufferError::SendRequestError)?; // 处理发送错误
                                                      // Handle sending error

        // 等待 Manager 的确认
        // Wait for Manager's confirmation
        reply_rx
            .await // 等待 oneshot 回复
            // Wait for oneshot reply
            .map_err(BufferError::ReceiveReplyError)? // 处理接收错误
            // Handle receiving error
            .map_err(BufferError::ManagerError) // 将 Result<Ok, ManagerError> 转换为 Result<Ok, BufferError>
                                                // Convert Result<Ok, ManagerError> to Result<Ok, BufferError>
    }

    /// (内部) 发送分块提交请求给 Manager (由 ChunkAgent 调用)
    /// (Internal) Send a chunked submission request to the Manager (called by ChunkAgent)
    pub(crate) async fn submit_chunked_bytes_internal(
        &self,
        params: SubmitParams,
        chunks: Vec<Bytes>,
    ) -> Result<(), BufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = params.into_chunked_request(chunks, reply_tx);

        self.request_tx
            .send(request)
            .await
            .map_err(BufferError::SendRequestError)?; // 处理发送错误
                                                      // Handle sending error

        // 等待 Manager 的确认
        // Wait for Manager's confirmation
        reply_rx
            .await // 等待 oneshot 回复
            // Wait for oneshot reply
            .map_err(BufferError::ReceiveReplyError)? // 处理接收错误
            // Handle receiving error
            .map_err(BufferError::ManagerError) // 将 Result<Ok, ManagerError> 转换为 Result<Ok, BufferError>
                                                // Convert Result<Ok, ManagerError> to Result<Ok, BufferError>
    }

    /// (内部) 发送预留失败信息给 Manager (由 Agent 的 Drop 调用)
    /// (Internal) Send reservation failure information to the Manager (called by Agent's Drop)
    /// 使用 `try_send` 以避免阻塞 Drop。
    /// Use `try_send` to avoid blocking Drop.
    pub(crate) fn send_failed_info(&self, failed_info: FailedReservationInfo) {
        let req = Request::FailedInfo(FailedInfoRequest {
            info: failed_info.clone(), // 克隆信息以便发送
                                       // Clone information for sending
        });
        // 尝试发送，如果通道已满或关闭则记录警告，但不阻塞
        // Try to send, log a warning if the channel is full or closed, but don't block
        if let Err(e) = self.request_tx.try_send(req) {
            warn!(
                "(Handle) 发送 Res {} (Offset {}) 的 FailedInfo 失败 (通道可能已满或关闭): {}",
                failed_info.id, failed_info.offset, e
            );
            // warn!(
            //     "(Handle) Failed to send FailedInfo for Res {} (Offset {}) (channel may be full or closed): {}",
            //     failed_info.id, failed_info.offset, e
            // );
            // 根据错误类型可以决定是否需要更复杂的处理，例如放入一个备用队列
            // Based on the error type, more complex handling might be needed, such as putting it in a backup queue
            // if let mpsc::error::TrySendError::Closed(_) = e {
            //     // Manager 已关闭，无需处理
            //     // Manager is closed, no need to handle
            // } else {
            //     // 通道已满，可能需要策略重试或记录
            //     // Channel is full, may need strategy retry or logging
            // }
        } else {
            trace!(
                "(Handle) Res {} (Offset {}) 的 FailedInfo 已发送 (try_send)",
                failed_info.id,
                failed_info.offset
            );
            // trace!(
            //     "(Handle) FailedInfo for Res {} (Offset {}) has been sent (try_send)",
            //     failed_info.id,
            //     failed_info.offset
            // );
        }
    }
}

// 在 src/handle.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*;
    // 导入父模块中的所有内容
    // Import all contents from the parent module
    use crate::types::{
        AbsoluteOffset, CommitType, FailedReservationInfo, FinalizeResult, GroupId, Request,
    };
    use crate::ManagerError;
    use std::num::NonZeroUsize;
    use tokio::sync::mpsc;

    // 测试 ZeroCopyHandle 的创建
    // Test ZeroCopyHandle creation
    #[test]
    fn test_zero_copy_handle_new() {
        let (request_tx, _request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);
        // 主要检查 request_tx 是否被正确存储，但不能直接访问私有字段
        // Mainly check if request_tx is stored correctly, but cannot directly access private fields
        // 我们可以通过克隆来间接验证
        // We can indirectly verify through cloning
        let _cloned_handle = handle.clone();
        // 没有太多可断言的，除非我们想测试 Debug impl
        // Not much to assert, unless we want to test the Debug impl
        assert!(format!("{:?}", handle).contains("ZeroCopyHandle"));
    }

    // 测试 reserve_writer 方法（模拟 Manager 回复成功）
    #[tokio::test]
    async fn test_reserve_writer_success() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        // 启动一个任务来模拟 Manager 的行为
        tokio::spawn(async move {
            if let Some(Request::Reserve(req)) = request_rx.recv().await {
                let res_id: ReservationId = 1;
                let offset: AbsoluteOffset = 100;
                let group_id: GroupId = 0;
                // 模拟成功回复
                req.reply_tx.send(Ok((res_id, offset, group_id))).unwrap();
            }
        });

        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(submit_agent) => {
                assert_eq!(submit_agent.id, 1);
                assert_eq!(submit_agent.offset, 100);
                assert_eq!(submit_agent.size, 50);
                assert!(!submit_agent.consumed);
            }
            Err(e) => panic!("reserve_writer 应该成功, 但返回错误: {:?}", e),
        }
    }

    // 测试 reserve_writer 方法（模拟 Manager 回复失败）
    #[tokio::test]
    async fn test_reserve_writer_manager_error() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::Reserve(req)) = request_rx.recv().await {
                // 模拟 Manager 内部错误
                req.reply_tx
                    .send(Err(ManagerError::Internal("测试错误".to_string())))
                    .unwrap();
            }
        });

        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(_) => panic!("reserve_writer 应该失败"),
            Err(BufferError::ManagerError(ManagerError::Internal(msg))) => {
                assert_eq!(msg, "测试错误");
            }
            Err(e) => panic!("期望 ManagerError::Internal, 但得到 {:?}", e),
        }
    }

    // 测试 reserve_writer 方法 (模拟请求通道关闭)
    #[tokio::test]
    async fn test_reserve_writer_send_error() {
        let (request_tx, request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);
        drop(request_rx); // 关闭接收端，使发送失败

        let size = NonZeroUsize::new(50).unwrap();
        match handle.reserve_writer(size).await {
            Ok(_) => panic!("reserve_writer 应该失败"),
            Err(BufferError::SendRequestError(_)) => {
                // 成功捕获发送错误
            }
            Err(e) => panic!("期望 SendRequestError, 但得到 {:?}", e),
        }
    }

    // 测试 finalize 方法 (模拟 Manager 回复)
    #[tokio::test]
    async fn test_finalize_success() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx); // handle 被消耗

        tokio::spawn(async move {
            if let Some(Request::Finalize { reply_tx }) = request_rx.recv().await {
                let finalize_result = FinalizeResult::default();
                reply_tx.send(Some(finalize_result)).unwrap();
            }
        });

        match handle.finalize().await {
            Ok(report) => {
                assert!(report.is_empty());
            }
            Err(e) => panic!("finalize 应该成功, 但返回错误: {:?}", e),
        }
    }

    // 测试 finalize 方法 (模拟 Manager 回复 None)
    #[tokio::test]
    async fn test_finalize_success_none_report() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::Finalize { reply_tx }) = request_rx.recv().await {
                reply_tx.send(None).unwrap(); // Manager 可能返回 None
            }
        });

        match handle.finalize().await {
            Ok(report) => {
                // 当 Manager 返回 None 时，handle.finalize 会将其转换为 Default::default()
                assert!(report.is_empty(), "报告应该为空，因为 Manager 返回了 None");
            }
            Err(e) => panic!("finalize 应该成功（即使报告为 None）, 但返回错误: {:?}", e),
        }
    }

    // 测试内部方法 submit_single_bytes_internal (模拟成功)
    #[tokio::test]
    async fn test_submit_single_bytes_internal_success() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = request_rx.recv().await {
                // 验证请求内容
                assert_eq!(req.reservation_id, 1);
                assert_eq!(req.absolute_offset, 100);
                matches!(req.data, CommitType::Single(_));
                req.reply_tx.send(Ok(())).unwrap();
            }
        });

        let result = handle
            .submit_single_bytes_internal(
                SubmitParams {
                    res_id: 1,
                    group_id: 0,
                    offset: 100,
                },
                Bytes::from_static(b"test"),
            )
            .await;
        assert!(result.is_ok());
    }

    // 测试内部方法 submit_chunked_bytes_internal (模拟失败)
    #[tokio::test]
    async fn test_submit_chunked_bytes_internal_error() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(request_tx);

        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = request_rx.recv().await {
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
        match result {
            Err(BufferError::ManagerError(ManagerError::AlreadyCommitted(res_id))) => {
                assert_eq!(res_id, 2);
            }
            _ => panic!("期望 AlreadyCommitted 错误, 得到 {:?}", result),
        }
    }

    // 测试 send_failed_info 方法
    // 这个方法使用 try_send，所以我们需要检查通道中是否收到了消息
    #[tokio::test]
    async fn test_send_failed_info() {
        let (request_tx, mut request_rx) = mpsc::channel::<Request>(10); // 足够大的缓冲
        let handle = ZeroCopyHandle::new(request_tx);

        let failed_info = FailedReservationInfo {
            id: 123,
            group_id: 0,
            offset: 456,
            size: 789,
        };

        handle.send_failed_info(failed_info.clone());

        // 尝试从通道接收消息
        match tokio::time::timeout(std::time::Duration::from_millis(100), request_rx.recv()).await {
            Ok(Some(Request::FailedInfo(req))) => {
                assert_eq!(req.info, failed_info);
            }
            Ok(Some(_)) => panic!("收到了非 FailedInfo 类型的请求"),
            Ok(None) => panic!("通道意外关闭"),
            Err(_) => panic!("在规定时间内未收到 FailedInfo 请求 (try_send 可能失败或未发送)"),
        }
    }

    // 测试 send_failed_info 在通道满或关闭时的情况
    #[tokio::test]
    async fn test_send_failed_info_channel_full_or_closed() {
        // 1. 通道满的情况 (缓冲区为1，先发送一个阻塞)
        //    注意：try_send 的行为是如果通道满则立即返回错误，不会阻塞。
        //    这里我们用一个容量为0的通道来模拟（实际上mpsc不允许0，最小为1且会立即满如果发送端先发）。
        //    或者一个容量为1的，然后先填满它。
        //    由于try_send的非阻塞特性，我们主要关注它是否如预期般不panic。
        //    实际的错误处理（如日志）是在函数内部，我们无法直接断言日志输出。
        //    所以这个测试主要是确保调用 try_send 不会因通道问题而panic。

        let (request_tx_full, mut request_rx_full) = mpsc::channel::<Request>(1);
        let handle_full = ZeroCopyHandle::new(request_tx_full);

        // 先填满通道
        let (dummy_reply_tx, _) = oneshot::channel();
        handle_full
            .request_tx
            .send(Request::Finalize {
                reply_tx: dummy_reply_tx,
            })
            .await
            .unwrap();

        let failed_info_full = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 1,
            size: 1,
        };
        handle_full.send_failed_info(failed_info_full.clone()); // 应该会 try_send 失败并记录警告

        // 尝试接收第一个消息，确认通道确实是满的
        assert!(request_rx_full.recv().await.is_some());
        // 此时通道空了，如果再try_send会成功。
        // 这个测试并不能很好地验证“满”的情况，因为try_send之后我们无法知道内部是否打印了警告。
        // 更好的方法是如果 send_failed_info 返回 Result，但它目前不返回。

        // 2. 通道关闭的情况
        let (request_tx_closed, request_rx_closed) = mpsc::channel::<Request>(1);
        let handle_closed = ZeroCopyHandle::new(request_tx_closed);
        drop(request_rx_closed); // 关闭接收端

        let failed_info_closed = FailedReservationInfo {
            id: 2,
            group_id: 0,
            offset: 2,
            size: 2,
        };
        handle_closed.send_failed_info(failed_info_closed); // 应该会 try_send 失败并记录警告
                                                            // 同样，我们无法直接断言警告。这个测试仅确保调用不会 panic。
    }
}
