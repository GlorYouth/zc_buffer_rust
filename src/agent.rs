//! 定义提交代理 (`SubmitAgent`, `SingleAgent`, `ChunkAgent`)
//! 这些代理简化了用户与 `Manager` 的交互，特别是数据提交过程。
//! Agent 在 Drop 时会自动通知 Manager 未完成的预留。

use crate::{
    error::{BufferError, ManagerError}, // 引入错误类型
    handle::ZeroCopyHandle,             // 需要 Handle 来与 Manager 通信
    types::{AbsoluteOffset, FailedReservationInfo, GroupId, ReservationId, SubmitParams}, // 引入所需类型
};
use bytes::Bytes;
use tracing::{debug, error, info, trace, warn};

// --- SubmitAgent (初始凭证) ---
/// 代表一个已成功获取的预留写入凭证。
/// 用户需要决定将其转换为 `SingleAgent` (单次提交) 或 `ChunkAgent` (分块提交)。
/// 如果这个 Agent 被 Drop 而未转换或提交，它将通知 Manager 预留失败。
#[derive(Debug)]
pub struct SubmitAgent {
    /// 预留的唯一 ID
    pub(crate) id: ReservationId,
    /// 预留所属的分组 ID
    pub(crate) group_id: GroupId,
    /// 该预留分配到的绝对起始偏移量
    pub(crate) offset: AbsoluteOffset,
    /// 该预留分配到的确定大小 (字节)
    pub(crate) size: usize,
    /// 用于与 Manager 通信的 Handle (内部持有 Sender)
    pub(crate) handle: ZeroCopyHandle, // 使用 Handle 而不是直接用 Sender
    /// 标记此 Agent 是否已被消费 (转换为 Single/Chunk 或 Drop)
    pub(crate) consumed: bool,
}

impl SubmitAgent {
    /// 获取预留 ID
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// 获取预留的起始偏移量
    pub fn offset(&self) -> AbsoluteOffset {
        self.offset
    }

    /// 获取预留的大小
    pub fn size(&self) -> usize {
        self.size
    }

    /// 将此凭证转换为用于单次提交的 `SingleAgent`。
    /// 转换后，原 `SubmitAgent` 将失效，不再触发 Drop 时通知。
    pub fn into_single_agent(mut self) -> SingleAgent {
        self.consumed = true; // 标记为已消费
        SingleAgent {
            id: self.id,
            group_id: self.group_id,
            offset: self.offset,
            size: self.size,
            handle: self.handle.clone(), // 克隆 Handle
            committed: false,            // 初始未提交
        }
    }

    /// 将此凭证转换为用于分块提交的 `ChunkAgent`。
    /// 转换后，原 `SubmitAgent` 将失效，不再触发 Drop 时通知。
    pub fn into_chunk_agent(mut self) -> ChunkAgent {
        self.consumed = true; // 标记为已消费
        ChunkAgent {
            id: self.id,
            group_id: self.group_id,
            offset: self.offset,
            size: self.size,
            handle: self.handle.clone(), // 克隆 Handle
            chunks: Vec::new(),          // 初始化块列表
            current_size: 0,             // 初始已提交大小为 0
            committed: false,            // 初始未提交
        }
    }
}

// SubmitAgent 的 Drop 实现：如果未被消费，则通知 Manager 失败
impl Drop for SubmitAgent {
    fn drop(&mut self) {
        // 只有当 Agent 未被转换为 Single/Chunk 时才发送失败信息
        if !self.consumed {
            warn!(
                "(Agent {}) Drop: SubmitAgent 未被使用，通知 Manager 失败 (ID: {}, Offset: {}, Size: {})",
                self.id, self.id, self.offset, self.size
            );
            // 使用 Handle 发送失败信息
            self.handle.send_failed_info(FailedReservationInfo {
                id: self.id,
                group_id: self.group_id,
                offset: self.offset,
                size: self.size,
            });
        }
    }
}

// --- SingleAgent (单次提交代理) ---
/// 用于执行单次完整数据提交的代理。
/// 用户调用 `submit_bytes` 方法提交数据。
/// 如果 Agent 在 `submit_bytes` 成功前被 Drop，它将通知 Manager 预留失败。
#[derive(Debug)]
pub struct SingleAgent {
    id: ReservationId,
    group_id: GroupId,
    offset: AbsoluteOffset,
    size: usize,
    handle: ZeroCopyHandle,
    /// 标记数据是否已成功提交给 Manager
    committed: bool,
}

impl SingleAgent {
    /// 获取预留 ID
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// 获取预留的起始偏移量
    pub fn offset(&self) -> AbsoluteOffset {
        self.offset
    }

    /// 获取预留的大小
    pub fn size(&self) -> usize {
        self.size
    }

    /// 提交完整的预留数据。
    /// 此方法会进行大小校验，然后将数据发送给 Manager。
    /// 成功提交后，Agent 的 Drop 不再发送失败通知。
    pub async fn submit_bytes(mut self, bytes: Bytes) -> Result<(), BufferError> {
        // 防止重复提交
        if self.committed {
            error!("(Agent {}) 尝试重复提交 (SingleAgent)", self.id);
            return Err(ManagerError::AlreadyCommitted(self.id).into());
        }

        // 校验提交的数据大小是否与预留大小完全一致
        let actual_size = bytes.len();
        if actual_size != self.size {
            error!(
                "(Agent {}) 提交大小错误 (SingleAgent): 期望 {}, 实际 {}",
                self.id, self.size, actual_size
            );
            // 注意：这里返回错误后，Agent 会被 Drop，触发 FailedInfo
            return Err(ManagerError::SubmitSizeIncorrect {
                reservation_id: self.id,
                expected: self.size,
                actual: actual_size,
            }
            .into());
        }

        debug!(
            "(Agent {}) 准备提交单块数据 (Offset: {}, Size: {})",
            self.id, self.offset, self.size
        );

        // 调用 Handle 的辅助方法将数据发送给 Manager
        match self
            .handle
            .submit_single_bytes_internal(
                SubmitParams {
                    res_id: self.id,
                    group_id: self.group_id,
                    offset: self.offset,
                },
                bytes,
            )
            .await
        {
            Ok(_) => {
                info!("(Agent {}) 单块数据提交成功", self.id);
                self.committed = true; // 标记为已提交
                Ok(())
            }
            Err(e) => {
                error!("(Agent {}) 单块数据提交失败: {:?}", self.id, e);
                // 提交失败，Agent 会 Drop 并发送 FailedInfo (除非错误是 ManagerFinalizing)
                Err(e)
            }
        }
    }
}

// SingleAgent 的 Drop 实现：如果未成功提交，则通知 Manager 失败
impl Drop for SingleAgent {
    fn drop(&mut self) {
        if !self.committed {
            warn!(
                "(Agent {}) Drop: SingleAgent 未成功提交，通知 Manager 失败 (ID: {}, Offset: {}, Size: {})",
                self.id, self.id, self.offset, self.size
            );
            self.handle.send_failed_info(FailedReservationInfo {
                id: self.id,
                group_id: self.group_id,
                offset: self.offset,
                size: self.size,
            });
        }
    }
}

// --- ChunkAgent (分块提交代理) ---
/// 用于执行分块数据提交的代理。
/// 用户多次调用 `submit_chunk` 添加数据块，最后调用 `commit` 将所有块发送给 Manager。
/// 如果 Agent 在 `commit` 成功前被 Drop，它将通知 Manager 预留失败。
#[derive(Debug)]
pub struct ChunkAgent {
    id: ReservationId,
    group_id: GroupId,
    offset: AbsoluteOffset, // 预留的起始偏移
    size: usize,            // 预留的总大小
    handle: ZeroCopyHandle,
    /// 存储已添加的数据块
    chunks: Vec<Bytes>,
    /// 当前已添加块的总大小
    current_size: usize,
    /// 标记是否已成功调用 commit
    committed: bool,
}

impl ChunkAgent {
    /// 获取预留 ID
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// 获取预留的起始偏移量
    pub fn offset(&self) -> AbsoluteOffset {
        self.offset
    }

    /// 获取预留的总大小
    pub fn size(&self) -> usize {
        self.size
    }

    /// 获取当前已添加块的总大小
    pub fn current_size(&self) -> usize {
        self.current_size
    }

    /// 添加一个数据块到 Agent 内部缓存。
    /// 此方法会检查添加块后是否会超过预留的总大小。
    /// 注意：此方法是同步的，不与 Manager 交互。
    pub fn submit_chunk(&mut self, chunk: Bytes) -> Result<(), ManagerError> {
        // 防止在已调用 commit 后再添加块
        if self.committed {
            error!("(Agent {}) 尝试在 Commit 后添加块 (ChunkAgent)", self.id);
            return Err(ManagerError::AlreadyCommitted(self.id));
        }

        let chunk_len = chunk.len();
        // 检查添加此块是否会超出预留总大小
        if self.current_size + chunk_len > self.size {
            error!(
                "(Agent {}) 添加块导致超出预留大小 (ChunkAgent): 当前 {}, 添加 {}, 总共 {}, 预留 {}",
                self.id, self.current_size, chunk_len, self.current_size + chunk_len, self.size
            );
            return Err(ManagerError::SubmitSizeTooLarge {
                reservation_id: self.id,
                largest: self.size - self.current_size, // 剩余可用大小
                actual: chunk_len,
            });
        }

        // 添加块到列表并更新当前大小
        self.current_size += chunk_len;
        self.chunks.push(chunk);
        trace!(
            "(Agent {}) 添加块成功 (ChunkAgent): 大小 {}, 当前总大小 {}",
            self.id,
            chunk_len,
            self.current_size
        );
        Ok(())
    }

    /// 将所有已添加的块作为一个整体提交给 Manager。
    /// 此方法会检查当前已添加的总大小是否等于预留大小。
    /// 成功提交后，Agent 的 Drop 不再发送失败通知。
    /// 注意：调用 commit 会消耗 Agent。
    pub async fn commit(mut self) -> Result<(), BufferError> {
        // 防止重复提交
        if self.committed {
            error!("(Agent {}) 尝试重复 Commit (ChunkAgent)", self.id);
            return Err(ManagerError::AlreadyCommitted(self.id).into());
        }

        // 检查最终提交的总大小是否等于预留大小
        if self.current_size != self.size {
            error!(
                "(Agent {}) Commit 时总大小不匹配 (ChunkAgent): 期望 {}, 实际 {}",
                self.id, self.size, self.current_size
            );
            // 大小不匹配，Agent 会被 Drop 并发送 FailedInfo
            return Err(ManagerError::CommitSizeMismatch {
                reservation_id: self.id,
                expected: self.size,
                actual: self.current_size,
            }
            .into());
        }

        debug!(
            "(Agent {}) 准备 Commit 分块数据 (Offset: {}, Total Size: {}, Chunks: {})",
            self.id,
            self.offset,
            self.current_size,
            self.chunks.len()
        );

        // 调用 Handle 的辅助方法将所有块发送给 Manager
        // 需要 take chunks 的所有权
        let chunks_to_send = std::mem::take(&mut self.chunks);
        match self
            .handle
            .submit_chunked_bytes_internal(
                SubmitParams {
                    res_id: self.id,
                    group_id: self.group_id,
                    offset: self.offset,
                },
                chunks_to_send,
            )
            .await
        {
            Ok(_) => {
                info!("(Agent {}) 分块数据 Commit 成功", self.id);
                self.committed = true;
                Ok(())
            }
            Err(e) => {
                error!("(Agent {}) 分块数据 Commit 失败: {:?}", self.id, e);
                // Commit 失败，Agent 会 Drop 并发送 FailedInfo
                Err(e)
            }
        }
    }
}

// ChunkAgent 的 Drop 实现：如果未成功 Commit，则通知 Manager 失败
impl Drop for ChunkAgent {
    fn drop(&mut self) {
        if !self.committed {
            warn!(
                 "(Agent {}) Drop: ChunkAgent 未成功 Commit (或未调用 Commit)，通知 Manager 失败 (ID: {}, Offset: {}, Size: {})",
                 self.id, self.id, self.offset, self.size
             );
            self.handle.send_failed_info(FailedReservationInfo {
                id: self.id,
                group_id: self.group_id,
                offset: self.offset,
                size: self.size, // 发送的是预留的总大小
            });
        }
    }
}

// 在 src/agent.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CommitType;
    // 导入父模块中的所有内容
    use crate::{
        handle::ZeroCopyHandle,
        types::{AbsoluteOffset, Request, ReservationId},
        BufferError, ManagerError,
    };
    use bytes::Bytes;
    use tokio::sync::mpsc;

    // 辅助函数：创建一个测试用的 ZeroCopyHandle 和一个请求接收器
    fn setup_handle_and_receiver() -> (ZeroCopyHandle, mpsc::Receiver<Request>) {
        let (request_tx, request_rx) = mpsc::channel::<Request>(10);
        let handle = ZeroCopyHandle::new(request_tx);
        (handle, request_rx)
    }

    // --- SubmitAgent Tests ---
    #[test]
    fn test_submit_agent_creation_and_accessors() {
        let (handle, _rx) = setup_handle_and_receiver();
        let agent = SubmitAgent {
            id: 1,
            group_id: 0,
            offset: 100,
            size: 50,
            handle,
            consumed: false,
        };

        assert_eq!(agent.id(), 1);
        assert_eq!(agent.offset(), 100);
        assert_eq!(agent.size(), 50);
        assert!(!agent.consumed);
    }

    #[test]
    fn test_submit_agent_into_single_agent() {
        let (handle, _rx) = setup_handle_and_receiver();
        let submit_agent = SubmitAgent {
            id: 1,
            group_id: 0,
            offset: 100,
            size: 50,
            handle: handle.clone(),
            consumed: false,
        };

        let single_agent = submit_agent.into_single_agent();
        assert_eq!(single_agent.id, 1);
        assert_eq!(single_agent.offset, 100);
        assert_eq!(single_agent.size, 50);
        assert_eq!(single_agent.group_id, 0);
        assert!(!single_agent.committed);
        // submit_agent 应该已经被消耗，其 consumed 字段理论上应为 true, 但我们无法直接访问原 submit_agent
    }

    #[test]
    fn test_submit_agent_into_chunk_agent() {
        let (handle, _rx) = setup_handle_and_receiver();
        let submit_agent = SubmitAgent {
            id: 1,
            group_id: 0,
            offset: 100,
            size: 50,
            handle: handle.clone(),
            consumed: false,
        };

        let chunk_agent = submit_agent.into_chunk_agent();
        assert_eq!(chunk_agent.id, 1);
        assert_eq!(chunk_agent.offset, 100);
        assert_eq!(chunk_agent.size, 50);
        assert_eq!(chunk_agent.group_id, 0);
        assert!(chunk_agent.chunks.is_empty());
        assert_eq!(chunk_agent.current_size, 0);
        assert!(!chunk_agent.committed);
    }

    #[tokio::test]
    async fn test_submit_agent_drop_sends_failed_info() {
        let (handle, mut rx) = setup_handle_and_receiver();
        let agent_id: ReservationId = 10;
        let agent_offset: AbsoluteOffset = 200;
        let agent_size: usize = 30;

        {
            let _submit_agent = SubmitAgent {
                id: agent_id,
                group_id: 1,
                offset: agent_offset,
                size: agent_size,
                handle: handle.clone(),
                consumed: false, // 关键：未被消费
            };
            // _submit_agent 在这里 drop
        }

        // 检查是否发送了 FailedInfoRequest
        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(Request::FailedInfo(req))) => {
                assert_eq!(req.info.id, agent_id);
                assert_eq!(req.info.offset, agent_offset);
                assert_eq!(req.info.size, agent_size);
            }
            _ => panic!("SubmitAgent drop 时未发送 FailedInfo 或发送了错误类型"),
        }
    }

    #[tokio::test]
    async fn test_submit_agent_drop_consumed_does_not_send_failed_info() {
        let (handle, mut rx) = setup_handle_and_receiver();
        {
            let mut submit_agent = SubmitAgent {
                id: 10,
                group_id: 1,
                offset: 200,
                size: 30,
                handle: handle.clone(),
                consumed: false,
            };
            submit_agent.consumed = true; // 标记为已消费
                                          // submit_agent 在这里 drop
        }

        // 检查是否没有发送 FailedInfoRequest
        match tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await {
            Ok(Some(_)) => panic!("不应发送任何信息当 SubmitAgent 已被消费"),
            Ok(None) => {} // 通道关闭，正常
            Err(_) => {}   // 超时，表示没有消息发送，符合预期
        }
    }

    // --- SingleAgent Tests ---
    #[tokio::test]
    async fn test_single_agent_submit_bytes_success() {
        let (mock_request_tx, mut mock_request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(mock_request_tx);
        let agent_id: ReservationId = 2;
        let agent_size: usize = 10;

        // 模拟 Manager 回复成功
        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = mock_request_rx.recv().await {
                assert_eq!(req.reservation_id, agent_id);
                assert_eq!(
                    req.data.clone().get_single_bytes().unwrap().len(),
                    agent_size
                );
                req.reply_tx.send(Ok(())).unwrap();
            }
        });

        let single_agent = SingleAgent {
            id: agent_id,
            group_id: 0,
            offset: 100,
            size: agent_size,
            handle,
            committed: false,
        };

        let data = Bytes::from(vec![0u8; agent_size]);
        let result = single_agent.submit_bytes(data).await;
        assert!(result.is_ok());
        // single_agent 在 submit_bytes 成功后，其内部 committed 标志应为 true
        // 但由于 submit_bytes 消耗了 self，我们无法直接检查原 agent
        // 但如果再次调用（虽然API设计为消耗），或者在Drop时不发送FailedInfo，则可以推断。
    }

    #[tokio::test]
    async fn test_single_agent_submit_bytes_size_mismatch_at_agent() {
        let (handle, _rx) = setup_handle_and_receiver(); // Manager 不会被调用
        let agent = SingleAgent {
            id: 2,
            group_id: 0,
            offset: 100,
            size: 10, // 期望大小 10
            handle,
            committed: false,
        };

        let data = Bytes::from(vec![0u8; 5]); // 实际大小 5
        let result = agent.submit_bytes(data).await;
        match result {
            Err(BufferError::ManagerError(ManagerError::SubmitSizeIncorrect {
                reservation_id,
                expected,
                actual,
            })) => {
                assert_eq!(reservation_id, 2);
                assert_eq!(expected, 10);
                assert_eq!(actual, 5);
            }
            _ => panic!("期望 SubmitSizeIncorrect 错误, 得到 {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_single_agent_submit_bytes_manager_error() {
        let (mock_request_tx, mut mock_request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(mock_request_tx);
        let agent_id: ReservationId = 3;

        // 模拟 Manager 回复失败
        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = mock_request_rx.recv().await {
                req.reply_tx
                    .send(Err(ManagerError::ReservationNotFound(agent_id)))
                    .unwrap();
            }
        });

        let single_agent = SingleAgent {
            id: agent_id,
            group_id: 0,
            offset: 100,
            size: 10,
            handle,
            committed: false,
        };
        let data = Bytes::from(vec![0u8; 10]);
        let result = single_agent.submit_bytes(data).await;
        match result {
            Err(BufferError::ManagerError(ManagerError::ReservationNotFound(id))) => {
                assert_eq!(id, agent_id);
            }
            _ => panic!("期望 ReservationNotFound 错误, 得到 {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_single_agent_drop_not_committed_sends_failed_info() {
        let (handle, mut rx) = setup_handle_and_receiver();
        let agent_id: ReservationId = 20;
        let agent_offset: AbsoluteOffset = 220;
        let agent_size: usize = 32;
        {
            let _single_agent = SingleAgent {
                id: agent_id,
                group_id: 1,
                offset: agent_offset,
                size: agent_size,
                handle: handle.clone(),
                committed: false, // 关键：未提交
            };
            // _single_agent 在这里 drop
        }

        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(Request::FailedInfo(req))) => {
                assert_eq!(req.info.id, agent_id);
                assert_eq!(req.info.offset, agent_offset);
                assert_eq!(req.info.size, agent_size);
            }
            _ => panic!("SingleAgent (未提交) drop 时未发送 FailedInfo"),
        }
    }

    #[tokio::test]
    async fn test_single_agent_drop_committed_does_not_send_failed_info() {
        let (handle, mut rx) = setup_handle_and_receiver();
        {
            let mut single_agent = SingleAgent {
                // mut以便修改committed
                id: 20,
                group_id: 1,
                offset: 220,
                size: 32,
                handle: handle.clone(),
                committed: false,
            };
            single_agent.committed = true; // 标记为已提交
        }

        match tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await {
            Ok(Some(_)) => panic!("不应发送任何信息当 SingleAgent 已提交"),
            Ok(None) => {}
            Err(_) => {} // 超时，符合预期
        }
    }

    // --- ChunkAgent Tests ---
    #[test]
    fn test_chunk_agent_submit_chunk_success_and_current_size() {
        let (handle, _rx) = setup_handle_and_receiver();
        let mut agent = ChunkAgent {
            id: 3,
            group_id: 0,
            offset: 200,
            size: 100, // 总预留大小
            handle,
            chunks: Vec::new(),
            current_size: 0,
            committed: false,
        };

        assert_eq!(agent.current_size(), 0);
        let chunk1 = Bytes::from(vec![0u8; 30]);
        assert!(agent.submit_chunk(chunk1).is_ok());
        assert_eq!(agent.current_size(), 30);
        assert_eq!(agent.chunks.len(), 1);

        let chunk2 = Bytes::from(vec![0u8; 40]);
        assert!(agent.submit_chunk(chunk2).is_ok());
        assert_eq!(agent.current_size(), 70);
        assert_eq!(agent.chunks.len(), 2);
    }

    #[test]
    fn test_chunk_agent_submit_chunk_exceeds_size() {
        let (handle, _rx) = setup_handle_and_receiver();
        let mut agent = ChunkAgent {
            id: 3,
            group_id: 0,
            offset: 200,
            size: 50, // 总预留大小
            handle,
            chunks: Vec::new(),
            current_size: 0,
            committed: false,
        };

        let chunk1 = Bytes::from(vec![0u8; 30]);
        agent.submit_chunk(chunk1).unwrap();

        let chunk2 = Bytes::from(vec![0u8; 25]); // 30 + 25 = 55 > 50
        let result = agent.submit_chunk(chunk2);
        match result {
            Err(ManagerError::SubmitSizeTooLarge {
                reservation_id,
                largest,
                actual,
            }) => {
                assert_eq!(reservation_id, 3);
                assert_eq!(largest, 20); // 剩余 50 - 30 = 20
                assert_eq!(actual, 25);
            }
            _ => panic!("期望 SubmitSizeTooLarge 错误, 得到 {:?}", result),
        }
        assert_eq!(agent.current_size(), 30); // 大小未改变
    }

    #[tokio::test]
    async fn test_chunk_agent_commit_success() {
        let (mock_request_tx, mut mock_request_rx) = mpsc::channel::<Request>(1);
        let handle = ZeroCopyHandle::new(mock_request_tx);
        let agent_id: ReservationId = 4;
        let agent_size: usize = 50;

        tokio::spawn(async move {
            if let Some(Request::SubmitBytes(req)) = mock_request_rx.recv().await {
                assert_eq!(req.reservation_id, agent_id);
                if let CommitType::Chunked(chunks) = req.data {
                    assert_eq!(chunks.len(), 2);
                    assert_eq!(chunks[0].len(), 20);
                    assert_eq!(chunks[1].len(), 30);
                } else {
                    panic!("期望 CommitType::Chunked");
                }
                req.reply_tx.send(Ok(())).unwrap();
            }
        });

        let mut chunk_agent = ChunkAgent {
            id: agent_id,
            group_id: 0,
            offset: 300,
            size: agent_size,
            handle,
            chunks: Vec::new(),
            current_size: 0,
            committed: false,
        };
        chunk_agent
            .submit_chunk(Bytes::from(vec![0u8; 20]))
            .unwrap();
        chunk_agent
            .submit_chunk(Bytes::from(vec![0u8; 30]))
            .unwrap();

        let result = chunk_agent.commit().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_chunk_agent_commit_size_mismatch_at_agent() {
        let (handle, _rx) = setup_handle_and_receiver(); // Manager 不会被调用
        let mut agent = ChunkAgent {
            id: 4,
            group_id: 0,
            offset: 300,
            size: 50, // 期望总大小 50
            handle,
            chunks: Vec::new(),
            current_size: 0,
            committed: false,
        };
        agent.submit_chunk(Bytes::from(vec![0u8; 20])).unwrap(); // 当前大小 20

        let result = agent.commit().await; // 尝试提交
        match result {
            Err(BufferError::ManagerError(ManagerError::CommitSizeMismatch {
                reservation_id,
                expected,
                actual,
            })) => {
                assert_eq!(reservation_id, 4);
                assert_eq!(expected, 50);
                assert_eq!(actual, 20);
            }
            _ => panic!("期望 CommitSizeMismatch 错误, 得到 {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_chunk_agent_drop_not_committed_sends_failed_info() {
        let (handle, mut rx) = setup_handle_and_receiver();
        let agent_id: ReservationId = 30;
        let agent_offset: AbsoluteOffset = 330;
        let agent_size: usize = 33;
        {
            let _chunk_agent = ChunkAgent {
                id: agent_id,
                group_id: 1,
                offset: agent_offset,
                size: agent_size,
                handle: handle.clone(),
                chunks: Vec::new(),
                current_size: 0,
                committed: false, // 关键：未提交
            };
        }
        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(Request::FailedInfo(req))) => {
                assert_eq!(req.info.id, agent_id);
                assert_eq!(req.info.offset, agent_offset);
                assert_eq!(req.info.size, agent_size); // Drop时发送的是预留的总大小
            }
            _ => panic!("ChunkAgent (未提交) drop 时未发送 FailedInfo"),
        }
    }

    // 辅助方法：从 CommitType 中提取 Bytes (仅用于测试)
    impl CommitType {
        fn get_single_bytes(self) -> Option<Bytes> {
            match self {
                CommitType::Single(b) => Some(b),
                _ => None,
            }
        }
    }
}
