//! 定义核心数据结构和类型别名
//! 包含了与 Manager、Agent 和 Handle 交互所需的基础类型。

pub(crate) use crate::error::ManagerError;
// 引入 Manager 内部错误类型
use bytes::Bytes;
// 引入 Bytes 类型，用于高效处理二进制数据
use std::{
    collections::BTreeMap, // 引入有序 Map, HashMap 和 HashSet
    num::NonZeroUsize,     // 引入非零 usize，确保预留大小不为 0
};
use tokio::sync::oneshot;
// 引入 oneshot 通道，用于请求-回复模式

// --- 基本类型别名 ---

/// 预留的唯一标识符类型别名 (通常是递增的 u64)
pub type ReservationId = u64;
/// 缓冲区中的绝对偏移量类型别名 (从 0 开始计数)
pub type AbsoluteOffset = usize;
/// 分组的唯一标识符类型别名 (通常是递增的 u64)
pub type GroupId = u64;

// --- 通道和回调类型 ---

/// 用于 `submit_bytes` 请求的回复通道发送端类型别名。
/// 发送的是一个 `Result`，表示提交操作成功 (`Ok(())`) 或失败 (`Err(ManagerError)`)。
pub type SubmitReplyTx = oneshot::Sender<Result<(), ManagerError>>;

/// 用于 `reserve` 请求的回复通道发送端类型别名。
/// 成功时发送 `Ok((ReservationId, AbsoluteOffset, GroupId))`。
/// 失败时发送 `Err(ManagerError)`。
pub type ReserveReplyTx =
    oneshot::Sender<Result<(ReservationId, AbsoluteOffset, GroupId), ManagerError>>;

/// 用于 `finalize` 请求的回复通道发送端类型别名。
/// 发送的是 `Option<FinalizeResult>`。
pub type FinalizeReplyTx = oneshot::Sender<Option<FinalizeResult>>;

// --- 数据结构 ---

/// 代表一个失败或未完成的预留信息。
/// 主要用于 `FailedGroupData` 和 Agent Drop 时通知 Manager。
#[derive(Debug, Clone, PartialEq)] // 允许 Debug、Clone 和 PartialEq (方便测试)
pub struct FailedReservationInfo {
    /// 失败的预留 ID
    pub id: ReservationId,
    /// 该预留所属的分组 ID
    pub group_id: GroupId,
    /// 该预留的起始绝对偏移量
    pub offset: AbsoluteOffset,
    /// 该预留的大小（字节）
    pub size: usize,
}

/// 代表一个处理失败的分组及其相关信息。
/// 由 Manager 在 finalize 时收集，并通过 `failed_data_tx` 通道发送给失败数据消费者。
#[derive(Debug, Clone)] // 允许 Debug 和 Clone (发送通道或报告时可能需要)
pub struct FailedGroupData {
    /// 分组的唯一 ID
    pub group_id: GroupId,
    /// 该分组包含的所有 **已提交** 数据块，按绝对偏移量排序。
    /// Key: 块的起始绝对偏移量。
    /// Value: 数据块本身 (`Bytes`)。
    pub group_chunks: BTreeMap<AbsoluteOffset, Bytes>,
    /// 该分组内所有记录到的失败或未完成的预留信息。
    pub failed_reservations: Vec<FailedReservationInfo>,
}

/// `finalize` 操作完成后返回的结果。
/// 包含了所有处理失败 **且** 其数据未能通过 `failed_data_tx` 成功发送的分组信息。
/// 注意：成功的分组数据 **不会** 包含在此结果中，它们通过 `completed_data_tx` 发送。
#[derive(Debug, Default)] // Default 用于在 finalize 失败或无报告时返回空结果
pub struct FinalizeResult {
    /// 包含所有未能通过失败通道发送的 `FailedGroupData`。
    pub failed: Vec<FailedGroupData>,
}

impl FinalizeResult {
    /// 返回报告中包含的失败分组数量。
    pub fn failed_len(&self) -> usize {
        self.failed.len()
    }

    /// 检查报告是否为空（没有需要报告的失败分组）。
    pub fn is_empty(&self) -> bool {
        self.failed.is_empty()
    }
}

/// 发送给 **成功数据消费者** 的、已成功合并的分组数据类型别名。
/// 元组包含：
///   - `AbsoluteOffset`: 分组数据在逻辑缓冲区中的起始绝对偏移量。
///   - `Box<[u8]>`: 包含合并后数据的 Boxed 字节切片，所有权转移给消费者。
pub type SuccessfulGroupData = (AbsoluteOffset, Box<[u8]>);

/// 发送给 **失败数据消费者** 的数据类型别名 (使用 `FailedGroupData` 结构体)。
pub type FailedGroupDataTransmission = FailedGroupData;

/// 提交数据的类型，用于 `SubmitBytesRequest`。
#[derive(Debug, Clone)] // 需要 Clone，因为 Agent 可能需要在 Drop 前发送
pub enum CommitType {
    /// 分块提交的数据列表。Manager 需要按顺序合并它们。
    Chunked(Vec<Bytes>),
    /// 单次提交的完整数据。
    Single(Bytes),
}

// --- 请求结构体 (用于 Handle -> Manager 通信) ---

/// 请求预留写入空间的结构体。
#[derive(Debug)]
pub struct ReserveRequest {
    /// 请求预留的大小 (保证非零)。
    pub size: NonZeroUsize,
    /// 回复通道，用于异步接收预留结果。
    pub reply_tx: ReserveReplyTx,
}

/// 请求提交数据块到指定预留位的结构体。
#[derive(Debug)]
pub struct SubmitBytesRequest {
    /// 目标预留的 ID。
    pub reservation_id: ReservationId,
    /// 数据块在缓冲区中的绝对起始偏移量 (对于 `CommitType::Single` 和 `CommitType::Chunked` 都指预留的起始偏移)。
    pub absolute_offset: AbsoluteOffset,
    /// 目标预留所属的分组 ID。
    pub group_id: GroupId,
    /// 要提交的数据 (使用 `CommitType` 区分单次/分块)。
    pub data: CommitType,
    /// 回调通道，用于确认提交成功或失败。
    pub reply_tx: SubmitReplyTx,
}

/// 由 Agent 在 Drop 时发送的失败信息请求。
#[derive(Debug, Clone)] // 需要 Clone
pub struct FailedInfoRequest {
    /// 失败的预留信息。
    pub info: FailedReservationInfo,
}

/// 枚举类型，代表所有可能通过 Handle 发送给 Manager Actor 的请求。
/// Manager 的主事件循环 (`run` 方法) 会匹配这个枚举来分发处理逻辑。
#[derive(Debug)]
pub enum Request {
    /// 预留空间请求。
    Reserve(ReserveRequest),
    /// 提交数据块请求 (包括单次和分块)。
    SubmitBytes(SubmitBytesRequest),
    /// 接收到来自 Agent 的预留失败信息 (Agent Drop)。
    FailedInfo(FailedInfoRequest),
    /// 请求 Manager 执行 Finalize 操作，进行清理和报告失败。
    Finalize {
        /// 回复通道，用于接收 Finalize 操作的结果 (`Option<FinalizeResult>`)。
        reply_tx: FinalizeReplyTx,
    },
}

// --- Manager 内部状态 ---

/// 分块提交请求的参数结构
/// Parameters structure for chunked submission request
#[derive(Debug, Clone)]
pub struct SubmitParams {
    /// 预留ID
    /// Reservation ID
    pub(crate) res_id: ReservationId,

    /// 组ID
    /// Group ID
    pub(crate) group_id: GroupId,

    /// 预留的起始偏移
    /// The starting offset of the reservation
    pub(crate) offset: AbsoluteOffset,
}

impl SubmitParams {
    pub fn into_single_request(self, bytes: Bytes, tx: SubmitReplyTx) -> Request {
        Request::SubmitBytes(SubmitBytesRequest {
            reservation_id: self.res_id,
            absolute_offset: self.offset,
            group_id: self.group_id,
            data: CommitType::Single(bytes),
            reply_tx: tx,
        })
    }

    pub fn into_chunked_request(self, chunks: Vec<Bytes>, tx: SubmitReplyTx) -> Request {
        Request::SubmitBytes(SubmitBytesRequest {
            reservation_id: self.res_id,
            absolute_offset: self.offset,
            group_id: self.group_id,
            data: CommitType::Chunked(chunks),
            reply_tx: tx,
        })
    }
}

// 在 src/types.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*;
    // 导入父模块（即 types.rs）中的所有内容
    use std::num::NonZeroUsize;

    // 测试 FailedReservationInfo 结构体的创建和字段
    #[test]
    fn test_failed_reservation_info_creation() {
        let info = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 100,
            size: 50,
        };
        assert_eq!(info.id, 1);
        assert_eq!(info.group_id, 0);
        assert_eq!(info.offset, 100);
        assert_eq!(info.size, 50);
    }

    // 测试 FailedGroupData 结构体的创建
    #[test]
    fn test_failed_group_data_creation() {
        let chunks = BTreeMap::new();
        let reservations = Vec::new();
        let data = FailedGroupData {
            group_id: 10,
            group_chunks: chunks.clone(),
            failed_reservations: reservations.clone(),
        };
        assert_eq!(data.group_id, 10);
        assert!(data.group_chunks.is_empty());
        assert!(data.failed_reservations.is_empty());
    }

    // 测试 FinalizeResult 的默认值和方法
    #[test]
    fn test_finalize_result_default_and_methods() {
        let default_result = FinalizeResult::default();
        assert!(default_result.is_empty()); // 默认情况下应该是空的
        assert_eq!(default_result.failed_len(), 0); // 失败列表长度为0

        let result_with_failure = FinalizeResult {
            failed: vec![FailedGroupData {
                group_id: 1,
                group_chunks: BTreeMap::new(),
                failed_reservations: Vec::new(),
            }],
        };
        assert!(!result_with_failure.is_empty());
        assert_eq!(result_with_failure.failed_len(), 1);
    }

    // 测试 CommitType 枚举的变体
    #[test]
    fn test_commit_type_variants() {
        let single_data = Bytes::from_static(b"hello");
        let commit_single = CommitType::Single(single_data.clone());
        if let CommitType::Single(s) = commit_single {
            assert_eq!(s, single_data);
        } else {
            panic!("Expected CommitType::Single");
        }

        let chunk_data = vec![Bytes::from_static(b"world")];
        let commit_chunked = CommitType::Chunked(chunk_data.clone());
        if let CommitType::Chunked(c) = commit_chunked {
            assert_eq!(c, chunk_data);
        } else {
            panic!("Expected CommitType::Chunked");
        }
    }

    // 测试 ReserveRequest 结构体的创建
    #[test]
    fn test_reserve_request_creation() {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let size = NonZeroUsize::new(100).unwrap();
        let req = ReserveRequest { size, reply_tx };
        assert_eq!(req.size.get(), 100);
    }

    // 测试 SubmitBytesRequest 结构体的创建
    #[test]
    fn test_submit_bytes_request_creation() {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let data = CommitType::Single(Bytes::new());
        let req = SubmitBytesRequest {
            reservation_id: 1,
            absolute_offset: 0,
            group_id: 0,
            data: data.clone(),
            reply_tx,
        };
        assert_eq!(req.reservation_id, 1);
        assert_eq!(req.absolute_offset, 0);
        assert_eq!(req.group_id, 0);
        matches!(req.data, CommitType::Single(_));
    }

    // 测试 FailedInfoRequest 结构体的创建
    #[test]
    fn test_failed_info_request_creation() {
        let failed_res_info = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 0,
            size: 0,
        };
        let req = FailedInfoRequest {
            info: failed_res_info.clone(),
        };
        assert_eq!(req.info, failed_res_info);
    }

    // 测试 Request 枚举的变体
    #[test]
    fn test_request_enum_variants() {
        let (reserve_reply_tx, _) = oneshot::channel();
        let reserve_req_struct = ReserveRequest {
            size: NonZeroUsize::new(1).unwrap(),
            reply_tx: reserve_reply_tx,
        };
        let req_reserve = Request::Reserve(reserve_req_struct);
        matches!(req_reserve, Request::Reserve(_));

        let (submit_reply_tx, _) = oneshot::channel();
        let submit_req_struct = SubmitBytesRequest {
            reservation_id: 1,
            absolute_offset: 0,
            group_id: 0,
            data: CommitType::Single(Bytes::new()),
            reply_tx: submit_reply_tx,
        };
        let req_submit = Request::SubmitBytes(submit_req_struct);
        matches!(req_submit, Request::SubmitBytes(_));

        let failed_info_req_struct = FailedInfoRequest {
            info: FailedReservationInfo {
                id: 1,
                group_id: 0,
                offset: 0,
                size: 0,
            },
        };
        let req_failed_info = Request::FailedInfo(failed_info_req_struct);
        matches!(req_failed_info, Request::FailedInfo(_));

        let (finalize_reply_tx, _) = oneshot::channel();
        let req_finalize = Request::Finalize {
            reply_tx: finalize_reply_tx,
        };
        matches!(req_finalize, Request::Finalize { .. });
    }
}
