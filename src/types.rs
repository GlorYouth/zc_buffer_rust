//! 定义库的核心数据结构和类型别名。
//!
//! 这个模块包含了在 `ManagerActor`、`Agent` 和 `ZeroCopyHandle` 之间传递信息
//! 以及维护内部状态所需的基础类型定义。

// 引入 Manager 内部可能抛出的错误类型，主要用于请求的回复通道
pub(crate) use crate::error::ManagerError;
// 引入 `bytes::Bytes`，这是一个高效的、用于网络编程和零拷贝场景的字节容器
use bytes::Bytes;
use std::{
    collections::BTreeMap, // 使用 BTreeMap 来存储有序的数据块（按偏移量排序）
    num::NonZeroUsize,     // 用于确保预留大小至少为 1
};
// 引入 Tokio 的 `oneshot` 通道，用于实现异步请求-响应模式
use tokio::sync::oneshot;

// --- 基本类型别名 (Basic Type Aliases) ---

/// 预留空间的唯一标识符。
/// 通常是一个单调递增的 `u64` 值，由 `ReservationAllocator` 组件分配。
pub type ReservationId = u64;

/// 缓冲区中的绝对偏移量。
/// 表示数据在整个逻辑缓冲区中的起始位置，从 0 开始计数。
pub type AbsoluteOffset = usize;

/// 数据分组的唯一标识符。
/// 用于将相关的预留和提交操作归属于同一个逻辑单元。
/// 通常是一个单调递增的 `u64` 值，由 `GroupLifecycleManager` 组件分配。
pub type GroupId = u64;

// --- 通道和回调类型 (Channel and Callback Types) ---

/// 用于 `submit_bytes` 请求的回复通道发送端。
///
/// 当 `ManagerActor` 处理完一个 `SubmitBytesRequest` 后，会通过这个通道
/// 发送一个 `Result<(), ManagerError>` 来通知提交者操作是否成功。
/// `Ok(())` 表示成功，`Err(ManagerError)` 表示在处理过程中发生了错误。
pub type SubmitReplyTx = oneshot::Sender<Result<(), ManagerError>>;

/// 用于 `reserve` 请求的回复通道发送端。
///
/// 当 `ManagerActor` 处理完一个 `ReserveRequest` 后，会通过这个通道
/// 发送一个 `Result<(ReservationId, AbsoluteOffset, GroupId), ManagerError>`。
/// `Ok((res_id, offset, group_id))` 表示预留成功，返回预留 ID、绝对偏移量和所属组 ID。
/// `Err(ManagerError)` 表示预留失败（例如缓冲区空间不足）。
pub type ReserveReplyTx =
    oneshot::Sender<Result<(ReservationId, AbsoluteOffset, GroupId), ManagerError>>;

/// 用于 `finalize` 请求的回复通道发送端。
///
/// 当 `ManagerActor` 完成 `Finalize` 操作后，会通过这个通道发送一个 `Option<FinalizeResult>`。
/// `Some(FinalizeResult)` 包含了未能成功发送到失败通道的失败分组信息。
/// `None` 表示 finalize 操作本身失败或没有需要报告的失败信息。
pub type FinalizeReplyTx = oneshot::Sender<Option<FinalizeResult>>;

// --- 数据结构 (Data Structures) ---

/// 代表一个失败或未完成的预留空间信息。
///
/// 这个结构体用于记录那些被预留但最终没有被成功提交数据的空间。
/// 它会在以下情况被创建和使用：
/// 1. 当一个持有预留信息的 `Agent` (如 `ChunkAgent` 或 `SingleAgent`) 被 `drop` 时，
///    如果该预留尚未提交数据，`Agent` 会将对应的 `FailedReservationInfo` 发送给 `ManagerActor`。
/// 2. 在 `ManagerActor` 执行 `finalize` 操作时，收集到的所有失败预留信息会包含在
///    `FailedGroupData` 结构体中。
#[derive(Debug, Clone, PartialEq, Eq)] // 添加 Eq 以便在测试中更方便地比较
pub struct FailedReservationInfo {
    /// 失败预留的唯一 ID。
    pub id: ReservationId,
    /// 该预留所属的数据分组 ID。
    pub group_id: GroupId,
    /// 该预留空间在逻辑缓冲区中的起始绝对偏移量。
    pub offset: AbsoluteOffset,
    /// 该预留空间的大小（字节数）。
    pub size: usize,
}

/// 代表一个处理失败的数据分组及其相关信息。
///
/// 当 `ManagerActor` 执行 `finalize` 操作时，对于每个处理失败的分组（包含至少一个失败的预留），
/// 会创建一个 `FailedGroupData` 实例。
/// 这些实例随后会尝试通过配置的 `failed_data_tx` MPSC 通道发送给指定的失败数据消费者。
///
/// 这个结构体聚合了一个分组内所有成功提交的数据块以及所有失败的预留信息。
#[derive(Debug, Clone)] // Clone 是必需的，因为可能需要在 finalize 结果中也包含它
pub struct FailedGroupData {
    /// 分组的唯一 ID。
    pub group_id: GroupId,
    /// 该分组包含的所有 **已成功提交** 的数据块。
    /// 使用 `BTreeMap` 确保数据块按其在缓冲区中的绝对偏移量排序。
    /// Key: 数据块的起始绝对偏移量 (`AbsoluteOffset`)。
    /// Value: 数据块内容 (`Bytes`)。
    pub group_chunks: BTreeMap<AbsoluteOffset, Bytes>,
    /// 该分组内所有记录到的失败或未完成的预留信息列表。
    pub failed_reservations: Vec<FailedReservationInfo>,
}

/// `finalize` 操作完成后返回的结果。
///
/// 当调用 `ZeroCopyHandle::finalize` 时，`ManagerActor` 会处理所有当前的分组。
/// 处理完成后，`ManagerActor` 会尝试将成功的分组数据发送到 `completed_data_tx`，
/// 将失败的分组数据 (`FailedGroupData`) 发送到 `failed_data_tx`。
///
/// 这个 `FinalizeResult` 结构体 **仅** 包含那些 **处理失败** 且 **未能成功通过 `failed_data_tx` 发送**
/// 的分组信息 (`FailedGroupData`)。这种情况可能发生在 `failed_data_tx` 通道已满或关闭时。
///
/// **注意：** 成功处理的分组数据 **不会** 包含在此结果中。
#[derive(Debug, Default)]
pub struct FinalizeResult {
    /// 包含所有未能通过失败通道 (`failed_data_tx`) 成功发送的 `FailedGroupData`。
    pub failed: Vec<FailedGroupData>,
}

impl FinalizeResult {
    /// 返回此结果中包含的失败分组数量。
    pub fn failed_len(&self) -> usize {
        self.failed.len()
    }

    /// 检查此结果是否为空（即没有未能发送的失败分组）。
    pub fn is_empty(&self) -> bool {
        self.failed.is_empty()
    }
}

/// 发送给 **成功数据消费者** (`completed_data_tx`) 的数据类型别名。
///
/// 代表一个已成功处理并合并完成的数据分组。
/// 元组包含：
///   - `AbsoluteOffset`: 该分组数据在逻辑缓冲区中的起始绝对偏移量。
///   - `Box<[u8]>`: 包含分组所有合并后数据的 Boxed 字节切片。
///     使用 `Box<[u8]>` 可以将数据的所有权高效地转移给消费者。
pub type SuccessfulGroupData = (AbsoluteOffset, Box<[u8]>);

/// 发送给 **失败数据消费者** (`failed_data_tx`) 的数据类型别名。
///
/// 直接使用 `FailedGroupData` 结构体。
pub type FailedGroupDataTransmission = FailedGroupData;

/// 定义提交数据的类型，用于 `SubmitBytesRequest` 结构体。
///
/// 这个枚举区分了两种主要的提交方式：
/// 1. `Chunked`: 数据被分成多个块 (`Vec<Bytes>`) 进行提交。`ManagerActor` 需要负责
///    将这些块按顺序合并到对应的预留空间中。这通常与 `ChunkAgent` 配合使用。
/// 2. `Single`: 数据作为一个单独的块 (`Bytes`) 进行提交。这通常与 `SingleAgent` 配合使用。
#[derive(Debug, Clone)] // Clone 是必需的，因为 Agent 可能需要在 Drop 前创建并发送请求
pub enum CommitType {
    /// 分块提交的数据。包含一个 `Bytes` 向量，Manager 需要按顺序处理。
    Chunked(Vec<Bytes>),
    /// 单次提交的完整数据。
    Single(Bytes),
}

// --- 请求结构体 (Request Structures for Handle -> Manager Communication) ---
// 这些结构体定义了通过 `ZeroCopyHandle` 发送给 `ManagerActor` 的各种请求消息。

/// 请求预留写入空间的结构体。
/// 由 `ZeroCopyHandle::reserve` 方法创建并发送。
#[derive(Debug)] // 通常不需要 Clone，因为只创建一次并发送
pub struct ReserveRequest {
    /// 请求预留空间的大小（字节数）。
    /// 使用 `NonZeroUsize` 确保请求的大小至少为 1。
    pub size: NonZeroUsize,
    /// 用于接收预留结果的 `oneshot` 回复通道发送端。
    pub reply_tx: ReserveReplyTx,
}

/// 请求将数据块提交到指定预留空间的结构体。
/// 由 `Agent` (如 `ChunkAgent`, `SingleAgent`) 创建并发送。
#[derive(Debug)] // 通常不需要 Clone
pub struct SubmitBytesRequest {
    /// 目标预留空间的唯一 ID。
    pub reservation_id: ReservationId,
    /// 目标预留空间在逻辑缓冲区中的绝对起始偏移量。
    /// **注意:** 对于 `CommitType::Chunked` 和 `CommitType::Single`，
    /// 这个偏移量都指的是整个预留空间的起始偏移量，而不是某个块的偏移量。
    pub absolute_offset: AbsoluteOffset,
    /// 目标预留空间所属的数据分组 ID。
    pub group_id: GroupId,
    /// 要提交的数据，使用 `CommitType` 枚举表示。
    pub data: CommitType,
    /// 用于确认提交操作结果的 `oneshot` 回复通道发送端。
    pub reply_tx: SubmitReplyTx,
}

/// 由 `Agent` 在 `drop` 时发送的预留失败信息请求。
///
/// 当一个持有有效预留（尚未提交数据）的 `Agent` 被丢弃时，
/// 它会构建一个 `FailedInfoRequest` 并发送给 `ManagerActor`，
/// 以通知 Manager 这个预留空间未能被使用。
#[derive(Debug, Clone)] // 需要 Clone，因为 Agent 可能需要在 Drop 时创建
pub struct FailedInfoRequest {
    /// 包含失败预留信息的结构体。
    pub info: FailedReservationInfo,
}

/// 枚举类型，代表所有可能通过 `ZeroCopyHandle` 或内部组件发送给 `ManagerActor` 的请求。
///
/// `ManagerActor` 的主事件循环 (`run` 方法) 会接收 `Request` 类型的消息，
/// 并根据具体的变体（variant）来分发处理逻辑到相应的组件或内部函数。
#[derive(Debug)]
pub enum Request {
    /// 预留空间请求。
    Reserve(ReserveRequest),
    /// 提交数据块请求（包括单次提交和分块提交）。
    SubmitBytes(SubmitBytesRequest),
    /// 接收到来自 `Agent` 的预留失败信息（通常在 `Agent` 被 `drop` 时发送）。
    FailedInfo(FailedInfoRequest),
    /// 请求 `ManagerActor` 执行 Finalize 操作。
    /// 这会触发对当前所有分组的处理、数据合并、状态清理，并通过相应的通道发送结果。
    Finalize {
        /// 用于接收 Finalize 操作结果 (`Option<FinalizeResult>`) 的 `oneshot` 回复通道。
        reply_tx: FinalizeReplyTx,
    },
}

// --- Manager 内部状态辅助结构 (Internal Helper Structures) ---
// 这些结构体主要在 Manager 内部或相关组件（如 Agent）中使用，用于简化参数传递或状态表示。

/// 用于 `ChunkAgent` 和 `SingleAgent` 内部，存储提交所需的核心参数。
/// 便于在 Agent 内部传递和构建最终的 `SubmitBytesRequest`。
#[derive(Debug, Clone)] // Agent 可能需要 Clone
pub struct SubmitParams {
    /// 关联的预留 ID。
    pub(crate) res_id: ReservationId,
    /// 关联的分组 ID。
    pub(crate) group_id: GroupId,
    /// 预留空间的起始绝对偏移量。
    pub(crate) offset: AbsoluteOffset,
}

impl SubmitParams {
    /// 将 `SubmitParams` 转换为用于单次提交的 `Request::SubmitBytes`。
    pub fn into_single_request(self, bytes: Bytes, tx: SubmitReplyTx) -> Request {
        Request::SubmitBytes(SubmitBytesRequest {
            reservation_id: self.res_id,
            absolute_offset: self.offset,
            group_id: self.group_id,
            data: CommitType::Single(bytes),
            reply_tx: tx,
        })
    }

    /// 将 `SubmitParams` 转换为用于分块提交的 `Request::SubmitBytes`。
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

#[cfg(test)]
mod tests {
    //! 包含 `types` 模块中定义的各种结构体和枚举的单元测试。
    use super::*;
    // 导入父模块（即 `types.rs`）中的所有公开项
    use std::num::NonZeroUsize;

    /// 测试 `FailedReservationInfo` 结构体的创建和字段访问。
    /// 确保可以正确创建实例并访问其 `id`, `group_id`, `offset`, 和 `size` 字段。
    #[test]
    fn test_failed_reservation_info_creation() {
        let info = FailedReservationInfo {
            id: 1,         // 预留 ID
            group_id: 0,   // 分组 ID
            offset: 100,   // 偏移量
            size: 50,      // 大小
        };
        assert_eq!(info.id, 1);
        assert_eq!(info.group_id, 0);
        assert_eq!(info.offset, 100);
        assert_eq!(info.size, 50);
    }

    /// 测试 `FailedGroupData` 结构体的创建。
    /// 验证具有空数据块映射和空失败预留列表的 `FailedGroupData` 可以被创建。
    #[test]
    fn test_failed_group_data_creation() {
        let chunks = BTreeMap::new(); // 空的数据块 BTreeMap
        let reservations = Vec::new(); // 空的失败预留 Vec
        let data = FailedGroupData {
            group_id: 10,
            group_chunks: chunks.clone(), // 使用 clone 传入
            failed_reservations: reservations.clone(), // 使用 clone 传入
        };
        assert_eq!(data.group_id, 10);
        assert!(data.group_chunks.is_empty()); // 确认数据块映射为空
        assert!(data.failed_reservations.is_empty()); // 确认失败预留列表为空
    }

    /// 测试 `FinalizeResult` 的默认状态和其方法 `is_empty`, `failed_len`。
    /// 验证默认创建的 `FinalizeResult` 是空的，并且在添加失败数据后方法能正确反映状态。
    #[test]
    fn test_finalize_result_default_and_methods() {
        // 测试默认状态
        let default_result = FinalizeResult::default();
        assert!(default_result.is_empty()); // 默认应为空
        assert_eq!(default_result.failed_len(), 0); // 失败列表长度应为 0

        // 测试包含一个失败分组的状态
        let result_with_failure = FinalizeResult {
            failed: vec![FailedGroupData { // 创建一个包含一个元素的 Vec
                group_id: 1,
                group_chunks: BTreeMap::new(),
                failed_reservations: Vec::new(),
            }],
        };
        assert!(!result_with_failure.is_empty()); // 不应为空
        assert_eq!(result_with_failure.failed_len(), 1); // 失败列表长度应为 1
    }

    /// 测试 `CommitType` 枚举的两种变体：`Single` 和 `Chunked`。
    /// 确保可以正确创建这两种变体，并能通过模式匹配访问其内部数据。
    #[test]
    fn test_commit_type_variants() {
        // 测试 Single 变体
        let single_data = Bytes::from_static(b"hello");
        let commit_single = CommitType::Single(single_data.clone());
        if let CommitType::Single(s) = commit_single {
            assert_eq!(s, single_data); // 验证内部数据是否匹配
        } else {
            panic!("Expected CommitType::Single"); // 如果模式不匹配则失败
        }

        // 测试 Chunked 变体
        let chunk_data = vec![Bytes::from_static(b"world")];
        let commit_chunked = CommitType::Chunked(chunk_data.clone());
        if let CommitType::Chunked(c) = commit_chunked {
            assert_eq!(c, chunk_data); // 验证内部数据是否匹配
        } else {
            panic!("Expected CommitType::Chunked"); // 如果模式不匹配则失败
        }
    }

    /// 测试 `ReserveRequest` 结构体的创建。
    /// 验证可以成功创建一个带有 `NonZeroUsize` 大小和 `oneshot` 通道的 `ReserveRequest`。
    #[test]
    fn test_reserve_request_creation() {
        let (reply_tx, _reply_rx) = oneshot::channel(); // 创建一个 oneshot 通道对
        let size = NonZeroUsize::new(100).unwrap(); // 创建一个非零大小
        let req = ReserveRequest { size, reply_tx };
        assert_eq!(req.size.get(), 100); // 验证大小字段
        // reply_tx 字段存在即可，无需进一步测试其功能（由 oneshot 库保证）
    }

    /// 测试 `SubmitBytesRequest` 结构体的创建。
    /// 验证可以为 `Single` 类型的提交创建一个 `SubmitBytesRequest` 实例。
    #[test]
    fn test_submit_bytes_request_creation() {
        let (reply_tx, _reply_rx) = oneshot::channel(); // 创建 oneshot 通道
        let data = CommitType::Single(Bytes::new()); // 创建一个空的 Single CommitType
        let req = SubmitBytesRequest {
            reservation_id: 1,
            absolute_offset: 0,
            group_id: 0,
            data: data.clone(), // 传入 CommitType 数据
            reply_tx,           // 传入 oneshot 发送端
        };
        assert_eq!(req.reservation_id, 1);
        assert_eq!(req.absolute_offset, 0);
        assert_eq!(req.group_id, 0);
        // 使用 matches! 宏来检查 data 字段是否是 Single 变体，而不关心其内部值
        matches!(req.data, CommitType::Single(_));
    }

    /// 测试 `FailedInfoRequest` 结构体的创建。
    /// 验证可以基于一个 `FailedReservationInfo` 实例来创建 `FailedInfoRequest`。
    #[test]
    fn test_failed_info_request_creation() {
        let failed_res_info = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 0,
            size: 0,
        };
        let req = FailedInfoRequest {
            info: failed_res_info.clone(), // 使用 clone 传入
        };
        // 验证内部的 info 字段是否与原始的 failed_res_info 相等
        // 这里依赖 FailedReservationInfo 实现的 PartialEq
        assert_eq!(req.info, failed_res_info);
    }

    /// 测试 `Request` 枚举的不同变体。
    /// 验证可以正确创建 `Reserve`, `SubmitBytes`, `FailedInfo`, 和 `Finalize` 变体。
    #[test]
    fn test_request_enum_variants() {
        // 测试 Reserve 变体
        let (reserve_reply_tx, _) = oneshot::channel();
        let reserve_req_struct = ReserveRequest {
            size: NonZeroUsize::new(1).unwrap(),
            reply_tx: reserve_reply_tx,
        };
        let req_reserve = Request::Reserve(reserve_req_struct);
        matches!(req_reserve, Request::Reserve(_)); // 确认是 Reserve 变体

        // 测试 SubmitBytes 变体
        let (submit_reply_tx, _) = oneshot::channel();
        let submit_req_struct = SubmitBytesRequest {
            reservation_id: 1,
            absolute_offset: 0,
            group_id: 0,
            data: CommitType::Single(Bytes::new()),
            reply_tx: submit_reply_tx,
        };
        let req_submit = Request::SubmitBytes(submit_req_struct);
        matches!(req_submit, Request::SubmitBytes(_)); // 确认是 SubmitBytes 变体

        // 测试 FailedInfo 变体
        let failed_info_req_struct = FailedInfoRequest {
            info: FailedReservationInfo {
                id: 1,
                group_id: 0,
                offset: 0,
                size: 0,
            },
        };
        let req_failed_info = Request::FailedInfo(failed_info_req_struct);
        matches!(req_failed_info, Request::FailedInfo(_)); // 确认是 FailedInfo 变体

        // 测试 Finalize 变体
        let (finalize_reply_tx, _) = oneshot::channel();
        let req_finalize = Request::Finalize { reply_tx: finalize_reply_tx };
        matches!(req_finalize, Request::Finalize { .. }); // 确认是 Finalize 变体
    }
}
