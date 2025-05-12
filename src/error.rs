//! 定义库中使用的错误类型。
//!
//! 本模块包含两种主要的错误枚举：
//! - `ManagerError`: 表示在 `ManagerActor` 内部处理请求时可能发生的具体业务逻辑错误。
//!   这些错误通常通过请求的回调通道 (`oneshot::Sender`) 返回给调用者。
//! - `BufferError`: 这是暴露给库用户的顶层错误类型。
//!   它封装了与 `ManagerActor` 通信时可能发生的错误（如通道发送/接收失败）
//!   以及从 `ManagerActor` 收到的 `ManagerError`。

use crate::types::{AbsoluteOffset, GroupId, Request, ReservationId};
// 引入在错误消息中可能用到的类型
use std::ops::Range;
// 用于在错误消息中表示有效的偏移范围
use thiserror::Error;
// 使用 `thiserror` 宏可以方便地为枚举变体实现 `std::error::Error` trait 和 `Display` trait。
use tokio::sync::{mpsc, oneshot};
// 引入 Tokio 通道的错误类型，用于表示发送或接收失败

/// `ManagerActor` 内部处理逻辑中可能产生的具体错误。
///
/// 这些错误代表了在处理 `Request` 时可能遇到的各种失败场景，
/// 例如找不到预留、提交大小不匹配、分组数据不一致等。
/// 它们通常通过 `oneshot` 回复通道传递给请求的发起者（通常是 `ZeroCopyHandle` 或 `Agent`）。
#[derive(Error, Debug, Clone, PartialEq, Eq)] // 添加 Eq
pub enum ManagerError {
    /// 表示一个未预期的内部错误或不一致状态。
    /// 通常表示代码逻辑中存在 bug。
    #[error("内部逻辑错误: {0}")]
    Internal(String),

    /// 表示 `ManagerActor` 正在执行 `Finalize` 操作。
    /// 在此状态下，`ManagerActor` 通常不再接受新的 `Reserve` 或 `SubmitBytes` 请求。
    #[error("管理器当前正在执行 Finalize 操作，不接受新请求")]
    ManagerFinalizing,

    /// 表示请求中提供的 `ReservationId` 在 `ManagerActor` 的内部状态中未找到。
    /// 可能的原因：
    /// - 该 `ReservationId` 从未被成功分配过。
    /// - 该预留已经被成功提交或标记为失败，并已被清理。
    /// - 客户端代码错误地使用了无效的 `ReservationId`。
    #[error("预留 ID {0} 未找到")]
    ReservationNotFound(ReservationId),

    /// 表示在尝试提交数据到一个预留空间时 (`SubmitBytesRequest`)，
    /// 提供的总数据大小（对于 `CommitType::Single`）或累计块大小（对于 `CommitType::Chunked` 的最后一块）
    /// 与最初预留时指定的大小 (`ReserveRequest::size`) 不一致。
    #[error(
        "预留 ID {reservation_id} 的提交大小不匹配: 期望 {expected} 字节, 实际收到 {actual} 字节"
    )]
    CommitSizeMismatch {
        /// 相关的预留 ID。
        reservation_id: ReservationId,
        /// 预留时期望的大小。
        expected: usize,
        /// 实际提交的总大小。
        actual: usize,
    },

    /// 表示尝试为一个已经被成功提交数据的预留空间再次提交数据。
    /// 一个预留空间只能被成功提交一次。
    #[error("预留 ID {0} 已经被提交过数据")]
    AlreadyCommitted(ReservationId),

    /// (此错误目前不直接在 Manager 中抛出，由 Agent 在提交前检查)
    /// 表示 `SubmitBytesRequest` 中指定的 `absolute_offset` 与该 `ReservationId`
    /// 对应的预留空间的实际起始偏移量不匹配。
    /// 这通常指示客户端状态与 Manager 状态不同步或存在逻辑错误。
    #[error(
        "提交偏移量无效 (预留 ID {0}): 提交偏移量 {1} 与预留记录的范围 {2:?} 不匹配"
    )]
    SubmitOffsetInvalid(
        ReservationId,
        AbsoluteOffset,        // 提交请求中提供的偏移量
        Range<AbsoluteOffset>, // Manager 记录的该预留的有效偏移范围
    ),

    /// (此错误目前主要由 Agent 在分块提交时检查)
    /// 表示提交的数据块（由其起始偏移量和长度定义）超出了该预留空间的边界。
    /// 例如，向一个大小为 100，偏移为 50 的预留提交一个偏移为 140，长度为 20 的块。
    #[error(
        "提交的数据块范围无效 (预留 ID {0}): 块偏移 {1}, 块长度 {2} 超出了预留范围 {3:?}"
    )]
    SubmitRangeInvalid(
        ReservationId,
        AbsoluteOffset,        // 尝试提交的数据块的起始绝对偏移
        usize,                 // 尝试提交的数据块的长度
        Range<AbsoluteOffset>, // Manager 记录的该预留的有效偏移范围
    ),

    /// 由 `SingleAgent` 在提交前检查时发现，提供的 `Bytes` 数据大小与预留大小不符。
    #[error(
        "单次提交数据大小不正确 (预留 ID {reservation_id}): 期望 {expected} 字节, 实际提供 {actual} 字节"
    )]
    SubmitSizeIncorrect {
        /// 相关的预留 ID。
        reservation_id: ReservationId,
        /// 预留时期望的大小。
        expected: usize,
        /// 实际提供的 `Bytes` 的大小。
        actual: usize,
    },

    /// 由 `ChunkAgent` 在添加块 (`add_chunk`) 时检查发现，如果添加此块，
    /// 已添加的总大小将超过预留的总大小。
    #[error(
        "分块提交时块大小过大 (预留 ID {reservation_id}): 预留剩余空间 {largest} 字节, 尝试添加 {actual} 字节"
    )]
    SubmitSizeTooLarge {
        /// 相关的预留 ID。
        reservation_id: ReservationId,
        /// 当前预留还允许添加的最大字节数。
        largest: usize,
        /// 尝试添加的新块的大小。
        actual: usize,
    },

    /// 表示在处理一个已知的 `ReservationId` 时，无法在其元数据中找到关联的 `GroupId`。
    /// 这通常是一个内部逻辑错误，表示 `ManagerActor` 的状态可能已损坏。
    #[error("未能找到预留 ID {0} 关联的分组 ID (内部错误)")]
    GroupNotFoundForReservation(ReservationId),

    /// 表示尝试访问或操作一个不存在的 `GroupId`。
    /// 这通常也是一个内部逻辑错误。
    #[error("分组 ID {0} 未找到 (内部错误)")]
    GroupNotFound(GroupId),

    // --- 分组处理错误 (Group Processing Errors) ---
    // 这些错误通常在 `ManagerActor` 的 `finalize` 阶段，由 `GroupDataProcessor` 组件
    // 或相关的分组处理逻辑产生。

    /// 表示在检查一个分组的数据块时，发现它们的偏移量不是连续的。
    /// 例如，一个分组包含偏移量 0-99 和 200-299 的块，中间缺少 100-199。
    #[error("分组 {0} 的数据块不连续: 在偏移 {1} 后期望下一个块从 {1} 开始, 但实际为 {2}")]
    GroupNotContiguous(GroupId, AbsoluteOffset, AbsoluteOffset),

    /// 表示分组内所有已提交数据块的总大小与该分组元数据记录的期望总大小（所有成功预留的大小之和）不匹配。
    /// 这可能指示提交逻辑错误或状态不一致。
    #[error(
        "分组 {0} 的总大小不匹配: 元数据记录期望总大小 {1} 字节, 实际提交数据总大小 {2} 字节"
    )]
    GroupSizeMismatch(GroupId, usize, usize),

    /// 表示分组内第一个数据块的起始偏移量与分组元数据记录的期望起始偏移量不匹配。
    /// 这可能指示预留分配或分组逻辑错误。
    #[error(
        "分组 {0} 的起始偏移不匹配: 元数据记录期望偏移 {1:?}, 实际第一个数据块偏移 {2:?}"
    )]
    GroupOffsetMismatch(
        GroupId,
        Option<AbsoluteOffset>, // 元数据记录的期望起始偏移 (可能为 None 如果分组为空)
        Option<AbsoluteOffset>, // 实际找到的第一个数据块的起始偏移 (可能为 None)
    ),

    /// 表示分组处理逻辑发现该分组包含了至少一个失败的预留 (`FailedReservationInfo`)。
    /// 根据设计，包含失败预留的分组不能视为成功处理，其数据应通过失败通道发送。
    #[error("分组 {0} 因包含失败的预留而无法被视为成功处理")]
    GroupContainsFailures(GroupId),

    /// 表示在成功合并了一个分组的数据后，尝试通过 `completed_data_tx` MPSC 通道
    /// 将结果 (`SuccessfulGroupData`) 发送给消费者时失败。
    /// 最常见的原因是消费者已经关闭了接收端。
    #[error("发送分组 {0} 的成功合并数据失败 (通道可能已关闭或已满)")]
    SendCompletionFailed(GroupId),

    /// 表示在内部合并分组数据块时，最终合并得到的 `Vec<u8>` 的长度与预期不符。
    /// 这通常是一个内部合并逻辑的错误。
    #[error("合并分组 {0} 的数据块后大小校验失败: 期望 {1} 字节, 实际得到 {2} 字节")]
    MergeSizeMismatch(GroupId, usize, usize),
}

/// 用户与 `ZeroCopyHandle` 交互时可能遇到的顶层错误类型。
///
/// 这个枚举旨在为库的使用者提供一个统一的错误处理接口。
/// 它隐藏了底层的实现细节（如使用的是 MPSC 还是 `oneshot` 通道），
/// 并将通信错误和 `ManagerActor` 的业务逻辑错误整合在一起。
#[derive(Error, Debug)]
pub enum BufferError {
    /// 表示向 `ManagerActor` 发送请求 (`Request`) 时失败。
    ///
    /// 这通常发生在 `ManagerActor` 的后台任务已经终止（panic 或正常退出）的情况下，
    /// 导致连接 `ZeroCopyHandle` 和 `ManagerActor` 的 MPSC 请求通道被关闭。
    ///
    /// 底层错误是 `tokio::sync::mpsc::error::SendError<Request>`。
    #[error("向管理器发送请求失败 (管理器可能已停止): {0}")]
    SendRequestError(#[from] mpsc::error::SendError<Request>),

    /// 表示等待并接收来自 `ManagerActor` 的回复时失败。
    ///
    /// 每个发送给 `ManagerActor` 的请求（如 `Reserve`, `SubmitBytes`, `Finalize`）
    /// 都包含一个 `oneshot` 回复通道。如果在 `ManagerActor` 发送回复之前，
    /// 它就终止了，那么这个回复通道会被关闭，导致接收端出错。
    ///
    /// 底层错误是 `tokio::sync::oneshot::error::RecvError`。
    #[error("从管理器接收回复失败 (管理器可能已停止): {0}")]
    ReceiveReplyError(#[from] oneshot::error::RecvError),

    /// 表示 `ManagerActor` 成功接收并处理了请求，但在处理过程中遇到了业务逻辑错误。
    ///
    /// 这个变体包装了一个 `ManagerError` 枚举实例，提供了具体的失败原因。
    /// 通过 `#[from]` 属性，`ManagerError` 可以被自动转换为 `BufferError::ManagerError`。
    #[error("管理器报告处理错误: {0}")]
    ManagerError(#[from] ManagerError),
}

#[cfg(test)]
mod tests {
    //! 包含 `error` 模块中定义的错误类型的单元测试。
    use super::*;
    // 导入父模块（`error.rs`）中的所有公开项
    use std::ops::Range;
    // 导入 Range 用于构造部分错误变体

    /// 测试 `ManagerError` 枚举的各种变体的创建和相等性 (`PartialEq`)。
    /// 确保相同的错误变体和内容比较时相等，不同的错误变体或内容比较时不相等。
    /// 这也间接测试了 `#[derive(PartialEq)]` 是否按预期工作。
    #[test]
    fn test_manager_error_creation_and_equality() {
        // 测试 Internal
        assert_eq!(
            ManagerError::Internal("test".to_string()),
            ManagerError::Internal("test".to_string())
        );
        assert_ne!(
            ManagerError::Internal("test1".to_string()),
            ManagerError::Internal("test2".to_string())
        );

        // 测试 ManagerFinalizing (单元结构体变体)
        assert_eq!(
            ManagerError::ManagerFinalizing,
            ManagerError::ManagerFinalizing
        );

        // 测试 ReservationNotFound
        assert_eq!(
            ManagerError::ReservationNotFound(1),
            ManagerError::ReservationNotFound(1)
        );
        assert_ne!(
            ManagerError::ReservationNotFound(1),
            ManagerError::ReservationNotFound(2)
        );

        // 测试 CommitSizeMismatch (带命名成员的结构体变体)
        let cs_mismatch1 = ManagerError::CommitSizeMismatch {
            reservation_id: 1,
            expected: 100,
            actual: 90,
        };
        let cs_mismatch2 = ManagerError::CommitSizeMismatch {
            reservation_id: 1,
            expected: 100,
            actual: 90,
        };
        let cs_mismatch3 = ManagerError::CommitSizeMismatch {
            reservation_id: 2, // 不同的 reservation_id
            expected: 100,
            actual: 90,
        };
        assert_eq!(cs_mismatch1, cs_mismatch2); // 相同内容，应相等
        assert_ne!(cs_mismatch1, cs_mismatch3); // 不同内容，应不等

        // 测试 AlreadyCommitted
        assert_eq!(
            ManagerError::AlreadyCommitted(1),
            ManagerError::AlreadyCommitted(1)
        );

        // 测试 SubmitOffsetInvalid (带 Range)
        let offset_invalid1 = ManagerError::SubmitOffsetInvalid(1, 10, Range { start: 0, end: 20 });
        let offset_invalid2 = ManagerError::SubmitOffsetInvalid(1, 10, Range { start: 0, end: 20 });
        assert_eq!(offset_invalid1, offset_invalid2);

        // 测试 SubmitRangeInvalid
        let range_invalid1 =
            ManagerError::SubmitRangeInvalid(1, 5, 10, Range { start: 0, end: 10 });
        let range_invalid2 =
            ManagerError::SubmitRangeInvalid(1, 5, 10, Range { start: 0, end: 10 });
        assert_eq!(range_invalid1, range_invalid2);

        // 测试 SubmitSizeIncorrect
        let size_incorrect1 = ManagerError::SubmitSizeIncorrect {
            reservation_id: 1,
            expected: 10,
            actual: 5,
        };
        let size_incorrect2 = ManagerError::SubmitSizeIncorrect {
            reservation_id: 1,
            expected: 10,
            actual: 5,
        };
        assert_eq!(size_incorrect1, size_incorrect2);

        // 测试 SubmitSizeTooLarge
        let size_too_large1 = ManagerError::SubmitSizeTooLarge {
            reservation_id: 1,
            largest: 10,
            actual: 15,
        };
        let size_too_large2 = ManagerError::SubmitSizeTooLarge {
            reservation_id: 1,
            largest: 10,
            actual: 15,
        };
        assert_eq!(size_too_large1, size_too_large2);

        // 测试 GroupNotFoundForReservation
        assert_eq!(
            ManagerError::GroupNotFoundForReservation(1),
            ManagerError::GroupNotFoundForReservation(1)
        );
        // 测试 GroupNotFound
        assert_eq!(
            ManagerError::GroupNotFound(1),
            ManagerError::GroupNotFound(1)
        );
        // 测试 GroupNotContiguous
        assert_eq!(
            ManagerError::GroupNotContiguous(1, 100, 110),
            ManagerError::GroupNotContiguous(1, 100, 110)
        );
        // 测试 GroupSizeMismatch
        assert_eq!(
            ManagerError::GroupSizeMismatch(1, 100, 90),
            ManagerError::GroupSizeMismatch(1, 100, 90)
        );
        // 测试 GroupOffsetMismatch (带 Option)
        assert_eq!(
            ManagerError::GroupOffsetMismatch(1, Some(0), Some(10)),
            ManagerError::GroupOffsetMismatch(1, Some(0), Some(10))
        );
        // 测试 GroupContainsFailures
        assert_eq!(
            ManagerError::GroupContainsFailures(1),
            ManagerError::GroupContainsFailures(1)
        );
        // 测试 SendCompletionFailed
        assert_eq!(
            ManagerError::SendCompletionFailed(1),
            ManagerError::SendCompletionFailed(1)
        );
        // 测试 MergeSizeMismatch
        assert_eq!(
            ManagerError::MergeSizeMismatch(1, 100, 90),
            ManagerError::MergeSizeMismatch(1, 100, 90)
        );
    }

    /// 测试 `ManagerError` 的 `Display` trait 实现。
    /// `thiserror` 宏会自动根据 `#[error("...")]` 属性生成 `Display` 实现。
    /// 这个测试验证生成的错误消息格式是否符合预期。
    #[test]
    fn test_manager_error_display() {
        // 验证 Internal 错误的消息格式
        assert_eq!(
            format!("{}", ManagerError::Internal("abc".to_string())),
            "内部逻辑错误: abc"
        );
        // 验证 ManagerFinalizing 错误的消息格式
        assert_eq!(
            format!("{}", ManagerError::ManagerFinalizing),
            "管理器当前正在执行 Finalize 操作，不接受新请求"
        );
        // 可以为其他错误类型添加类似的 Display 测试，以确保消息清晰且准确。
        // 例如：
        assert_eq!(
            format!("{}", ManagerError::ReservationNotFound(42)),
            "预留 ID 42 未找到"
        );
        assert_eq!(
            format!("{}", ManagerError::CommitSizeMismatch{ reservation_id: 1, expected: 10, actual: 8}),
            "预留 ID 1 的提交大小不匹配: 期望 10 字节, 实际收到 8 字节"
        );
    }

    /// 测试 `BufferError` 的创建，特别是通过 `From<ManagerError>` 实现。
    /// 验证 `ManagerError` 可以被正确地转换为 `BufferError::ManagerError`。
    #[test]
    fn test_buffer_error_from_manager_error() {
        let manager_err = ManagerError::ReservationNotFound(123);
        // 使用 .into() 方法，它会调用 From<ManagerError> for BufferError 实现
        let buffer_err: BufferError = manager_err.clone().into(); // 需要 ManagerError 实现 Clone

        // 使用 match 确认转换后的 BufferError 是 ManagerError 变体，
        // 并且内部包含的 ManagerError 与原始的 manager_err 相等。
        match buffer_err {
            BufferError::ManagerError(me) => assert_eq!(me, manager_err),
            // 如果是其他变体（SendRequestError 或 ReceiveReplyError），则测试失败
            _ => panic!("Expected BufferError::ManagerError"),
        }
    }

    /*
    // 注释掉的部分：测试从通道错误转换到 BufferError
    // 这些测试比较难编写，因为 mpsc::error::SendError<T> 包含泛型 T，不易直接构造。
    // 通常依赖于模拟通道关闭的场景。
    // oneshot::error::RecvError 是单元结构体，相对容易构造。

    // 辅助函数来模拟 mpsc::SendError (实际的 SendError<Request> 很难直接构造)
    // 这里我们仅测试 BufferError::ManagerError，其他变体需要更复杂的设置
    // fn create_dummy_send_error() -> mpsc::error::SendError<Request> {
    //     // 实际构造 mpsc::SendError<T> 比较困难，因为它通常包含 T
    //     // 并且是在通道发送失败时由库内部创建的。
    //     // 此处我们跳过直接测试 SendRequestError 的构造，
    //     // 因为它的主要作用是包装实际的库错误。
    // }

    // fn create_dummy_recv_error() -> oneshot::error::RecvError {
    //     // oneshot::error::RecvError 是一个单元结构体，可以被构造
    //     oneshot::error::RecvError {}
    // }

    // #[test]
    // fn test_buffer_error_from_channel_errors() {
    //     // let send_err = create_dummy_send_error();
    //     // let buffer_send_err: BufferError = send_err.into();
    //     // matches!(buffer_send_err, BufferError::SendRequestError(_));
    //
    //     let recv_err = create_dummy_recv_error();
    //     let buffer_recv_err: BufferError = recv_err.into();
    //     matches!(buffer_recv_err, BufferError::ReceiveReplyError(_));
    // }
    */
}
