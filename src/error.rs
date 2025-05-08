//! 错误类型定义
//! 定义了 Manager 内部错误 (`ManagerError`) 和用户与 Handle 交互时可能遇到的顶层错误 (`BufferError`)。

use crate::types::{AbsoluteOffset, GroupId, Request, ReservationId}; // 引入所需类型
use std::ops::Range; // 用于表示范围
use thiserror::Error; // 引入 thiserror 宏，方便定义错误类型
use tokio::sync::{mpsc, oneshot}; // 引入 tokio 的通道错误类型

/// Manager 内部处理请求时可能发生的具体错误。
/// 这些错误通常通过 oneshot 通道回复给调用者（Handle）。
#[derive(Error, Debug, Clone, PartialEq)] // 使用 thiserror，允许 Debug、Clone 和 PartialEq (方便测试)
pub enum ManagerError {
    /// 表示一个未明确分类的内部逻辑错误或意外状态。
    #[error("内部错误: {0}")]
    Internal(String),

    /// 表示 Manager 正在执行 finalize 操作，不再接受新的常规请求。
    #[error("管理器当前正在结束 (Finalizing)")]
    ManagerFinalizing,

    /// 表示请求中指定的预留 ID 未找到。
    /// 可能原因：预留从未成功创建、已被处理（成功或失败）、或者客户端提供了错误的 ID。
    #[error("预留位 {0} 未找到")]
    ReservationNotFound(ReservationId),

    /// 表示在 Commit 预留时，提交的数据总大小与预留时申请的大小不匹配。
    #[error("预留位 {reservation_id} Commit 时数据总大小 ({actual} 字节) 与预留大小 ({expected} 字节) 不匹配")]
    CommitSizeMismatch {
        reservation_id: ReservationId,
        expected: usize,
        actual: usize,
    },

    /// 表示尝试提交一个已经被成功 Commit 的预留。
    #[error("预留位 {0} 已被提交")]
    AlreadyCommitted(ReservationId),

    /// 表示提交的数据块的绝对偏移量与预留的绝对偏移量不匹配。
    #[error("提交偏移无效 for Reservation {0}: 提交偏移 {1} 与预留偏移 {2:?} 不匹配")]
    SubmitOffsetInvalid(
        ReservationId,
        AbsoluteOffset,        // 提交的偏移
        Range<AbsoluteOffset>, // 预留的范围 (仅用于信息展示)
    ),

    /// 表示提交的数据块的范围（起始偏移 + 长度）超出了该预留所允许的范围。
    /// 通常发生在分块提交时。
    #[error("提交的数据块范围无效 for Reservation {0}: 偏移 {1}, 长度 {2} 超出了预留范围 {3:?}")]
    SubmitRangeInvalid(
        ReservationId,
        AbsoluteOffset,        // 数据块的绝对起始偏移
        usize,                 // 数据块的长度
        Range<AbsoluteOffset>, // 预留允许的有效偏移范围
    ),

    // /// (已移除) 表示在同一个预留中，尝试向同一个起始偏移量重复提交数据块。
    // /// 注意：当前 Manager 实现是在 BTreeMap 中覆盖，而非报错。如果需要严格禁止，可以重新引入。
    // #[error("重复提交数据块 for Reservation {0} at offset {1}")]
    // SubmitChunkDuplicate(ReservationId, AbsoluteOffset),
    /// 表示 Agent 在提交前校验发现数据大小与预留大小不符 (单次提交)。
    #[error("提交的数据大小无效 for Reservation {reservation_id}: 期望 {expected}, 实际 {actual}")]
    SubmitSizeIncorrect {
        reservation_id: ReservationId,
        expected: usize,
        actual: usize,
    },

    /// 表示 Agent 在添加块时发现会超出预留总大小 (分块提交)。
    #[error("提交的数据大小无效 for Reservation {reservation_id}: 剩余空间 {largest}, 尝试添加 {actual}")]
    SubmitSizeTooLarge {
        reservation_id: ReservationId,
        largest: usize, // 剩余可用空间
        actual: usize,  // 尝试添加的大小
    },

    /// 表示在处理某个预留时，未能找到它所属的分组信息（这是一个内部逻辑错误）。
    #[error("预留位 {0} 所属的分组未找到 (内部错误)")]
    GroupNotFoundForReservation(ReservationId),

    /// 表示尝试访问一个不存在的分组 ID（这是一个内部逻辑错误）。
    #[error("分组 {0} 未找到 (内部错误)")]
    GroupNotFound(GroupId),

    // --- Group Processing Errors (由 check_and_process 返回) ---
    /// 表示分组数据不连续。
    #[error("分组 {0} 数据不连续: 期望偏移 {1}, 实际偏移 {2}")]
    GroupNotContiguous(GroupId, AbsoluteOffset, AbsoluteOffset),

    /// 表示分组的总大小与根据元数据计算的期望大小不匹配。
    #[error("分组 {0} 大小不匹配: 元数据期望 {1}, 提交计算得到 {2}")]
    GroupSizeMismatch(GroupId, usize, usize),

    /// 表示分组的起始偏移与根据元数据计算的期望起始偏移不匹配。
    #[error("分组 {0} 起始偏移不匹配: 元数据期望 {1:?}, 提交计算得到 {2:?}")]
    // 使用 .1 .2 访问元组字段
    GroupOffsetMismatch(GroupId, Option<AbsoluteOffset>, Option<AbsoluteOffset>),

    /// 表示分组包含失败的预留信息，无法成功合并。
    #[error("分组 {0} 包含失败的预留信息，无法处理")]
    GroupContainsFailures(GroupId),

    /// 表示在尝试发送成功合并的分组数据到消费者时失败。
    #[error("发送分组 {0} 的成功数据失败 (通道可能关闭)")]
    SendCompletionFailed(GroupId),

    /// 表示合并分组数据时内部大小校验失败。
    #[error("分组 {0} 数据合并后大小校验失败: 期望 {1}, 实际 {2}")]
    MergeSizeMismatch(GroupId, usize, usize),
}

/// 用户与 Handle 交互时可能遇到的顶层错误类型。
/// 封装了底层的通信错误 (通道发送/接收失败) 和来自 Manager 的业务逻辑错误。
#[derive(Error, Debug)] // 使用 thiserror，允许 Debug 打印
pub enum BufferError {
    /// 表示向 Manager 发送请求失败。
    /// 通常原因：Manager Actor 任务已经panic或正常退出，导致请求通道关闭。
    #[error("发送请求到管理器失败 (通道可能已关闭): {0}")]
    SendRequestError(#[from] mpsc::error::SendError<Request>), // 包装 mpsc 发送错误

    /// 表示从 Manager 接收回复失败。
    /// 通常原因：Manager 在发送回复前就panic或退出了，导致 oneshot 回复通道关闭。
    #[error("接收管理器回复失败 (通道可能已关闭): {0}")]
    ReceiveReplyError(#[from] oneshot::error::RecvError), // 包装 oneshot 接收错误

    /// 包含从 Manager 返回的具体业务逻辑错误 (`ManagerError`)。
    /// 通过 `#[from]` 可以方便地将 `ManagerError` 转换为 `BufferError`。
    #[error("管理器未能成功处理请求: {0}")]
    ManagerError(#[from] ManagerError), // 包装 Manager 内部错误
}

// 在 src/error.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*; // 导入父模块中的所有内容
    use std::ops::Range;

    // 测试 ManagerError 变体的创建和 PartialEq
    #[test]
    fn test_manager_error_creation_and_equality() {
        assert_eq!(
            ManagerError::Internal("test".to_string()),
            ManagerError::Internal("test".to_string())
        );
        assert_ne!(
            ManagerError::Internal("test1".to_string()),
            ManagerError::Internal("test2".to_string())
        );

        assert_eq!(
            ManagerError::ManagerFinalizing,
            ManagerError::ManagerFinalizing
        );

        assert_eq!(
            ManagerError::ReservationNotFound(1),
            ManagerError::ReservationNotFound(1)
        );
        assert_ne!(
            ManagerError::ReservationNotFound(1),
            ManagerError::ReservationNotFound(2)
        );

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
            reservation_id: 2,
            expected: 100,
            actual: 90,
        };
        assert_eq!(cs_mismatch1, cs_mismatch2);
        assert_ne!(cs_mismatch1, cs_mismatch3);

        assert_eq!(
            ManagerError::AlreadyCommitted(1),
            ManagerError::AlreadyCommitted(1)
        );

        let offset_invalid1 = ManagerError::SubmitOffsetInvalid(1, 10, Range { start: 0, end: 20 });
        let offset_invalid2 = ManagerError::SubmitOffsetInvalid(1, 10, Range { start: 0, end: 20 });
        assert_eq!(offset_invalid1, offset_invalid2);

        let range_invalid1 =
            ManagerError::SubmitRangeInvalid(1, 5, 10, Range { start: 0, end: 10 });
        let range_invalid2 =
            ManagerError::SubmitRangeInvalid(1, 5, 10, Range { start: 0, end: 10 });
        assert_eq!(range_invalid1, range_invalid2);

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

        assert_eq!(
            ManagerError::GroupNotFoundForReservation(1),
            ManagerError::GroupNotFoundForReservation(1)
        );
        assert_eq!(
            ManagerError::GroupNotFound(1),
            ManagerError::GroupNotFound(1)
        );
        assert_eq!(
            ManagerError::GroupNotContiguous(1, 100, 110),
            ManagerError::GroupNotContiguous(1, 100, 110)
        );
        assert_eq!(
            ManagerError::GroupSizeMismatch(1, 100, 90),
            ManagerError::GroupSizeMismatch(1, 100, 90)
        );
        assert_eq!(
            ManagerError::GroupOffsetMismatch(1, Some(0), Some(10)),
            ManagerError::GroupOffsetMismatch(1, Some(0), Some(10))
        );
        assert_eq!(
            ManagerError::GroupContainsFailures(1),
            ManagerError::GroupContainsFailures(1)
        );
        assert_eq!(
            ManagerError::SendCompletionFailed(1),
            ManagerError::SendCompletionFailed(1)
        );
        assert_eq!(
            ManagerError::MergeSizeMismatch(1, 100, 90),
            ManagerError::MergeSizeMismatch(1, 100, 90)
        );
    }

    // 测试 ManagerError 的 Display 实现 (通过 thiserror 自动实现)
    #[test]
    fn test_manager_error_display() {
        assert_eq!(
            format!("{}", ManagerError::Internal("abc".to_string())),
            "内部错误: abc"
        );
        assert_eq!(
            format!("{}", ManagerError::ManagerFinalizing),
            "管理器当前正在结束 (Finalizing)"
        );
        // 可以为其他错误类型添加类似的 Display 测试
    }

    // 测试 BufferError 的创建 (主要通过 From trait)
    #[test]
    fn test_buffer_error_from_manager_error() {
        let manager_err = ManagerError::ReservationNotFound(123);
        let buffer_err: BufferError = manager_err.clone().into(); // 需要 ManagerError 实现 Clone
        match buffer_err {
            BufferError::ManagerError(me) => assert_eq!(me, manager_err),
            _ => panic!("Expected BufferError::ManagerError"),
        }
    }

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
}
