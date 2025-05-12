//! 预留分配器模块
//! 定义了用于管理预留ID和偏移量分配的 trait 和实现。

use crate::types::{AbsoluteOffset, ReservationId};

/// `ReservationAllocator` Trait
///
/// 定义了预留 ID 和偏移量分配的核心接口。
/// 实现此 Trait 的组件负责：
/// 1. 生成唯一的、递增的 `ReservationId`。
/// 2. 根据预留请求的大小，计算并分配逻辑缓冲区中的下一个可用的 `AbsoluteOffset`。
/// 3. (可选) 提供回滚最近一次偏移量分配的机制，用于处理预留创建后的错误情况。
pub trait ReservationAllocator: Send + 'static {
    /// 为一个新的预留请求分配唯一的 ID 和起始偏移量。
    ///
    /// 实现者需要：
    /// - 返回当前的 `next_reservation_id` 并将其递增。
    /// - 返回当前的 `next_allocation_offset`，并根据传入的 `size` 将其增加。
    ///
    /// # Arguments
    /// * `size` - 本次预留请求的大小（字节数）。
    ///
    /// # Returns
    /// 一个元组 `(ReservationId, AbsoluteOffset)`:
    ///   - `ReservationId`: 分配给这个新预留的唯一 ID。
    ///   - `AbsoluteOffset`: 分配给这个新预留的在逻辑缓冲区中的起始绝对偏移量。
    fn next_reservation_details(&mut self, size: usize) -> (ReservationId, AbsoluteOffset);

    /// (可选) 回滚最近一次的偏移量分配。
    ///
    /// 这个方法主要用于处理一种错误情况：Manager 成功调用了 `next_reservation_details`，
    /// 但随后向请求方 (Agent) 发送包含 `SubmitAgent` 的 `Ok` 回复失败了。
    /// 在这种情况下，偏移量已经被分配出去，但对应的 Agent 永远无法提交数据。
    /// 调用此方法可以将 `next_allocation_offset` 回退 `size` 字节，
    /// 以便后续的预留可以重新使用这部分空间。
    /// **注意：** 此方法不回滚 `ReservationId`。
    ///
    /// # Arguments
    /// * `size` - 需要回滚的大小，**必须** 与导致需要回滚的那次 `next_reservation_details` 调用中传入的 `size` 相等。
    fn rollback_offset_allocation(&mut self, size: usize);

    // /// 获取当前的下一个 ReservationId (仅用于状态检查或初始化，非核心逻辑)。
    // fn current_next_reservation_id(&self) -> ReservationId;

    // /// 获取当前的下一个分配偏移量 (仅用于状态检查或初始化，非核心逻辑)。
    // fn current_next_allocation_offset(&self) -> AbsoluteOffset;
}

/// `ReservationAllocator` Trait 的默认实现。
/// 使用两个简单的计数器来跟踪下一个可用的 ID 和偏移量。
pub struct DefaultReservationAllocator {
    /// 下一个可用的预留 ID。
    next_reservation_id: ReservationId,
    /// 下一个可用的逻辑缓冲区起始偏移量。
    next_allocation_offset: AbsoluteOffset,
}

impl DefaultReservationAllocator {
    /// 创建一个新的 `DefaultReservationAllocator` 实例。
    /// ID 和偏移量都从 0 开始。
    pub fn new() -> Self {
        DefaultReservationAllocator {
            next_reservation_id: 0, // ID 从 0 开始
            next_allocation_offset: 0, // 偏移量从 0 开始
        }
    }
}

// 为 DefaultReservationAllocator 实现 ReservationAllocator Trait
impl ReservationAllocator for DefaultReservationAllocator {
    /// 分配下一个预留 ID 和偏移量。
    fn next_reservation_details(&mut self, size: usize) -> (ReservationId, AbsoluteOffset) {
        // 获取当前 ID 并递增
        let reservation_id = self.next_reservation_id;
        self.next_reservation_id += 1;

        // 获取当前偏移量并增加 size
        let offset = self.next_allocation_offset;
        self.next_allocation_offset += size;

        // 返回分配的 ID 和偏移量
        (reservation_id, offset)
    }

    /// 回滚偏移量分配。
    fn rollback_offset_allocation(&mut self, size: usize) {
        // 从下一个可用偏移量中减去 size
        // 使用 saturating_sub 防止因意外调用导致偏移量下溢到负数（虽然 usize 不会变负，但会 panic 或环绕）
        self.next_allocation_offset = self.next_allocation_offset.saturating_sub(size);
    }

    // /// 获取当前下一个 ID (用于调试或状态检查)。
    // fn current_next_reservation_id(&self) -> ReservationId {
    //     self.next_reservation_id
    // }

    // /// 获取当前下一个偏移量 (用于调试或状态检查)。
    // fn current_next_allocation_offset(&self) -> AbsoluteOffset {
    //     self.next_allocation_offset
    // }
}
