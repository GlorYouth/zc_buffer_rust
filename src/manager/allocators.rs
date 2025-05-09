//! 预留分配器模块
//! 定义了用于管理预留ID和偏移量分配的 trait 和实现。

use crate::types::{AbsoluteOffset, ReservationId};

/// ReservationAllocator Trait
/// 定义了预留ID和偏移量分配的接口。
pub(crate) trait ReservationAllocator {
    /// 获取下一个预留的ID和起始偏移量。
    ///
    /// # Arguments
    /// * `size` - 本次预留请求的大小。
    ///
    /// # Returns
    /// 元组 `(ReservationId, AbsoluteOffset)`，分别表示新的预留ID和分配的绝对起始偏移量。
    fn next_reservation_details(&mut self, size: usize) -> (ReservationId, AbsoluteOffset);

    /// （可选）回滚最近一次的偏移量分配。
    /// 这在预留创建后但未能成功通知客户端（例如回复发送失败）时可能需要。
    ///
    /// # Arguments
    /// * `size` - 需要回滚的大小，应与上次分配时的大小一致。
    fn rollback_offset_allocation(&mut self, size: usize);

    // /// 获取当前下一个预留ID（主要用于Manager初始化或状态同步，不直接参与分配流程）
    // fn current_next_reservation_id(&self) -> ReservationId;

    // /// 获取当前下一个分配偏移（主要用于Manager初始化或状态同步）
    // fn current_next_allocation_offset(&self) -> AbsoluteOffset;
}

/// `ReservationAllocator` Trait 的默认实现。
pub(crate) struct DefaultReservationAllocator {
    next_reservation_id: ReservationId,
    next_allocation_offset: AbsoluteOffset,
}

impl DefaultReservationAllocator {
    /// 创建一个新的 `DefaultReservationAllocator` 实例。
    pub fn new() -> Self {
        DefaultReservationAllocator {
            next_reservation_id: 0,
            next_allocation_offset: 0,
        }
    }
}

impl ReservationAllocator for DefaultReservationAllocator {
    fn next_reservation_details(&mut self, size: usize) -> (ReservationId, AbsoluteOffset) {
        let reservation_id = self.next_reservation_id;
        self.next_reservation_id += 1;

        let offset = self.next_allocation_offset;
        self.next_allocation_offset += size;

        (reservation_id, offset)
    }

    fn rollback_offset_allocation(&mut self, size: usize) {
        // 确保回滚不会导致偏移量下溢
        self.next_allocation_offset = self.next_allocation_offset.saturating_sub(size);
    }

    // fn current_next_reservation_id(&self) -> ReservationId {
    //     self.next_reservation_id
    // }

    // fn current_next_allocation_offset(&self) -> AbsoluteOffset {
    //     self.next_allocation_offset
    // }
}
