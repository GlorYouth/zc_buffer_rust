//! Manager 内部使用的核心状态结构 (`GroupState`)
//!
//! 这个模块定义了 `GroupState` 结构体，用于表示和跟踪 Manager 内部一个数据分组的状态。
//! 一个 `GroupState` 实例聚合了属于该分组的所有预留信息、已提交的数据块、失败信息以及分组自身的元数据（如是否密封）。

use crate::types::{AbsoluteOffset, ReservationId}; // 引入类型别名
use crate::FailedReservationInfo; // 引入失败预留信息结构体
use bytes::Bytes; // 引入 Bytes 用于存储数据块
use std::collections::{BTreeMap, HashMap, HashSet}; // 引入集合类型

/// Manager 内部跟踪的单个分组的状态。
///
/// 分组（Group）是 Manager 管理缓冲区的基本单元。多个逻辑上连续或相关的预留
/// （由 `reserve_writer` 创建）会被归入同一个分组。
/// 当一个分组的大小达到预设阈值 (`min_group_commit_size`) 或 Manager 触发 `finalize` 时，
/// 该分组会被“密封”（sealed）。密封后，Manager 会处理该分组：
/// - 如果分组内所有预留都已成功提交数据，则将所有数据块按偏移量合并成一个大的 `Bytes` 对象，
///   连同起始偏移量一起发送给成功数据消费者 (`completed_data_tx`)。
/// - 如果分组内包含任何未提交的预留（例如 Agent 被 Drop）或提交失败的预留，
///   则该分组被视为失败。所有已成功提交的数据块和失败预留的信息会被收集起来，
///   通过 `FailedGroupDataTransmission` 发送给失败数据消费者 (`failed_data_tx`)。
///
/// `GroupState` 结构体存储了判断分组状态和处理分组所需的所有信息。
#[derive(Debug)] // 允许使用 {:?} 进行 Debug 打印
pub struct GroupState {
    /// 该分组内所有 **已添加** 的预留（无论最终成功或失败）的预期总大小累加值。
    /// 用于判断分组是否达到 `min_group_commit_size` 阈值。
    pub total_size: usize,
    /// 该分组当前包含的 **活跃且待处理** 的预留 ID 集合。
    /// - 当一个预留被添加到分组时，其 ID 加入此集合。
    /// - 当该预留对应的 Agent 提交数据成功、提交失败或被 Drop 时，其 ID 会从此集合中移除。
    /// `HashSet` 用于快速检查某个预留是否仍处于待处理状态。
    /// 分组只有在该集合为空（所有预留都有了最终状态）且已密封时，才能被最终处理。
    pub reservations: HashSet<ReservationId>,
    /// 存储分组内 **所有** 预留（包括已处理和待处理）的元数据。
    /// Key: `ReservationId`。
    /// Value: 元组 (`AbsoluteOffset` - 预留的起始绝对偏移量, `usize` - 预留的大小)。
    /// 此映射在预留添加到分组时创建，并在处理提交、失败和 Finalize 时用于查找预留的详细信息。
    /// 即使预留已处理（从 `reservations` 移除），其元数据仍保留在此处，直到分组本身被清理。
    pub reservation_metadata: HashMap<ReservationId, (AbsoluteOffset, usize)>,
    /// 存储此分组内已成功提交的数据块。
    /// Key: `AbsoluteOffset` - 提交数据对应的 **预留** 的起始绝对偏移量。
    /// Value: 一个元组，包含：
    ///   - `ReservationId`: 提交成功的预留 ID。
    ///   - `usize`: 该预留的总大小。
    ///   - `BTreeMap<usize, Bytes>`: 该预留对应的所有数据块。
    ///     - Key: `usize` - 数据块在 **预留内部** 的相对偏移量 (从 0 开始)。
    ///     - Value: `Bytes` - 实际的数据块。
    /// 使用 `BTreeMap` (有序映射) 确保：
    /// 1. 外层按预留的起始偏移量排序，方便后续按顺序合并分组数据。
    /// 2. 内层按块的相对偏移量排序，确保块在预留内部按正确顺序合并。
    pub committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
    /// 存储与此分组关联的失败预留信息 (`FailedReservationInfo`) 列表。
    /// 当一个预留因为 Agent Drop、提交大小错误、重复提交或其他原因失败时，
    /// 会创建一个 `FailedReservationInfo` 对象并添加到此列表中。
    /// 这些信息将在分组处理失败时，随同已提交的数据一起发送给失败消费者。
    pub failed_infos: Vec<FailedReservationInfo>,
    /// 标记该分组是否已被“密封”（sealed）。
    /// 初始状态为 `false`。
    /// 当分组的总大小 (`total_size`) 达到 `min_group_commit_size` 阈值，
    /// 或者 Manager 正在执行 `finalize` 操作时，此标记会被设为 `true`。
    /// 一旦密封，通常不再允许新的预留加入此分组（除非是 Finalize 强制处理）。
    /// 密封是分组进入最终处理（成功或失败）的前提条件之一。
    pub is_sealed: bool,
}

// ==================================
// GroupState 的实现块
// ==================================
// 为 GroupState 提供关联函数 (如 new) 和方法，方便地操作其内部状态。

impl GroupState {
    /// 创建一个新的、空的 `GroupState` 实例。
    /// 初始化所有字段为空集合或默认值。
    pub fn new() -> Self {
        GroupState {
            total_size: 0, // 初始大小为 0
            reservations: HashSet::new(), // 初始待处理预留为空
            reservation_metadata: HashMap::new(), // 初始元数据为空
            committed_data: BTreeMap::new(), // 初始已提交数据为空
            failed_infos: Vec::new(), // 初始失败信息为空
            is_sealed: false, // 初始未密封
        }
    }

    /// 向分组添加一个新的预留及其元数据。
    ///
    /// # Arguments
    /// * `res_id` - 新预留的唯一 ID。
    /// * `offset` - 新预留的起始绝对偏移量。
    /// * `size` - 新预留的大小。
    pub fn add_reservation(&mut self, res_id: ReservationId, offset: AbsoluteOffset, size: usize) {
        // 将 ID 加入待处理集合
        self.reservations.insert(res_id);
        // 存储预留的元数据（偏移量和大小）
        self.reservation_metadata.insert(res_id, (offset, size));
        // 累加分组的总大小
        self.total_size += size;
    }

    /// 从 **待处理** 预留集合 (`reservations`) 中移除一个预留 ID。
    ///
    /// 这通常在预留对应的操作完成时（成功提交、失败或 Agent Drop）调用。
    /// **注意：** 此方法 **不会** 移除 `reservation_metadata` 中的条目。
    ///
    /// # Arguments
    /// * `res_id` - 要移除的待处理预留的 ID。
    ///
    /// # Returns
    /// * `bool` - 如果 `res_id` 存在于 `reservations` 集合中并被成功移除，则返回 `true`，否则返回 `false`。
    pub fn remove_pending_reservation(&mut self, res_id: ReservationId) -> bool {
        self.reservations.remove(&res_id) // 只操作 `reservations` 集合
    }

    /// 根据预留 ID 获取其元数据（起始偏移量和大小）。
    ///
    /// # Arguments
    /// * `res_id` - 要查询的预留 ID。
    ///
    /// # Returns
    /// * `Option<(AbsoluteOffset, usize)>` - 如果找到对应的元数据，则返回包含偏移量和大小的元组的克隆；否则返回 `None`。
    pub fn get_reservation_metadata(
        &self,
        res_id: ReservationId,
    ) -> Option<(AbsoluteOffset, usize)> {
        // 从 reservation_metadata 获取引用，然后克隆其内容返回
        self.reservation_metadata.get(&res_id).cloned()
    }

    /// 检查分组的 **待处理** 集合中是否包含指定的预留 ID。
    ///
    /// # Arguments
    /// * `res_id` - 要检查的预留 ID。
    ///
    /// # Returns
    /// * `bool` - 如果 `reservations` 集合包含 `res_id`，则返回 `true`，否则返回 `false`。
    pub fn has_pending_reservation(&self, res_id: ReservationId) -> bool {
        self.reservations.contains(&res_id)
    }

    /// 向分组添加一个预留对应的所有已成功提交的数据块。
    ///
    /// # Arguments
    /// * `offset` - 提交数据对应的 **预留** 的起始绝对偏移量。
    /// * `res_id` - 成功提交的预留 ID。
    /// * `size` - 该预留的总大小。
    /// * `chunks` - 一个 `BTreeMap`，包含该预留的所有数据块，Key 是块在预留内的相对偏移，Value 是 `Bytes` 数据。
    pub fn add_committed_data(
        &mut self,
        offset: AbsoluteOffset, // 预留的起始偏移
        res_id: ReservationId,
        size: usize,
        chunks: BTreeMap<usize, Bytes>, // 注意：Key 是块的相对偏移
    ) {
        // 将预留 ID、大小和数据块集合作为一个元组存入 committed_data
        // Key 是预留的起始绝对偏移量
        self.committed_data.insert(offset, (res_id, size, chunks));
    }

    /// 检查在指定的 **预留** 起始偏移量处是否已有提交的数据。
    ///
    /// # Arguments
    /// * `offset` - 要检查的预留起始绝对偏移量。
    ///
    /// # Returns
    /// * `bool` - 如果 `committed_data` 包含以 `offset` 为键的条目，则返回 `true`，否则返回 `false`。
    pub fn has_committed_data(&self, offset: AbsoluteOffset) -> bool {
        self.committed_data.contains_key(&offset)
    }

    /// 向分组添加一个失败预留的信息记录。
    ///
    /// # Arguments
    /// * `info` - 要添加的 `FailedReservationInfo` 实例。
    pub fn add_failed_info(&mut self, info: FailedReservationInfo) {
        self.failed_infos.push(info);
    }

    /// 判断该分组是否应该被密封。
    /// 条件是：分组尚未密封 (`!is_sealed`) 且其总大小 (`total_size`) 大于或等于指定的最小大小 (`min_size`)。
    ///
    /// # Arguments
    /// * `min_size` - 分组密封的最小大小阈值。
    ///
    /// # Returns
    /// * `bool` - 如果满足密封条件，返回 `true`，否则返回 `false`。
    pub fn should_seal(&self, min_size: usize) -> bool {
        !self.is_sealed && self.total_size >= min_size
    }

    /// 将分组标记为已密封。
    pub fn seal(&mut self) {
        self.is_sealed = true;
    }

    /// 检查分组是否已完成处理的条件。
    /// 条件是：分组已被密封 (`is_sealed`) 且所有预留都已处理完毕（`reservations` 集合为空）。
    ///
    /// # Returns
    /// * `bool` - 如果满足完成条件，返回 `true`，否则返回 `false`。
    pub fn is_complete(&self) -> bool {
        self.is_sealed && self.reservations.is_empty()
    }

    /// 检查分组是否完全为空。
    /// 用于判断分组是否可以被安全地从 Manager 的状态中移除。
    /// 条件是：待处理预留、已提交数据、失败信息 **和** 预留元数据都为空。
    ///
    /// # Returns
    /// * `bool` - 如果所有相关集合都为空，返回 `true`，否则返回 `false`。
    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty() // 检查待处理预留
            && self.committed_data.is_empty() // 检查已提交数据
            && self.failed_infos.is_empty() // 检查失败信息
            && self.reservation_metadata.is_empty() // 检查预留元数据
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // 单元测试移至更合适的地方，但保留 GroupState::new 的基本测试
    #[test]
    fn test_group_state_new() {
        let group_state = GroupState::new();
        assert_eq!(group_state.total_size, 0, "初始 total_size 应为 0");
        assert!(group_state.reservations.is_empty(), "初始 reservations 应为空");
        assert!(group_state.reservation_metadata.is_empty(), "初始 reservation_metadata 应为空");
        assert!(group_state.committed_data.is_empty(), "初始 committed_data 应为空");
        assert!(group_state.failed_infos.is_empty(), "初始 failed_infos 应为空");
        assert!(!group_state.is_sealed, "初始 is_sealed 应为 false");
    }
}
