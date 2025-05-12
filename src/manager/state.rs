use crate::types::{AbsoluteOffset, ReservationId};
use crate::FailedReservationInfo;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Manager 内部跟踪的单个分组的状态。
/// 分组用于将逻辑上相邻或相关的预留聚合在一起处理。
#[derive(Debug)] // 允许 Debug 打印
pub struct GroupState {
    /// 该分组内所有预留的预期总大小累加值。
    pub total_size: usize,
    /// 该分组当前包含的 **待处理** 预留的 ID 集合 (`HashSet` 用于快速查找)。
    /// 当预留被提交或失败时，会从此集合中移除。
    pub reservations: HashSet<ReservationId>,
    /// 存储分组内 **所有** 预留（无论状态如何）的元数据 (ID -> (Offset, Size))。
    /// 用于验证提交、处理失败和 Finalize。
    pub reservation_metadata: HashMap<ReservationId, (AbsoluteOffset, usize)>,
    /// 存储已成功提交预留的数据和元信息。
    /// Key: 预留的起始绝对偏移量。
    /// Value: 元组 (`ReservationId`, `usize` (预留大小), `BTreeMap<usize (块相对偏移), Bytes>` (该预留对应的所有数据块))。
    /// 外层 BTreeMap 按预留偏移排序，内层 BTreeMap 按块在预留内的相对偏移排序。
    pub committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
    /// 存储此分组内记录到的失败预留信息 (`FailedReservationInfo`)。
    pub failed_infos: Vec<FailedReservationInfo>,
    /// 标记该分组是否已“密封”。
    /// 密封后，通常不再接受新的预留加入此分组 (除非是 Manager 强制密封)。
    /// 当分组大小达到阈值 (`min_group_commit_size`) 或 Manager Finalizing 时会被密封。
    pub is_sealed: bool,
}

// ==================================
// GroupState 辅助函数
// ==================================
// 将一些 GroupState 的操作逻辑封装成方法，提高内聚性

impl GroupState {
    /// 创建一个新的 GroupState
    pub fn new() -> Self {
        GroupState {
            total_size: 0,
            reservations: HashSet::new(),
            reservation_metadata: HashMap::new(),
            committed_data: BTreeMap::new(),
            failed_infos: Vec::new(),
            is_sealed: false,
        }
    }

    /// 向分组添加一个新的预留
    pub fn add_reservation(&mut self, res_id: ReservationId, offset: AbsoluteOffset, size: usize) {
        self.reservations.insert(res_id);
        self.reservation_metadata.insert(res_id, (offset, size));
        self.total_size += size;
    }

    /// 移除一个 **待处理** 预留 ID （通常在提交成功或失败时调用）
    /// **修改点**: 只从 `reservations` 集合移除，不再碰 `reservation_metadata`。
    /// 返回是否成功从集合中移除。
    pub fn remove_pending_reservation(&mut self, res_id: ReservationId) -> bool {
        self.reservations.remove(&res_id)
    }

    /// 获取预留的元数据 (Offset, Size)
    pub fn get_reservation_metadata(
        &self,
        res_id: ReservationId,
    ) -> Option<(AbsoluteOffset, usize)> {
        self.reservation_metadata.get(&res_id).cloned() // 返回克隆的数据
    }

    /// 检查分组中是否包含指定的待处理预留
    pub fn has_pending_reservation(&self, res_id: ReservationId) -> bool {
        self.reservations.contains(&res_id)
    }

    /// 向分组添加已提交的数据
    pub fn add_committed_data(
        &mut self,
        offset: AbsoluteOffset, // 预留的起始偏移
        res_id: ReservationId,
        size: usize,
        chunks: BTreeMap<AbsoluteOffset, Bytes>, // 块相对偏移 -> 块数据
    ) {
        self.committed_data.insert(offset, (res_id, size, chunks));
    }

    /// 检查在指定偏移处是否已有提交的数据
    pub fn has_committed_data(&self, offset: AbsoluteOffset) -> bool {
        self.committed_data.contains_key(&offset)
    }

    /// 向分组添加失败预留的信息
    pub fn add_failed_info(&mut self, info: FailedReservationInfo) {
        self.failed_infos.push(info);
    }

    /// 检查分组是否应该被密封
    pub fn should_seal(&self, min_size: usize) -> bool {
        !self.is_sealed && self.total_size >= min_size
    }

    /// 密封分组
    pub fn seal(&mut self) {
        self.is_sealed = true;
    }

    /// 检查分组是否完成（已密封且无待处理预留）
    pub fn is_complete(&self) -> bool {
        self.is_sealed && self.reservations.is_empty()
    }

    /// 检查分组是否为空（没有待处理、已提交、失败信息 **和元数据**）
    /// **修改点**: 加入对 metadata 的检查。
    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
            && self.committed_data.is_empty()
            && self.failed_infos.is_empty()
            && self.reservation_metadata.is_empty() // 确保元数据也空了
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // 测试 GroupState 的辅助函数 (已移至 manager/helpers.rs 的测试中)
    // 但 GroupState 的 new 方法可以在这里简单测试
    #[test]
    fn test_group_state_new() {
        let group_state = GroupState::new();
        assert_eq!(group_state.total_size, 0);
        assert!(group_state.reservations.is_empty());
        assert!(group_state.reservation_metadata.is_empty());
        assert!(group_state.committed_data.is_empty());
        assert!(group_state.failed_infos.is_empty());
        assert!(!group_state.is_sealed); // 初始未密封
    }
}
