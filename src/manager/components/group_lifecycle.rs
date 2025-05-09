//! 分组生命周期管理模块
//! 定义了用于管理分组创建、状态更新、密封等操作的 trait 和实现。

use crate::manager::state::GroupState;
use crate::types::{AbsoluteOffset, GroupId, ManagerError, ReservationId};
// 引入 ManagerError
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// GroupLifecycleManager Trait
/// 定义了管理分组生命周期的接口。
pub(crate) trait GroupLifecycleManager: Send {
    /// 为新的预留查找或创建一个分组。
    ///
    /// # Arguments
    /// * `res_id` - 初始预留的ID。
    /// * `offset` - 初始预留的绝对偏移量。
    /// * `size` - 初始预留的大小。
    ///
    /// # Returns
    /// 元组 `(GroupId, bool)`，分别表示目标分组ID和是否创建了新分组的布尔值。
    fn find_or_create_group_for_reservation(
        &mut self,
        res_id: ReservationId,
        offset: AbsoluteOffset,
        size: usize,
    ) -> (GroupId, bool /* created_new */);

    /// 获取指定 GroupID 对应的分组状态的可变引用。
    fn get_group_mut(&mut self, group_id: GroupId) -> Option<&mut GroupState>;

    /// 获取指定 GroupID 对应的分组状态的不可变引用。
    fn get_group_ref(&self, group_id: GroupId) -> Option<&GroupState>;

    // /// 检查指定分组是否已密封。
    // /// 如果分组不存在，则返回错误。
    // fn is_group_sealed(&self, group_id: GroupId) -> Result<bool, ManagerError>;

    /// 根据配置的最小提交大小阈值尝试密封指定分组。
    /// 如果分组大小达到阈值且之前未密封，则将其密封。
    /// 如果分组因此操作而被密封，且该分组是当前活动分组，则清除活动分组ID。
    ///
    /// # Returns
    /// `Ok(true)` 如果分组被密封，`Ok(false)` 如果未达到密封条件或已密封。
    /// `Err(ManagerError::GroupNotFound)` 如果分组不存在。
    fn seal_group_if_threshold_reached(&mut self, group_id: GroupId) -> Result<bool, ManagerError>;

    /// 强制密封一个分组（通常在 Finalize 期间使用）。
    /// 如果分组不存在，则返回错误。
    fn force_seal_group(&mut self, group_id: GroupId) -> Result<(), ManagerError>;

    /// 从管理器中移除一个分组，并返回其状态（如果存在）。
    /// 此操作通常在准备处理一个已完成或失败的分组时调用。
    /// 如果移除的是活动分组，则清除活动分组ID。
    fn take_group(&mut self, group_id: GroupId) -> Option<GroupState>;

    /// 将一个分组状态重新插入管理器。
    /// 此操作通常在尝试处理分组失败后，需要将分组状态放回以待后续处理（如Finalize）时调用。
    fn insert_group(&mut self, group_id: GroupId, group_state: GroupState);

    // /// 获取当前活动分组的ID。
    // fn get_active_group_id(&self) -> Option<GroupId>;

    /// 当一个新分组被创建后，根据其是否被立即密封来更新活动分组ID。
    /// 如果新创建的分组未密封，则将其设为活动分组。否则，清除活动分组ID。
    fn update_active_group_after_creation(&mut self, new_group_id: GroupId);

    /// 当一个分组被密封或移除时，如果它是当前活动分组，则清除活动分组ID。
    fn clear_active_group_id_if_matches(&mut self, group_id_sealed_or_removed: GroupId);

    /// 获取所有当前存在的分组的ID列表。
    fn all_group_ids(&self) -> Vec<GroupId>;

    /// 清理指定分组内的一个无效预留。
    /// 这通常在Reserve请求的回复发送失败时调用。
    /// 会从分组的元数据和待处理预留集合中移除该预留，并更新分组的总大小。
    /// 如果分组因此变空，则将其移除。
    fn cleanup_reservation_in_group(&mut self, res_id: ReservationId, group_id: GroupId);
}

/// `GroupLifecycleManager` Trait 的默认实现。
pub(crate) struct DefaultGroupLifecycleManager {
    groups: HashMap<GroupId, GroupState>,
    next_group_id: GroupId,
    active_group_id: Option<GroupId>,
    min_group_commit_size: usize,
}

impl DefaultGroupLifecycleManager {
    /// 创建一个新的 `DefaultGroupLifecycleManager`。
    ///
    /// # Arguments
    /// * `min_group_commit_size` - 触发分组密封的最小字节数阈值。
    pub fn new(min_group_commit_size: usize) -> Self {
        DefaultGroupLifecycleManager {
            groups: HashMap::new(),
            next_group_id: 0,
            active_group_id: None,
            min_group_commit_size,
        }
    }

    /// 内部辅助函数：创建一个新的分组状态并将其添加到 `groups` Map 中。
    /// 返回新创建的分组 ID。
    fn internal_create_new_group(
        &mut self,
        initial_res_id: ReservationId,
        initial_offset: AbsoluteOffset,
        initial_size: usize,
    ) -> GroupId {
        let group_id = self.next_group_id;
        self.next_group_id += 1;

        let mut new_group = GroupState::new();
        new_group.add_reservation(initial_res_id, initial_offset, initial_size);

        debug!(
            "(GroupLifecycle) 创建新分组 {}, 初始成员 Res {}, 初始大小 {}",
            group_id, initial_res_id, initial_size
        );

        // 检查新创建的分组是否立即达到密封阈值
        if new_group.should_seal(self.min_group_commit_size) {
            info!(
                "(GroupLifecycle) 新分组 {} 创建时 (大小 {}) 即达到或超过阈值 {}，将被立即密封。",
                group_id, new_group.total_size, self.min_group_commit_size
            );
            new_group.seal();
        }

        self.groups.insert(group_id, new_group);
        group_id
    }
}

impl GroupLifecycleManager for DefaultGroupLifecycleManager {
    fn find_or_create_group_for_reservation(
        &mut self,
        res_id: ReservationId,
        offset: AbsoluteOffset,
        size: usize,
    ) -> (GroupId, bool /* created_new */) {
        if let Some(active_id) = self.active_group_id {
            // 尝试获取活动分组的可变引用
            if let Some(group) = self.groups.get_mut(&active_id) {
                // 如果活动分组已密封，则需要创建新分组
                if group.is_sealed {
                    debug!(
                        "(GroupLifecycle) 活动分组 {} 已密封，为 Res {} (Offset {}, Size {}) 创建新分组",
                        active_id, res_id, offset, size
                    );
                    let new_group_id = self.internal_create_new_group(res_id, offset, size);
                    (new_group_id, true)
                } else {
                    // 加入现有活动分组
                    group.add_reservation(res_id, offset, size);
                    debug!(
                        "(GroupLifecycle) Res {} (Offset {}, Size {}) 加入活动分组 {}, 更新后大小 {}, Res 数量 {}, Meta 数量 {}",
                        res_id, offset, size, active_id, group.total_size, group.reservations.len(), group.reservation_metadata.len()
                    );
                    (active_id, false)
                }
            } else {
                // 活动ID存在但分组不存在，这通常是一个内部错误状态。
                // 按照鲁棒性原则，我们记录警告并创建一个新分组。
                warn!("(GroupLifecycle) 内部状态警告：活动分组ID {} 存在，但找不到对应分组。将为 Res {} 创建新分组。", active_id, res_id);
                let new_group_id = self.internal_create_new_group(res_id, offset, size);
                (new_group_id, true)
            }
        } else {
            // 没有活动分组，创建新的
            debug!(
                "(GroupLifecycle) 没有活动分组，为 Res {} (Offset {}, Size {}) 创建新分组",
                res_id, offset, size
            );
            let new_group_id = self.internal_create_new_group(res_id, offset, size);
            (new_group_id, true)
        }
    }

    fn get_group_mut(&mut self, group_id: GroupId) -> Option<&mut GroupState> {
        self.groups.get_mut(&group_id)
    }

    fn get_group_ref(&self, group_id: GroupId) -> Option<&GroupState> {
        self.groups.get(&group_id)
    }

    // fn is_group_sealed(&self, group_id: GroupId) -> Result<bool, ManagerError> {
    //     self.groups.get(&group_id)
    //         .map(|g| g.is_sealed)
    //         .ok_or(ManagerError::GroupNotFound(group_id))
    // }

    fn seal_group_if_threshold_reached(&mut self, group_id: GroupId) -> Result<bool, ManagerError> {
        let min_size = self.min_group_commit_size;
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(ManagerError::GroupNotFound(group_id))?;

        if group.should_seal(min_size) {
            // should_seal 内部会检查 !is_sealed
            info!(
                "(GroupLifecycle) 分组 {} (大小 {}) 达到或超过阈值 {}，将被密封。",
                group_id, group.total_size, min_size
            );
            group.seal();
            self.clear_active_group_id_if_matches(group_id); // 如果密封的是活动分组，清除
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn force_seal_group(&mut self, group_id: GroupId) -> Result<(), ManagerError> {
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(ManagerError::GroupNotFound(group_id))?;
        if !group.is_sealed {
            debug!("(GroupLifecycle) Finalize: 强制密封 Group {}", group_id);
            group.seal();
            // 强制密封时也可能需要清除活动分组ID，如果它恰好是活动分组
            self.clear_active_group_id_if_matches(group_id);
        }
        Ok(())
    }

    fn take_group(&mut self, group_id: GroupId) -> Option<GroupState> {
        let group_state = self.groups.remove(&group_id);
        if group_state.is_some() {
            // 如果移除的是活动分组，则清除 active_group_id
            self.clear_active_group_id_if_matches(group_id);
        }
        group_state
    }

    fn insert_group(&mut self, group_id: GroupId, group_state: GroupState) {
        self.groups.insert(group_id, group_state);
        // 重新插入分组后，不自动将其设为活动分组，除非有特定逻辑要求
    }

    // fn get_active_group_id(&self) -> Option<GroupId> {
    //     self.active_group_id
    // }

    fn update_active_group_after_creation(&mut self, new_group_id: GroupId) {
        // 检查新创建的分组是否存在且未密封
        if let Some(new_group) = self.groups.get(&new_group_id) {
            if !new_group.is_sealed {
                self.active_group_id = Some(new_group_id);
                debug!("(GroupLifecycle) 新分组 {} 设置为活动分组", new_group_id);
            } else {
                // 新创建的分组立即被密封了，则当前没有活动分组
                self.active_group_id = None;
                debug!(
                    "(GroupLifecycle) 新分组 {} 创建时即被密封，无活动分组",
                    new_group_id
                );
            }
        } else {
            // 这种情况理论上不应发生，因为 new_group_id 是刚创建的
            warn!(
                "(GroupLifecycle) 尝试更新活动分组ID时，新分组 {} 未找到。",
                new_group_id
            );
            self.active_group_id = None;
        }
    }

    fn clear_active_group_id_if_matches(&mut self, group_id_sealed_or_removed: GroupId) {
        if self.active_group_id == Some(group_id_sealed_or_removed) {
            self.active_group_id = None;
            debug!(
                "(GroupLifecycle) 活动分组 ID 已清除 (因分组 {} 被密封/移除)",
                group_id_sealed_or_removed
            );
        }
    }

    fn all_group_ids(&self) -> Vec<GroupId> {
        self.groups.keys().cloned().collect()
    }

    fn cleanup_reservation_in_group(&mut self, res_id: ReservationId, group_id: GroupId) {
        warn!(
            "(GroupLifecycle) 清理因回复失败导致的无效 Reservation {} (原属 Group {})",
            res_id, group_id
        );

        let mut group_became_empty = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            let mut size_to_remove = 0;
            // 从元数据获取大小并移除
            if let Some((_offset, size)) = group.reservation_metadata.remove(&res_id) {
                size_to_remove = size;
                debug!(
                    "(GroupLifecycle) 从分组 {} 的元数据中移除 Res {} (因 Reserve 回复失败)",
                    group_id, res_id
                );
            } else {
                warn!(
                    "(GroupLifecycle) 清理失败预留 {} 时在其分组 {} 的元数据中未找到记录",
                    res_id, group_id
                );
            }

            // 只从待处理集合中移除
            if group.reservations.remove(&res_id) {
                debug!(
                    "(GroupLifecycle) 从分组 {} 的 reservations 集合中移除 Res {}",
                    group_id, res_id
                );
            } else {
                if size_to_remove > 0 {
                    // 只有在元数据确实存在时才警告
                    warn!("(GroupLifecycle) 清理失败预留 {} 时在其分组 {} 的 reservations 集合中未找到记录 (元数据已移除)", res_id, group_id);
                }
            }

            // 更新分组总大小 (如果确实移除了元数据)
            if size_to_remove > 0 {
                group.total_size = group.total_size.saturating_sub(size_to_remove);
                debug!(
                    "(GroupLifecycle) 分组 {} 因预留 {} (回复失败) 而更新大小，移除 {} 字节，剩余大小 {}",
                    group_id, res_id, size_to_remove, group.total_size
                );
            }

            if group.is_empty() {
                group_became_empty = true;
            }
        } else {
            warn!(
                "(GroupLifecycle) 清理失败预留 {} 时找不到其所属分组 {}",
                res_id, group_id
            );
            return; // 分组不存在，直接返回
        }

        // 如果分组变空，则移除分组并清理活动ID
        if group_became_empty {
            debug!(
                "(GroupLifecycle) 分组 {} 因预留 {} (回复失败) 而变空，将移除分组。",
                group_id, res_id
            );
            self.groups.remove(&group_id);
            self.clear_active_group_id_if_matches(group_id);
        }
    }
}
