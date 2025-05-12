//! 分组生命周期管理模块
//! 定义了用于管理分组创建、状态更新、密封等操作的 trait 和实现。

use crate::manager::state::GroupState;
use crate::types::{AbsoluteOffset, GroupId, ManagerError, ReservationId};
// 引入 ManagerError
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// `GroupLifecycleManager` Trait
///
/// 定义了管理数据分组（`GroupState`）生命周期的接口。
/// 这个 Trait 的实现者负责：
/// - 为新的预留决定归属的分组（查找现有活动分组或创建新分组）。
/// - 跟踪分组的状态（大小、包含的预留、是否密封）。
/// - 根据配置的阈值或外部指令（如 Finalize）密封分组。
/// - 提供对分组状态的访问和修改接口。
/// - 管理活动分组的概念（即当前接收新预留的分组）。
/// - 提供移除和重新插入分组状态的机制（用于处理和恢复）。
/// - 提供清理无效预留的机制。
pub trait GroupLifecycleManager: Send + 'static {
    /// 为一个新的预留查找或创建一个合适的分组。
    ///
    /// 逻辑通常是：
    /// 1. 检查是否存在活动分组 (`active_group_id`)。
    /// 2. 如果存在且未密封，则将新预留添加到该活动分组。
    /// 3. 如果存在但已密封，或不存在活动分组，则创建一个新的分组，并将新预留作为其第一个成员。
    ///
    /// # Arguments
    /// * `res_id` - 新预留的唯一 ID。
    /// * `offset` - 新预留的起始绝对偏移量。
    /// * `size` - 新预留的大小。
    ///
    /// # Returns
    /// 一个元组 `(GroupId, bool)`:
    ///   - `GroupId`: 新预留所属的分组 ID (可能是现有活动分组或新创建的分组)。
    ///   - `bool`: 一个布尔值，`true` 表示创建了新分组，`false` 表示加入了现有活动分组。
    fn find_or_create_group_for_reservation(
        &mut self,
        res_id: ReservationId,
        offset: AbsoluteOffset,
        size: usize,
    ) -> (GroupId, /* created_new */ bool);

    /// 获取指定 `GroupId` 对应的分组状态 (`GroupState`) 的可变引用。
    /// 如果分组不存在，则返回 `None`。
    fn get_group_mut(&mut self, group_id: GroupId) -> Option<&mut GroupState>;

    /// 获取指定 `GroupId` 对应的分组状态 (`GroupState`) 的不可变引用。
    /// 如果分组不存在，则返回 `None`。
    fn get_group_ref(&self, group_id: GroupId) -> Option<&GroupState>;

    // /// 检查指定分组是否已密封。（可选方法，根据需要启用）
    // /// 如果分组不存在，则返回错误。
    // fn is_group_sealed(&self, group_id: GroupId) -> Result<bool, ManagerError>;

    /// 根据配置的最小提交大小阈值，尝试密封指定的分组。
    ///
    /// 检查指定 `group_id` 的分组：
    /// - 是否存在。
    /// - 是否尚未密封。
    /// - 其总大小 (`total_size`) 是否达到或超过 `min_group_commit_size` 阈值。
    /// 如果所有条件满足，则将该分组标记为已密封 (`is_sealed = true`)。
    /// 如果被密封的分组恰好是当前活动分组，则清除 `active_group_id`。
    ///
    /// # Arguments
    /// * `group_id` - 要尝试密封的分组 ID。
    ///
    /// # Returns
    /// * `Ok(true)`: 如果分组成功被此调用密封。
    /// * `Ok(false)`: 如果分组未达到密封条件或之前已被密封。
    /// * `Err(ManagerError::GroupNotFound)`: 如果指定 `group_id` 的分组不存在。
    fn seal_group_if_threshold_reached(&mut self, group_id: GroupId) -> Result<bool, ManagerError>;

    /// 强制将指定分组标记为已密封，无论其大小如何。
    /// 主要用于 `Finalize` 过程，确保所有分组都进入可处理状态。
    /// 如果分组已被密封，则此操作无效。
    /// 如果强制密封的分组恰好是当前活动分组，则清除 `active_group_id`。
    ///
    /// # Arguments
    /// * `group_id` - 要强制密封的分组 ID。
    ///
    /// # Returns
    /// * `Ok(())`: 如果分组存在并成功（或已经）密封。
    /// * `Err(ManagerError::GroupNotFound)`: 如果指定 `group_id` 的分组不存在。
    fn force_seal_group(&mut self, group_id: GroupId) -> Result<(), ManagerError>;

    /// 从管理器中移除指定 `group_id` 的分组，并返回其拥有的 `GroupState`。
    ///
    /// 这个操作通常在准备处理一个已完成（密封且无待处理预留）或失败的分组时调用，
    /// 将分组状态的所有权转移给处理逻辑（如 `GroupDataProcessor` 或 `FinalizationHandler`）。
    /// 如果被移除的分组是当前活动分组，则清除 `active_group_id`。
    ///
    /// # Arguments
    /// * `group_id` - 要移除并获取其状态的分组 ID。
    ///
    /// # Returns
    /// * `Some(GroupState)`: 如果分组存在并成功移除，返回其状态。
    /// * `None`: 如果指定 `group_id` 的分组不存在。
    fn take_group(&mut self, group_id: GroupId) -> Option<GroupState>;

    /// 将一个之前被 `take_group` 移除的分组状态重新插入管理器。
    ///
    /// 这个操作主要用于错误恢复场景。例如，当 `try_process_taken_group_state` 尝试处理分组失败时，
    /// 它会将获取到的 `GroupState` 通过此方法放回 `GroupLifecycleManager`，
    /// 以便后续的 `Finalize` 过程可以再次尝试处理或将其报告为失败。
    /// 重新插入的分组 **不会** 自动成为活动分组。
    ///
    /// # Arguments
    /// * `group_id` - 要重新插入的分组的 ID。
    /// * `group_state` - 要重新插入的分组状态。
    fn insert_group(&mut self, group_id: GroupId, group_state: GroupState);

    // /// 获取当前活动分组的 ID。（可选方法，根据需要启用）
    // fn get_active_group_id(&self) -> Option<GroupId>;

    /// 在创建了一个新分组后，更新活动分组 ID (`active_group_id`)。
    ///
    /// 逻辑是：检查新创建的分组 (`new_group_id`) 是否存在且 **未** 被立即密封。
    /// 如果是，则将 `active_group_id` 设置为 `new_group_id`。
    /// 如果新分组被立即密封（例如，其第一个预留就达到了阈值），则清除 `active_group_id` (设置为 `None`)。
    ///
    /// # Arguments
    /// * `new_group_id` - 刚刚被创建的分组的 ID。
    fn update_active_group_after_creation(&mut self, new_group_id: GroupId);

    /// 当一个分组被密封或被 `take_group` 移除时，检查它是否是当前活动分组。
    /// 如果是，则清除 `active_group_id` (设置为 `None`)，因为活动分组必须是未密封且存在的。
    ///
    /// # Arguments
    /// * `group_id_sealed_or_removed` - 被密封或移除的分组的 ID。
    fn clear_active_group_id_if_matches(&mut self, group_id_sealed_or_removed: GroupId);

    /// 获取当前管理器中所有存在的分组的 ID 列表。
    /// 主要用于 `Finalize` 过程，迭代处理所有剩余分组。
    fn all_group_ids(&self) -> Vec<GroupId>;

    /// 清理指定分组内的一个无效预留信息。
    ///
    /// 这个方法通常在 Manager 处理 `Reserve` 请求时，向 Agent 发送预留结果 (`Ok(SubmitAgent)`) 失败后调用。
    /// 发送失败意味着 Agent 无法获得 `SubmitAgent`，该预留永远不会被提交或 Drop。
    /// 因此，需要从对应的 `GroupState` 中移除此预留的记录：
    /// 1. 从 `reservation_metadata` 中移除预留条目。
    /// 2. 从 `reservations` (待处理预留集合) 中移除预留 ID。
    /// 3. 从分组的 `total_size` 中减去该预留的大小。
    /// 如果清理后分组变为空（没有任何预留记录），则将该分组自身从管理器中移除。
    ///
    /// # Arguments
    /// * `res_id` - 需要清理的无效预留的 ID。
    /// * `group_id` - 该预留所属的分组 ID。
    fn cleanup_reservation_in_group(&mut self, res_id: ReservationId, group_id: GroupId);
}

/// `GroupLifecycleManager` Trait 的默认实现。
/// 使用 `HashMap` 存储分组状态，并维护一个自增的 `next_group_id` 和可选的 `active_group_id`。
pub struct DefaultGroupLifecycleManager {
    /// 存储所有分组状态的 HashMap，键是 GroupId。
    groups: HashMap<GroupId, GroupState>,
    /// 用于生成下一个新分组的 ID。
    next_group_id: GroupId,
    /// 当前活动分组的 ID。新预留会尝试加入此分组（如果存在且未密封）。
    active_group_id: Option<GroupId>,
    /// 分组自动密封的大小阈值 (字节)。
    min_group_commit_size: usize,
}

impl DefaultGroupLifecycleManager {
    /// 创建一个新的 `DefaultGroupLifecycleManager` 实例。
    ///
    /// # Arguments
    /// * `min_group_commit_size` - 触发分组自动密封的最小字节数阈值。
    pub fn new(min_group_commit_size: usize) -> Self {
        info!(
            "(GroupLifecycle) 初始化 DefaultGroupLifecycleManager，最小分组提交大小: {}",
            min_group_commit_size
        );
        DefaultGroupLifecycleManager {
            groups: HashMap::new(),
            next_group_id: 0,
            active_group_id: None,
            min_group_commit_size,
        }
    }

    /// 内部辅助函数：创建一个新的分组，添加初始预留，并将其插入 `groups` Map。
    /// 还会检查新分组是否因第一个预留就达到阈值而需要立即密封。
    ///
    /// # Arguments
    /// * `initial_res_id` - 新分组的第一个预留的 ID。
    /// * `initial_offset` - 第一个预留的偏移量。
    /// * `initial_size` - 第一个预留的大小。
    ///
    /// # Returns
    /// 新创建的分组的 `GroupId`。
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
            if let Some(group) = self.groups.get_mut(&active_id) {
                if group.is_sealed {
                    debug!(
                        "(GroupLifecycle) 活动分组 {} 已密封，为 Res {} (Offset {}, Size {}) 创建新分组",
                        active_id, res_id, offset, size
                    );
                    let new_group_id = self.internal_create_new_group(res_id, offset, size);
                    (new_group_id, true)
                } else {
                    group.add_reservation(res_id, offset, size);
                    debug!(
                        "(GroupLifecycle) Res {} (Offset {}, Size {}) 加入活动分组 {}, 更新后大小 {}, Res 数量 {}, Meta 数量 {}",
                        res_id, offset, size, active_id, group.total_size, group.reservations.len(), group.reservation_metadata.len()
                    );
                    (active_id, false)
                }
            } else {
                warn!("(GroupLifecycle) 内部状态警告：活动分组ID {} 存在，但找不到对应分组。将为 Res {} 创建新分组。", active_id, res_id);
                self.active_group_id = None;
                let new_group_id = self.internal_create_new_group(res_id, offset, size);
                (new_group_id, true)
            }
        } else {
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

    fn seal_group_if_threshold_reached(&mut self, group_id: GroupId) -> Result<bool, ManagerError> {
        let min_size = self.min_group_commit_size;
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(ManagerError::GroupNotFound(group_id))?;

        if group.should_seal(min_size) {
            info!(
                "(GroupLifecycle) 分组 {} (大小 {}) 达到或超过阈值 {}，将被密封。",
                group_id, group.total_size, min_size
            );
            group.seal();
            self.clear_active_group_id_if_matches(group_id);
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
            self.clear_active_group_id_if_matches(group_id);
        }
        Ok(())
    }

    fn take_group(&mut self, group_id: GroupId) -> Option<GroupState> {
        let group_state = self.groups.remove(&group_id);
        if group_state.is_some() {
            self.clear_active_group_id_if_matches(group_id);
        }
        group_state
    }

    fn insert_group(&mut self, group_id: GroupId, group_state: GroupState) {
        self.groups.insert(group_id, group_state);
    }

    fn update_active_group_after_creation(&mut self, new_group_id: GroupId) {
        if let Some(new_group) = self.groups.get(&new_group_id) {
            if !new_group.is_sealed {
                self.active_group_id = Some(new_group_id);
                debug!("(GroupLifecycle) 新分组 {} 设置为活动分组", new_group_id);
            } else {
                self.active_group_id = None;
                debug!(
                    "(GroupLifecycle) 新分组 {} 创建时即被密封，无活动分组",
                    new_group_id
                );
            }
        } else {
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

            if group.reservations.remove(&res_id) {
                debug!(
                    "(GroupLifecycle) 从分组 {} 的 reservations 集合中移除 Res {}",
                    group_id, res_id
                );
            } else {
                if size_to_remove > 0 {
                    warn!("(GroupLifecycle) 清理失败预留 {} 时在其分组 {} 的 reservations 集合中未找到记录 (元数据已移除)", res_id, group_id);
                }
            }

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
            return;
        }

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
