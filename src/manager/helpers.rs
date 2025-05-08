//! Manager 辅助函数实现

use super::{AbsoluteOffset, GroupId, GroupState, Manager, ManagerError, ReservationId};
use crate::types::{FailedReservationInfo, SuccessfulGroupData};
// 引入所需类型
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
// 确保导入 HashMap
use tracing::{debug, error, info, trace, warn};

// ==================================
// Manager 实现块 - 辅助函数
// ==================================

impl Manager {
    /// 创建一个新的分组状态并将其添加到 `groups` Map 中
    /// 返回新创建的分组 ID
    pub(super) fn create_new_group(
        &mut self,
        initial_res_id: ReservationId,
        initial_offset: AbsoluteOffset,
        initial_size: usize,
    ) -> GroupId {
        let group_id = self.next_group_id;
        self.next_group_id += 1;

        // 使用 GroupState::new 创建新分组
        let mut new_group = GroupState::new();
        // 添加初始预留信息
        new_group.add_reservation(initial_res_id, initial_offset, initial_size);

        debug!(
            "(Manager) 创建新分组 {}, 初始成员 Res {}, 初始大小 {}",
            group_id, initial_res_id, initial_size
        );

        // 检查新创建的分组是否立即达到密封阈值
        if new_group.should_seal(self.min_group_commit_size) {
            info!(
                "(Manager) 新分组 {} 创建时 (大小 {}) 即达到或超过阈值 {}，将被立即密封。",
                group_id, new_group.total_size, self.min_group_commit_size
            );
            new_group.seal();
        }

        // 将新分组插入 Manager 的分组集合中
        self.groups.insert(group_id, new_group);
        group_id
    }

    /// 清理因发送 Reserve 回复失败而创建的无效预留状态及其关联信息
    pub(super) fn cleanup_failed_reservation(&mut self, res_id: ReservationId, group_id: GroupId) {
        warn!(
            "(Manager) 清理因回复失败导致的无效 Reservation {} (原属 Group {})",
            res_id, group_id
        );

        if let Some(group) = self.groups.get_mut(&group_id) {
            let mut size_to_remove = 0;
            // **修改点**: 仍然需要从元数据获取大小并移除，因为这个预留彻底无效了
            if let Some((_offset, size)) = group.reservation_metadata.remove(&res_id) {
                size_to_remove = size;
                debug!(
                    "(Manager) 从分组 {} 的元数据中移除 Res {} (因 Reserve 回复失败)",
                    group_id, res_id
                );
            } else {
                warn!(
                    "(Manager) 清理失败预留 {} 时在其分组 {} 的元数据中未找到记录",
                    res_id, group_id
                );
            }

            // **修改点**: 只从待处理集合中移除
            if group.reservations.remove(&res_id) {
                debug!(
                    "(Manager) 从分组 {} 的 reservations 集合中移除 Res {}",
                    group_id, res_id
                );
            } else {
                if size_to_remove > 0 {
                    // 只有在元数据确实存在时才警告
                    warn!("(Manager) 清理失败预留 {} 时在其分组 {} 的 reservations 集合中未找到记录 (元数据已移除)", res_id, group_id);
                }
            }

            // 更新分组总大小 (如果确实移除了元数据)
            if size_to_remove > 0 {
                group.total_size = group.total_size.saturating_sub(size_to_remove);
                debug!(
                    "(Manager) 分组 {} 因预留 {} (回复失败) 而更新大小，移除 {} 字节，剩余大小 {}",
                    group_id, res_id, size_to_remove, group.total_size
                );
            }

            // 检查分组是否变空 (现在也检查元数据是否为空)
            if group.is_empty() {
                // is_empty 内部会检查所有集合包括 metadata
                debug!(
                    "(Manager) 分组 {} 因预留 {} (回复失败) 而变空，将移除分组。",
                    group_id, res_id
                );
                self.groups.remove(&group_id);
                if self.active_group_id == Some(group_id) {
                    self.active_group_id = None;
                    debug!("(Manager) 活动分组 ID 已清除 (因分组 {} 被移除)", group_id);
                }
            }
        } else {
            warn!(
                "(Manager) 清理失败预留 {} 时找不到其所属分组 {}",
                res_id, group_id
            );
        }
    }

    /// 检查并处理一个已完成（所有预留都已 Commit 或 Failed）且已密封的分组
    /// 成功则发送数据到 completed_data_tx，失败则准备失败信息（但不在此处发送到 failed_data_tx）
    /// 返回 Ok(bool) 表示是否成功处理了分组 (true=成功发送/空分组, false=分组不存在/状态错误)
    /// 返回 Err(ManagerError) 表示分组处理失败（例如不连续、包含失败信息等），此时分组状态会被保留以便 finalize 处理。
    pub(super) async fn check_and_process_completed_group(
        &mut self,
        group_id: GroupId,
    ) -> Result<bool, ManagerError> {
        // 1. 尝试获取分组状态的可变引用
        // **修改点**: 使用 entry API 可以更原子地处理查找和后续操作，
        // 但这里保持 get_mut 然后后续处理，需要小心借用。
        if !self.groups.contains_key(&group_id) {
            warn!("(Manager) [check_and_process] 找不到分组 {}", group_id);
            return Ok(false); // 分组不存在，未处理
        }

        // 先进行只读检查，确认分组是否真的符合处理条件
        // 这避免了不必要的可变借用和数据 take
        let can_process = {
            let group = self.groups.get(&group_id).unwrap(); // 此时确认存在
            group.is_complete()
        };

        if !can_process {
            // 如果分组实际上未完成 (例如并发修改?), 则不处理
            warn!(
                "(Manager) [check_and_process] 尝试处理未完成的分组 {}",
                group_id
            );
            // 这种情况不返回 Err，因为分组可能稍后完成，返回 Ok(false) 表示当前未处理
            return Ok(false);
        }

        // 确认可以处理，现在获取可变引用并 take 数据
        // 注意：take 之后如果出错需要放回或丢弃
        let mut group_state = match self.groups.remove(&group_id) {
            Some(state) => state,
            None => {
                // 在检查和移除之间被并发移除了？
                error!(
                    "(Manager) [check_and_process] 在尝试移除分组 {} 时发现其已被移除",
                    group_id
                );
                return Ok(false); // 无法处理
            }
        };

        // 3. 从取出的 group_state 中提取数据
        let committed_data_map = std::mem::take(&mut group_state.committed_data);
        let failed_reservation_infos = std::mem::take(&mut group_state.failed_infos);
        // **修改点**: 元数据现在应该还在 group_state 中，直接访问或克隆
        let reservation_metadata = std::mem::take(&mut group_state.reservation_metadata); // Take 元数据的所有权

        info!(
            "(Manager) [check_and_process] 检查取出的分组 {} 状态 (含 {} 提交项, {} 失败项, {} 元数据项)",
            group_id, committed_data_map.len(), failed_reservation_infos.len(), reservation_metadata.len()
        );

        // --- 4. 处理包含失败预留的分组 ---
        if !failed_reservation_infos.is_empty() {
            warn!(
                "(Manager) [check_and_process] 分组 {} 包含 {} 个失败预留，判定为处理失败。",
                group_id,
                failed_reservation_infos.len()
            );
            // **修改点**: 将包含失败信息和数据的 GroupState 放回 Manager，以便 finalize 处理
            // 因为 finalize 会遍历 self.groups，所以需要插回去
            group_state.committed_data = committed_data_map; // 放回数据
            group_state.failed_infos = failed_reservation_infos; // 放回失败信息
            group_state.reservation_metadata = reservation_metadata; // 放回元数据
            self.groups.insert(group_id, group_state); // 插回 Manager
            debug!(
                "(Manager) [check_and_process] 已将失败分组 {} 的状态放回 Manager",
                group_id
            );

            // 返回错误，指示 finalize 需要处理这个失败分组
            return Err(ManagerError::GroupContainsFailures(group_id));
        }

        // --- 5. 处理没有失败预留的分组 ---

        // 处理空分组 (没有提交数据，也没有失败信息)
        if committed_data_map.is_empty() {
            debug!(
                "(Manager) [check_and_process] 分组 {} 既无提交数据也无失败信息，视为空分组处理。",
                group_id
            );
            // 空分组已从 Manager 移除，直接返回 Ok(true) 表示处理完成
            return Ok(true);
        }

        // 验证连续性和总大小
        // **修改点**: 传入取出的 reservation_metadata
        let validation_result = self.validate_group_continuity_and_size(
            group_id,
            &committed_data_map,
            &reservation_metadata, // 传入元数据引用
        );

        match validation_result {
            Ok((start_offset, total_size)) => {
                // 验证通过，数据连续且大小正确
                info!(
                    "(Manager) [check_and_process] 分组 {} 数据连续且大小正确 (Offset: {}, Size: {})，准备合并发送...",
                    group_id, start_offset, total_size
                );

                // 合并数据
                let merged_data =
                    self.merge_committed_data(group_id, committed_data_map, total_size);

                match merged_data {
                    Ok(data_to_send_vec) => {
                        // 合并成功，发送数据
                        let data_to_send: SuccessfulGroupData =
                            (start_offset, data_to_send_vec.into_boxed_slice()); // 转为 Box<[u8]>

                        if let Err(e) = self.completed_data_tx.send(data_to_send).await {
                            error!(
                                "(Manager) [check_and_process] 发送完成的分组 {} 数据失败: {}",
                                group_id, e
                            );
                            // **修改点**: 发送失败，分组状态已从 Manager 移除，尝试标记为失败并交由 finalize 处理？
                            // 或者直接认为是数据丢失？目前选择记录错误并返回失败
                            // 构建一个临时的失败信息放入 group_state，然后插回？
                            group_state.failed_infos.push(FailedReservationInfo {
                                id: ReservationId::MAX, // 特殊 ID 表示发送失败
                                group_id,
                                offset: start_offset,
                                size: total_size,
                            });
                            group_state.reservation_metadata = reservation_metadata; // 放回元数据
                                                                                     // committed_data 已被 merge 消耗
                            self.groups.insert(group_id, group_state); // 尝试放回
                            Err(ManagerError::SendCompletionFailed(group_id))
                        } else {
                            info!(
                                "(Manager) [check_and_process] 分组 {} 已成功合并并发送给消费者。",
                                group_id
                            );
                            // 成功处理并发送，分组已从 Manager 移除，返回 Ok(true)
                            Ok(true)
                        }
                    }
                    Err(merge_err) => {
                        // 合并过程中出现错误
                        error!(
                            "(Manager) [check_and_process] 分组 {} 数据合并失败: {:?}",
                            group_id, merge_err
                        );
                        // **修正点**: 验证失败，放回 group_state，其 committed_data 为空
                        // committed_data_map 在这里没有被消耗，但 group_state.committed_data 已经是空的
                        group_state.reservation_metadata = reservation_metadata; // 放回元数据
                                                                                 // failed_infos 是空的
                        self.groups.insert(group_id, group_state); // 放回 Manager

                        Err(merge_err) // 将合并错误传递出去
                    }
                }
            }
            Err(validation_err) => {
                // 验证失败
                warn!(
                    "(Manager) [check_and_process] 分组 {} 数据不连续或大小/偏移不匹配: {:?}. 此分组处理失败。",
                    group_id, validation_err
                );
                // **修改点**: 验证失败，将状态放回 Manager
                group_state.committed_data = committed_data_map; // 放回提交的数据
                                                                 // failed_infos 是空的
                group_state.reservation_metadata = reservation_metadata; // 放回元数据
                self.groups.insert(group_id, group_state); // 放回

                Err(validation_err) // 返回验证错误
            }
        }
    } // check_and_process_completed_group 结束

    /// 验证分组数据的连续性、总大小和起始偏移
    /// **修改点**: 直接使用传入的 metadata map
    fn validate_group_continuity_and_size(
        &self,
        group_id: GroupId,
        committed_data: &BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
        metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>, // 使用传入的 Map
    ) -> Result<(AbsoluteOffset, usize), ManagerError> {
        // 返回 Ok((起始偏移, 总大小)) 或 Err

        // ... (内部逻辑不变，现在 metadata 是正确的了) ...

        if committed_data.is_empty() {
            debug!(
                "(Manager) [validate] 分组 {} 没有提交的数据，无法验证。",
                group_id
            );
            return Err(ManagerError::Internal(format!(
                "尝试验证空分组 {}",
                group_id
            )));
        }

        let mut calculated_total_size = 0;
        let mut current_expected_offset: Option<AbsoluteOffset> = None;
        let mut actual_group_start_offset: Option<AbsoluteOffset> = None;

        for (&res_start_offset, (_id, res_size, _chunks)) in committed_data {
            if actual_group_start_offset.is_none() {
                actual_group_start_offset = Some(res_start_offset);
            }
            if let Some(expected) = current_expected_offset {
                if res_start_offset != expected {
                    error!(
                        "(Manager) [validate] 分组 {} 数据不连续！Res {} (Offset {}) 处中断。期望 {}, 实际 {}",
                        group_id, _id, res_start_offset, expected, res_start_offset
                    );
                    return Err(ManagerError::GroupNotContiguous(
                        group_id,
                        expected,
                        res_start_offset,
                    ));
                }
            }
            calculated_total_size += res_size;
            current_expected_offset = Some(res_start_offset + res_size);
        }

        // 从元数据计算期望的总大小和起始偏移
        // **重点**: 现在 metadata 包含了所有（包括已成功提交）的预留信息
        let expected_total_size_from_meta: usize =
            metadata.values().map(|&(_offset, size)| size).sum();
        let expected_group_start_offset_from_meta =
            metadata.values().map(|&(offset, _size)| offset).min();

        // 比较计算出的总大小和元数据期望的总大小
        if calculated_total_size != expected_total_size_from_meta {
            error!(
                "(Manager) [validate] 分组 {} 大小不匹配！元数据期望 {}, 提交计算得到 {}",
                group_id, expected_total_size_from_meta, calculated_total_size
            );
            // Log the metadata content for debugging
            trace!(
                "(Manager) [validate] Metadata for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ManagerError::GroupSizeMismatch(
                group_id,
                expected_total_size_from_meta,
                calculated_total_size,
            ));
        }

        if actual_group_start_offset != expected_group_start_offset_from_meta {
            error!(
                "(Manager) [validate] 分组 {} 起始偏移不匹配！元数据期望 {:?}, 提交计算得到 {:?}",
                group_id, expected_group_start_offset_from_meta, actual_group_start_offset
            );
            trace!(
                "(Manager) [validate] Metadata for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ManagerError::GroupOffsetMismatch(
                group_id,
                expected_group_start_offset_from_meta,
                actual_group_start_offset,
            ));
        }

        // 所有检查通过
        info!(
            "(Manager) [validate] Group {} 验证通过 (Offset {:?}, Size {})",
            group_id, actual_group_start_offset, calculated_total_size
        );
        Ok((actual_group_start_offset.unwrap(), calculated_total_size)) // unwrap 安全
    }

    /// 合并已提交的分组数据
    fn merge_committed_data(
        &self,
        group_id: GroupId,
        // 注意：这里接收 committed_data 的所有权
        committed_data: BTreeMap<
            AbsoluteOffset,
            (ReservationId, usize, BTreeMap<AbsoluteOffset, Bytes>),
        >,
        expected_total_size: usize,
    ) -> Result<Vec<u8>, ManagerError> {
        // 返回合并后的 Vec<u8> 或 Err
        trace!(
            "(Manager) [merge] 开始合并 Group {} 数据，预期总大小 {}",
            group_id,
            expected_total_size
        );
        let mut merged_data_vec = Vec::with_capacity(expected_total_size);

        // 再次确保按预留偏移顺序合并 (BTreeMap 保证)
        for (_res_offset, (_res_id, _res_size, reservation_chunks)) in committed_data {
            // 确保按块偏移顺序合并 (内层 BTreeMap 保证)
            for (_chunk_relative_offset, chunk_bytes) in reservation_chunks {
                merged_data_vec.extend_from_slice(&chunk_bytes);
            }
        }

        // 最后校验合并后的大小是否与预期一致
        if merged_data_vec.len() != expected_total_size {
            error!(
                "(Manager) [merge] 分组 {} 合并后大小 {} 与预期大小 {} 不符！",
                group_id,
                merged_data_vec.len(),
                expected_total_size
            );
            // 合并失败
            Err(ManagerError::MergeSizeMismatch(
                group_id,
                expected_total_size,
                merged_data_vec.len(),
            ))
        } else {
            trace!(
                "(Manager) [merge] Group {} 数据合并成功，得到 {} 字节",
                group_id,
                merged_data_vec.len()
            );
            Ok(merged_data_vec)
        }
    }
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

// 在 src/manager/helpers.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*;
    // 导入父模块中的所有内容
    use crate::{
        manager::Manager, // 需要 Manager 结构体定义
        types::{
            FailedGroupDataTransmission, FailedReservationInfo, GroupState, Request,
            SuccessfulGroupData,
        },
    };
    use bytes::Bytes;
    use std::collections::{BTreeMap, HashMap};
    use tokio::sync::mpsc;

    // 辅助函数：创建一个基本的 Manager 实例 (不启动 Actor)
    // 注意：这只是为了测试 Manager 的同步辅助函数，对于需要 Actor 运行的逻辑无效。
    fn new_test_manager(min_group_commit_size: usize) -> Manager {
        let (_request_tx, request_rx) = mpsc::channel::<Request>(1);
        let (completed_data_tx, _completed_data_rx) = mpsc::channel::<SuccessfulGroupData>(1);
        let (failed_data_tx, _failed_data_rx) = mpsc::channel::<FailedGroupDataTransmission>(1);

        Manager {
            request_rx,
            completed_data_tx,
            failed_data_tx,
            groups: HashMap::new(),
            next_reservation_id: 0,
            next_group_id: 0,
            next_allocation_offset: 0,
            active_group_id: None,
            min_group_commit_size,
            is_finalizing: false,
        }
    }

    // --- GroupState 方法测试 ---
    #[test]
    fn test_group_state_new_and_add_reservation() {
        let mut gs = GroupState::new();
        assert_eq!(gs.total_size, 0);
        assert!(gs.reservations.is_empty());
        assert!(gs.reservation_metadata.is_empty());

        gs.add_reservation(1, 100, 50);
        assert_eq!(gs.total_size, 50);
        assert!(gs.reservations.contains(&1));
        assert_eq!(gs.reservation_metadata.get(&1), Some(&(100, 50)));

        gs.add_reservation(2, 150, 30);
        assert_eq!(gs.total_size, 80); // 50 + 30
        assert!(gs.reservations.contains(&2));
        assert_eq!(gs.reservation_metadata.get(&2), Some(&(150, 30)));
    }

    #[test]
    fn test_group_state_remove_pending_reservation() {
        let mut gs = GroupState::new();
        gs.add_reservation(1, 0, 10); // metadata 和 reservations 都有
        gs.add_reservation(2, 10, 20);

        assert!(gs.remove_pending_reservation(1)); // 从 reservations 移除
        assert!(!gs.reservations.contains(&1));
        assert!(gs.reservation_metadata.contains_key(&1)); // 元数据应该还在

        assert!(!gs.remove_pending_reservation(3)); // 移除不存在的
    }

    #[test]
    fn test_group_state_has_pending_reservation() {
        let mut gs = GroupState::new();
        gs.add_reservation(1, 0, 10);
        gs.remove_pending_reservation(1); // 模拟提交或失败，从待处理移除

        assert!(!gs.has_pending_reservation(1)); // 不在待处理中
        gs.add_reservation(2, 10, 5);
        assert!(gs.has_pending_reservation(2)); // 在待处理中
    }

    #[test]
    fn test_group_state_add_and_has_committed_data() {
        let mut gs = GroupState::new();
        let mut chunks = BTreeMap::new();
        chunks.insert(0, Bytes::from_static(b"data"));
        gs.add_committed_data(100, 1, 4, chunks.clone());

        assert!(gs.has_committed_data(100));
        assert!(!gs.has_committed_data(200));
        let committed_entry = gs.committed_data.get(&100);
        assert!(committed_entry.is_some());
        if let Some((res_id, size, r_chunks)) = committed_entry {
            assert_eq!(*res_id, 1);
            assert_eq!(*size, 4);
            assert_eq!(*r_chunks, chunks);
        }
    }

    #[test]
    fn test_group_state_add_failed_info() {
        let mut gs = GroupState::new();
        let info = FailedReservationInfo {
            id: 1,
            group_id: 0,
            offset: 0,
            size: 10,
        };
        gs.add_failed_info(info.clone());
        assert_eq!(gs.failed_infos.len(), 1);
        assert_eq!(gs.failed_infos[0], info);
    }

    #[test]
    fn test_group_state_should_seal_and_seal() {
        let mut gs = GroupState::new();
        gs.total_size = 50;
        let min_commit_size = 100;

        assert!(!gs.is_sealed);
        assert!(!gs.should_seal(min_commit_size)); // 50 < 100

        gs.total_size = 100;
        assert!(gs.should_seal(min_commit_size)); // 100 >= 100

        gs.seal();
        assert!(gs.is_sealed);
        assert!(!gs.should_seal(min_commit_size)); // 已密封，不再应该密封
    }

    #[test]
    fn test_group_state_is_complete() {
        let mut gs = GroupState::new();
        gs.add_reservation(1, 0, 10); // 有待处理的预留
        gs.is_sealed = true;
        assert!(!gs.is_complete()); // 未完成，因为 reservations 不为空

        gs.remove_pending_reservation(1); // 移除待处理
        assert!(gs.is_complete()); // 完成，reservations 为空且已密封

        gs.is_sealed = false;
        assert!(!gs.is_complete()); // 未密封，不完成
    }

    #[test]
    fn test_group_state_is_empty() {
        let mut gs = GroupState::new();
        assert!(gs.is_empty()); // 新建的应该是空的

        gs.add_reservation(1, 0, 10);
        assert!(!gs.is_empty()); // 有元数据和待处理预留

        gs.remove_pending_reservation(1);
        // 此时 reservations 为空，但 reservation_metadata 不为空
        assert!(!gs.is_empty());

        // 模拟 finalize 或清理后，元数据也被移除
        gs.reservation_metadata.remove(&1);
        assert!(gs.is_empty());

        gs.add_failed_info(FailedReservationInfo {
            id: 2,
            group_id: 0,
            offset: 10,
            size: 5,
        });
        assert!(!gs.is_empty()); // 有失败信息

        gs.failed_infos.clear();
        let mut chunks = BTreeMap::new();
        chunks.insert(0, Bytes::new());
        gs.add_committed_data(20, 3, 0, chunks);
        assert!(!gs.is_empty()); // 有提交数据
    }

    // --- Manager 辅助函数测试 ---
    #[test]
    fn test_manager_create_new_group() {
        let mut manager = new_test_manager(100);
        let initial_res_id = 0;
        let initial_offset = 0;
        let initial_size = 50;

        let group_id = manager.create_new_group(initial_res_id, initial_offset, initial_size);
        assert_eq!(group_id, 0); // 第一个组 ID
        assert_eq!(manager.next_group_id, 1);

        let group_state = manager.groups.get(&group_id).unwrap();
        assert_eq!(group_state.total_size, initial_size);
        assert!(group_state.reservations.contains(&initial_res_id));
        assert_eq!(
            group_state.reservation_metadata.get(&initial_res_id),
            Some(&(initial_offset, initial_size))
        );
        assert!(!group_state.is_sealed); // 50 < 100

        // 测试创建时即密封
        let mut manager2 = new_test_manager(50);
        let group_id2 = manager2.create_new_group(1, 100, 60); // 60 >= 50
        let group_state2 = manager2.groups.get(&group_id2).unwrap();
        assert!(group_state2.is_sealed);
    }

    #[test]
    fn test_manager_cleanup_failed_reservation() {
        let mut manager = new_test_manager(100);
        let res_id = 0;
        let group_id = manager.create_new_group(res_id, 0, 50);
        manager.active_group_id = Some(group_id);

        manager.cleanup_failed_reservation(res_id, group_id);

        let group_state = manager.groups.get(&group_id);
        // 预留已无效，分组大小应减少
        // 并且因为分组变空（只有一个预留），分组本身也应该被移除
        assert!(group_state.is_none(), "分组应该因为变空而被移除");
        assert_eq!(manager.active_group_id, None, "活动分组ID应被清除");

        // 测试清理不存在的预留或分组
        manager.cleanup_failed_reservation(99, 99); // 不应 panic

        // 测试清理后分组未空的情况
        let mut manager2 = new_test_manager(100);
        let res_id1 = 1;
        let res_id2 = 2;
        let group_id2 = manager2.create_new_group(res_id1, 0, 50);
        // 手动添加第二个预留到同一个组
        manager2
            .groups
            .get_mut(&group_id2)
            .unwrap()
            .add_reservation(res_id2, 50, 30);
        assert_eq!(manager2.groups.get(&group_id2).unwrap().total_size, 80);

        manager2.cleanup_failed_reservation(res_id1, group_id2);
        let group_state_after_cleanup = manager2.groups.get(&group_id2).unwrap();
        assert_eq!(group_state_after_cleanup.total_size, 30); // 80 - 50
        assert!(!group_state_after_cleanup.reservations.contains(&res_id1));
        assert!(!group_state_after_cleanup
            .reservation_metadata
            .contains_key(&res_id1));
        assert!(group_state_after_cleanup.reservations.contains(&res_id2)); // res_id2 应该还在
    }

    #[test]
    fn test_validate_group_continuity_and_size_success() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();
        let mut metadata = HashMap::new();

        // Res 1: offset 0, size 10
        let mut chunks1 = BTreeMap::new();
        chunks1.insert(0, Bytes::from_static(b"0123456789"));
        committed_data.insert(0, (1, 10, chunks1));
        metadata.insert(1, (0, 10));

        // Res 2: offset 10, size 5
        let mut chunks2 = BTreeMap::new();
        chunks2.insert(0, Bytes::from_static(b"abcde"));
        committed_data.insert(10, (2, 5, chunks2));
        metadata.insert(2, (10, 5));

        match manager.validate_group_continuity_and_size(group_id, &committed_data, &metadata) {
            Ok((start_offset, total_size)) => {
                assert_eq!(start_offset, 0);
                assert_eq!(total_size, 15);
            }
            Err(e) => panic!("验证应该成功，但失败了: {:?}", e),
        }
    }

    #[test]
    fn test_validate_group_not_contiguous() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();
        let mut metadata = HashMap::new();

        // Res 1: offset 0, size 10
        let mut chunks1 = BTreeMap::new();
        chunks1.insert(0, Bytes::from_static(b"0123456789"));
        committed_data.insert(0, (1, 10, chunks1));
        metadata.insert(1, (0, 10));

        // Res 2: offset 15 (应该是10), size 5
        let mut chunks2 = BTreeMap::new();
        chunks2.insert(0, Bytes::from_static(b"abcde"));
        committed_data.insert(15, (2, 5, chunks2)); // 不连续
        metadata.insert(2, (15, 5));

        match manager.validate_group_continuity_and_size(group_id, &committed_data, &metadata) {
            Err(ManagerError::GroupNotContiguous(gid, expected, actual)) => {
                assert_eq!(gid, group_id);
                assert_eq!(expected, 10); // Res1 结束后期望偏移是 10
                assert_eq!(actual, 15); // Res2 实际起始偏移是 15
            }
            Ok(_) => panic!("验证应该因不连续而失败"),
            Err(e) => panic!("期望 GroupNotContiguous 错误, 但得到 {:?}", e),
        }
    }

    #[test]
    fn test_validate_group_size_mismatch() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();
        let mut metadata = HashMap::new(); // 元数据将指示不同的大小

        // Res 1: offset 0, size 10 (提交的数据)
        let mut chunks1 = BTreeMap::new();
        chunks1.insert(0, Bytes::from_static(b"0123456789"));
        committed_data.insert(0, (1, 10, chunks1));
        metadata.insert(1, (0, 15)); // 元数据说这个预留是 15 字节

        match manager.validate_group_continuity_and_size(group_id, &committed_data, &metadata) {
            Err(ManagerError::GroupSizeMismatch(
                gid,
                expected_meta_total,
                actual_committed_total,
            )) => {
                assert_eq!(gid, group_id);
                assert_eq!(expected_meta_total, 15); // 元数据总大小
                assert_eq!(actual_committed_total, 10); // 提交数据总大小
            }
            Ok(_) => panic!("验证应该因大小不匹配而失败"),
            Err(e) => panic!("期望 GroupSizeMismatch 错误, 但得到 {:?}", e),
        }
    }

    #[test]
    fn test_validate_group_offset_mismatch() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();
        let mut metadata = HashMap::new();

        // Committed data starts at offset 10
        let mut c_chunks = BTreeMap::new();
        c_chunks.insert(0, Bytes::from_static(b"data"));
        committed_data.insert(10, (1, 4, c_chunks));

        // Metadata says reservation 1 should start at offset 0
        metadata.insert(1, (0, 4));

        match manager.validate_group_continuity_and_size(group_id, &committed_data, &metadata) {
            Err(ManagerError::GroupOffsetMismatch(
                gid,
                expected_meta_offset,
                actual_committed_offset,
            )) => {
                assert_eq!(gid, group_id);
                assert_eq!(expected_meta_offset, Some(0));
                assert_eq!(actual_committed_offset, Some(10));
            }
            Ok(_) => panic!("验证应该因偏移不匹配而失败"),
            Err(e) => panic!("期望 GroupOffsetMismatch 错误, 但得到 {:?}", e),
        }
    }

    #[test]
    fn test_merge_committed_data_success() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();

        let mut chunks_res1 = BTreeMap::new();
        chunks_res1.insert(0, Bytes::from_static(b"Hello")); // 相对偏移 0
        chunks_res1.insert(5, Bytes::from_static(b" ")); // 相对偏移 5 (相对于 Res1 的起始)
        committed_data.insert(0, (1, 6, chunks_res1)); // Res1: offset 0, size 6

        let mut chunks_res2 = BTreeMap::new();
        chunks_res2.insert(0, Bytes::from_static(b"World")); // 相对偏移 0
        committed_data.insert(6, (2, 5, chunks_res2)); // Res2: offset 6, size 5

        let expected_total_size = 11; // "Hello " (6) + "World" (5)
        match manager.merge_committed_data(group_id, committed_data, expected_total_size) {
            Ok(merged_vec) => {
                assert_eq!(merged_vec.len(), expected_total_size);
                assert_eq!(String::from_utf8(merged_vec).unwrap(), "Hello World");
            }
            Err(e) => panic!("合并应该成功，但失败了: {:?}", e),
        }
    }

    #[test]
    fn test_merge_committed_data_size_mismatch() {
        let manager = new_test_manager(100);
        let group_id = 0;
        let mut committed_data = BTreeMap::new();
        let mut chunks = BTreeMap::new();
        chunks.insert(0, Bytes::from_static(b"data"));
        committed_data.insert(0, (1, 4, chunks));

        let expected_total_size = 5; // 期望大小与实际合并大小不符
        match manager.merge_committed_data(group_id, committed_data, expected_total_size) {
            Err(ManagerError::MergeSizeMismatch(gid, expected, actual)) => {
                assert_eq!(gid, group_id);
                assert_eq!(expected, expected_total_size);
                assert_eq!(actual, 4); // "data" 的长度
            }
            Ok(_) => panic!("合并应该因大小不符而失败"),
            Err(e) => panic!("期望 MergeSizeMismatch 错误, 但得到 {:?}", e),
        }
    }

    // 测试 check_and_process_completed_group 较为复杂，因为它涉及到 Manager 的状态修改
    // 和异步通道发送。下面是一些简化的场景或思路。
    // 完整的测试可能更适合集成测试。

    #[tokio::test]
    async fn test_check_and_process_empty_group() {
        let mut manager = new_test_manager(100);
        let group_id = manager.create_new_group(0, 0, 0); // 创建一个空组（虽然create_new_group通常有大小）
        manager
            .groups
            .get_mut(&group_id)
            .unwrap()
            .reservations
            .clear(); // 确保无待处理
        manager
            .groups
            .get_mut(&group_id)
            .unwrap()
            .reservation_metadata
            .clear(); // 确保无元数据
        manager.groups.get_mut(&group_id).unwrap().is_sealed = true; // 密封它

        // 确保分组在 Manager 中
        assert!(manager.groups.contains_key(&group_id));

        match manager.check_and_process_completed_group(group_id).await {
            Ok(processed) => {
                assert!(processed, "空分组应该被成功处理 (processed = true)");
                assert!(
                    !manager.groups.contains_key(&group_id),
                    "处理后空分组应该从 Manager 中移除"
                );
            }
            Err(e) => panic!("处理空分组不应失败: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_check_and_process_group_with_failures() {
        let mut manager = new_test_manager(100);
        let group_id = manager.create_new_group(0, 0, 10);
        manager
            .groups
            .get_mut(&group_id)
            .unwrap()
            .add_failed_info(FailedReservationInfo {
                id: 0,
                group_id,
                offset: 0,
                size: 10,
            });
        manager
            .groups
            .get_mut(&group_id)
            .unwrap()
            .reservations
            .clear(); // 无待处理
        manager.groups.get_mut(&group_id).unwrap().is_sealed = true; // 已密封

        match manager.check_and_process_completed_group(group_id).await {
            Err(ManagerError::GroupContainsFailures(gid)) => {
                assert_eq!(gid, group_id);
                assert!(
                    manager.groups.contains_key(&group_id),
                    "含失败的分组应被放回 Manager"
                );
            }
            Ok(_) => panic!("有失败信息的分组处理应该返回 Err"),
            Err(e) => panic!("期望 GroupContainsFailures, 得到 {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_check_and_process_successful_group_sends_data() {
        // 这个测试需要 manager 的 completed_data_tx 能被接收
        let (completed_tx, mut completed_rx) = mpsc::channel::<SuccessfulGroupData>(1);
        let (_request_tx, request_rx) = mpsc::channel::<Request>(1);
        let (failed_data_tx, _failed_data_rx) = mpsc::channel::<FailedGroupDataTransmission>(1);

        let mut manager = Manager {
            request_rx,
            completed_data_tx: completed_tx, // 使用我们创建的发送端
            failed_data_tx,
            groups: HashMap::new(),
            next_reservation_id: 0,
            next_group_id: 0,
            next_allocation_offset: 0,
            active_group_id: None,
            min_group_commit_size: 10,
            is_finalizing: false,
        };

        let res_id = 1;
        let group_id = manager.create_new_group(res_id, 0, 20); // size 20, min_commit_size 10 -> 会密封
        let group_state = manager.groups.get_mut(&group_id).unwrap();

        // 模拟提交数据
        let mut chunks = BTreeMap::new();
        let data_bytes = Bytes::from_static(b"01234567890123456789"); // 20 bytes
        chunks.insert(0, data_bytes.clone());
        group_state.add_committed_data(0, res_id, 20, chunks);
        group_state.remove_pending_reservation(res_id); // 标记为已处理
                                                        // group_state 已经是 sealed 的

        assert!(group_state.is_complete());

        let process_result = manager.check_and_process_completed_group(group_id).await;
        assert!(
            process_result.is_ok() && process_result.unwrap(),
            "成功分组处理应返回 Ok(true)"
        );
        assert!(
            !manager.groups.contains_key(&group_id),
            "成功处理的分组应被移除"
        );

        // 检查是否发送了数据
        match tokio::time::timeout(std::time::Duration::from_millis(100), completed_rx.recv()).await
        {
            Ok(Some((offset, data))) => {
                assert_eq!(offset, 0);
                assert_eq!(*data, data_bytes.to_vec());
            }
            _ => panic!("未在 completed_data_tx 上收到数据"),
        }
    }
}
