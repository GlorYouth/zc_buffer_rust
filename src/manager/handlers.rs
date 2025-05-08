//! Manager 请求处理逻辑实现

use super::{
    types::{CommitType, FailedInfoRequest}, // 引入 CommitType 和 FailedInfoRequest
    GroupId,
    Manager,
    ManagerError,
    ReserveRequest,
    SubmitBytesRequest,
};
use std::collections::BTreeMap; // 引入 HashSet
use tracing::{debug, error, info, trace, warn}; // 引入 warn

// ==================================
// Manager 实现块 - 请求处理
// ==================================

impl Manager {
    /// 处理预留 (Reserve) 请求
    pub(super) fn handle_reserve(&mut self, req: ReserveRequest) {
        // 1. 分配预留 ID
        let reservation_id = self.next_reservation_id;
        self.next_reservation_id += 1;

        // 2. 分配偏移量并计算大小
        let offset = self.next_allocation_offset;
        let size = usize::from(req.size); // 从 NonZeroUsize 获取 usize
        self.next_allocation_offset += size;

        // 3. 决定目标分组并更新状态
        let target_group_id: GroupId;
        let mut created_new_group = false;

        // 检查是否有活动分组
        if let Some(active_id) = self.active_group_id {
            // 获取活动分组的可变引用
            if let Some(group) = self.groups.get_mut(&active_id) {
                // 如果活动分组已密封，则需要创建新分组
                if group.is_sealed {
                    debug!(
                        "(Manager) 活动分组 {} 已密封，为 Res {} (Offset {}, Size {}) 创建新分组",
                        active_id, reservation_id, offset, size
                    );
                    // 创建新分组，传入初始预留信息
                    target_group_id = self.create_new_group(reservation_id, offset, size);
                    created_new_group = true;
                } else {
                    // 加入现有活动分组
                    target_group_id = active_id;
                    group.add_reservation(reservation_id, offset, size); // 使用辅助函数更新分组状态
                    debug!(
                        "(Manager) Res {} (Offset {}, Size {}) 加入活动分组 {}, 更新后大小 {}, Res 数量 {}, Meta 数量 {}",
                        reservation_id, offset, size, target_group_id, group.total_size, group.reservations.len(), group.reservation_metadata.len()
                    );
                    // 检查是否达到密封阈值
                    if group.should_seal(self.min_group_commit_size) {
                        info!(
                            "(Manager) 分组 {} (大小 {}) 达到或超过阈值 {}，将被密封。",
                            target_group_id, group.total_size, self.min_group_commit_size
                        );
                        group.seal(); // 使用辅助函数密封分组
                        self.active_group_id = None; // 清除活动分组 ID
                        debug!(
                            "(Manager) 活动分组 ID 已清除 (因分组 {} 密封)",
                            target_group_id
                        );
                    }
                }
            } else {
                // 致命错误：活动 ID 指向的分组不存在，这通常不应发生
                error!(
                    "(Manager) 内部错误：找不到活动分组 {} 的状态 for Res {}",
                    active_id, reservation_id
                );
                let _ = req.reply_tx.send(Err(ManagerError::Internal(format!(
                    "找不到活动分组 {}",
                    active_id
                ))));
                // 重要：回滚分配的偏移量
                self.next_allocation_offset -= size;
                return; // 提前返回
            }
        } else {
            // 没有活动分组，创建新的
            debug!(
                "(Manager) 没有活动分组，为 Res {} (Offset {}, Size {}) 创建新分组",
                reservation_id, offset, size
            );
            target_group_id = self.create_new_group(reservation_id, offset, size);
            created_new_group = true;
        }

        // 更新活动分组 ID (如果创建了新的且未密封的组)
        if created_new_group {
            // 再次获取分组以检查其密封状态（create_new_group 可能因达到阈值而直接密封）
            if let Some(new_group) = self.groups.get(&target_group_id) {
                if !new_group.is_sealed {
                    self.active_group_id = Some(target_group_id);
                    debug!("(Manager) 新分组 {} 设置为活动分组", target_group_id);
                } else {
                    // 如果新创建的分组直接被密封了，则当前没有活动分组
                    self.active_group_id = None;
                    debug!(
                        "(Manager) 新分组 {} 创建时即被密封，无活动分组",
                        target_group_id
                    );
                }
            } else {
                // 致命错误：刚创建的分组找不到了，这通常不应发生
                error!(
                    "(Manager) 内部错误：创建新分组 {} 后查找失败",
                    target_group_id
                );
                let _ = req.reply_tx.send(Err(ManagerError::Internal(format!(
                    "创建分组 {} 后查找失败",
                    target_group_id
                ))));
                // 回滚偏移量
                self.next_allocation_offset -= size;
                // 注意：GroupID 一般不回滚，但可以考虑记录此异常
                return; // 提前返回
            }
        }

        // 4. 回复: 发送 预留ID, 偏移量 和 目标分组ID
        if req
            .reply_tx
            .send(Ok((reservation_id, offset, target_group_id))) // 返回 GroupId
            .is_err()
        {
            // 发送回复失败，可能是客户端已放弃
            error!("(Manager) 发送 Reserve 回复失败 for Res {}", reservation_id);
            // 需要清理因此次预留而可能创建或修改的状态
            self.cleanup_failed_reservation(reservation_id, target_group_id);
        } else {
            trace!(
                "(Manager) 已成功回复 Reserve 请求 for Res {} (ID={}, Offset={}, Size={}, Group={})",
                reservation_id, reservation_id, offset, size, target_group_id
            );
        }
    }

    /// 处理提交数据块 (SubmitBytes) 请求
    pub(super) async fn handle_submit_bytes(&mut self, req: SubmitBytesRequest) {
        // 1. 获取预留对应的分组 ID
        let group_id = req.group_id;

        // 2. 获取分组的可变引用
        let group = match self.groups.get_mut(&group_id) {
            Some(g) => g,
            None => {
                // 理论上 find_group_for_reservation 找到了，这里不应该找不到
                error!(
                    "(Manager) SubmitBytes 内部错误: 找不到 Group {} (for Res {})",
                    group_id, req.reservation_id
                );
                let _ = req
                    .reply_tx
                    .send(Err(ManagerError::GroupNotFound(group_id)));
                return;
            }
        };

        // 3. 预检查 (Validation)
        //   a. 检查预留元数据是否存在且匹配
        let (expected_offset, expected_size) = match group
            .get_reservation_metadata(req.reservation_id)
        {
            Some(meta) => meta,
            None => {
                error!(
                    "(Manager) SubmitBytes 错误: Res {} 不在 Group {} 的元数据中 (可能已被处理或状态错误)",
                    req.reservation_id, group_id
                );
                let _ = req
                    .reply_tx
                    .send(Err(ManagerError::ReservationNotFound(req.reservation_id)));
                return;
            }
        };

        //   b. 检查提交的偏移量是否匹配
        if req.absolute_offset != expected_offset {
            error!(
                "(Manager) SubmitBytes 错误: Res {} 提交偏移 {} 与预留偏移 {} 不匹配",
                req.reservation_id, req.absolute_offset, expected_offset
            );
            let _ = req.reply_tx.send(Err(ManagerError::SubmitOffsetInvalid(
                req.reservation_id,
                req.absolute_offset,
                expected_offset..(expected_offset + expected_size), // 提供预期的范围
            )));
            return;
        }

        //   c. 检查是否已提交过此偏移的数据
        if group.has_committed_data(req.absolute_offset) {
            warn!(
                "(Manager) SubmitBytes 警告: Res {} (Offset {}) 似乎已被提交",
                req.reservation_id, req.absolute_offset
            );
            let _ = req
                .reply_tx
                .send(Err(ManagerError::AlreadyCommitted(req.reservation_id)));
            return;
        }

        // 4. 处理提交的数据并进行大小校验
        let mut actual_submitted_size = 0;
        let mut reservation_chunks = BTreeMap::new(); // Key: 块相对于预留的偏移, Value: Bytes

        match req.data {
            CommitType::Single(bytes) => {
                actual_submitted_size = bytes.len();
                // 校验单块大小
                if actual_submitted_size != expected_size {
                    error!(
                        "(Manager) SubmitBytes 错误: Res {} (Offset {}) 单块提交大小 {} 与预留大小 {} 不符",
                        req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size
                    );
                    let _ = req.reply_tx.send(Err(ManagerError::CommitSizeMismatch {
                        reservation_id: req.reservation_id,
                        expected: expected_size,
                        actual: actual_submitted_size,
                    }));
                    return;
                }
                // 单块提交，相对偏移为 0
                reservation_chunks.insert(0, bytes);
                debug!(
                    "(Manager) Res {} (Offset {}) 收到单块提交，大小 {}",
                    req.reservation_id, req.absolute_offset, actual_submitted_size
                );
            }
            CommitType::Chunked(chunks) => {
                let mut current_chunk_relative_offset = 0; // 块在预留内的相对偏移
                for chunk in chunks {
                    let len = chunk.len();
                    // 检查分块是否超出预留范围
                    if current_chunk_relative_offset + len > expected_size {
                        error!(
                            "(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交超出预留大小 {} (当前块偏移 {}, 长度 {})",
                            req.reservation_id, req.absolute_offset, expected_size, current_chunk_relative_offset, len
                        );
                        let _ = req.reply_tx.send(Err(ManagerError::SubmitRangeInvalid(
                            req.reservation_id,
                            req.absolute_offset + current_chunk_relative_offset, // 提交块的全局偏移
                            len,
                            expected_offset..(expected_offset + expected_size),
                        )));
                        return;
                    }
                    reservation_chunks.insert(current_chunk_relative_offset, chunk);
                    current_chunk_relative_offset += len;
                    actual_submitted_size += len;
                }
                // 校验分块提交的总大小
                if actual_submitted_size != expected_size {
                    error!(
                        "(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交总大小 {} 与预留大小 {} 不符",
                        req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size
                    );
                    let _ = req.reply_tx.send(Err(ManagerError::CommitSizeMismatch {
                        reservation_id: req.reservation_id,
                        expected: expected_size,
                        actual: actual_submitted_size,
                    }));
                    return;
                }
                debug!(
                    "(Manager) Res {} (Offset {}) 收到分块提交，共 {} 块，总大小 {}",
                    req.reservation_id,
                    req.absolute_offset,
                    reservation_chunks.len(),
                    actual_submitted_size
                );
            }
        }

        // 5. 更新分组状态：将提交的数据存入，并移除预留 ID
        group.add_committed_data(
            req.absolute_offset, // 使用预留的绝对起始偏移作为 Key
            req.reservation_id,
            expected_size,      // 存储期望的大小
            reservation_chunks, // 存储数据块
        );
        trace!(
            "(Manager) Res {} (Offset {}) 数据已存入 Group {} 的 committed_data ({} 项)",
            req.reservation_id,
            req.absolute_offset,
            group_id,
            group.committed_data.len()
        );

        // **修改点**: 只从待处理集合中移除 ID
        if group.remove_pending_reservation(req.reservation_id) {
            trace!(
                "(Manager) Res {} 从 Group {} 的待处理集合中移除 (因成功提交)",
                req.reservation_id,
                group_id
            );
        } else {
            // 如果在待处理集合中找不到，可能意味着重复提交或其他状态问题
            // 但因为前面检查了 has_committed_data，这里找不到更可能是逻辑错误
            warn!(
                "(Manager) SubmitBytes: Res {} 在提交成功后尝试从待处理集合移除时未找到",
                req.reservation_id
            );
        }

        // 6. 发送成功回复
        if req.reply_tx.send(Ok(())).is_err() {
            error!(
                "(Manager) 发送 SubmitBytes 成功回复失败 for Res {}",
                req.reservation_id
            );
            // 回复失败，状态已经改变，回滚困难。依赖 finalize 清理。
            // 这是一个潜在的问题点，如果提交的是最后一个使分组完成的预留，但回复失败，
            // 分组可能不会立即被检查和处理。
        } else {
            trace!(
                "(Manager) SubmitBytes 成功回复已发送 for Res {}",
                req.reservation_id
            );
        }

        // --- 7. 检查分组是否完成 ---
        // 注意：此时 group 变量仍然是可变借用。我们需要检查完成后再决定是否调用 check_and_process
        let should_check_completion = self
            .groups
            .get(&group_id)
            .map_or(false, |g| g.is_complete());

        if should_check_completion {
            info!(
                "(Manager) Group {} 已密封且所有活动预留已提交/处理，准备检查和处理完成状态...",
                group_id
            );
            let group_id_to_process = group_id;
            // 调用 check_and_process (已修改为先 remove 再处理)
            match self
                .check_and_process_completed_group(group_id_to_process)
                .await
            {
                Ok(processed) => {
                    if processed {
                        info!(
                            "(Manager) Group {} 成功处理完成 (可能已被移除)。",
                            group_id_to_process
                        );
                        // 注意：check_and_process 内部成功时已移除
                    } else {
                        warn!("(Manager) Group {} 处理完成检查返回未处理 (可能已被并发移除或状态错误)", group_id_to_process);
                    }
                }
                Err(e) => {
                    error!(
                        "(Manager) Group {} 处理完成时失败: {:?}",
                        group_id_to_process, e
                    );
                    // 失败时，check_and_process 会将状态放回 self.groups
                    warn!(
                        "(Manager) Group {} 处理失败，状态已放回，保留以待 Finalize 处理",
                        group_id_to_process
                    );
                }
            }
        } else {
            // 重新获取分组状态以打印日志 (避免借用冲突)
            if let Some(current_group_state) = self.groups.get(&group_id) {
                trace!(
                    "(Manager) Group {} 尚未完成 (剩余 reservations: {}, 密封: {})",
                    group_id,
                    current_group_state.reservations.len(),
                    current_group_state.is_sealed
                );
            } else {
                warn!("(Manager) SubmitBytes 后记录日志时找不到分组 {}", group_id);
            }
        }
    } // handle_submit_bytes 结束

    /// 处理 Agent Drop 时发送的失败信息
    pub(super) async fn handle_failed_info(&mut self, req: FailedInfoRequest) {
        let failed_info = req.info; // 提取 FailedReservationInfo
        let res_id = failed_info.id;

        // 1. 获取预留对应的分组 ID
        let group_id = failed_info.group_id;

        // 2. 获取分组的可变引用
        let group = match self.groups.get_mut(&group_id) {
            Some(g) => g,
            None => {
                // 理论上 find_group_for_reservation 找到了，这里不应找不到
                warn!(
                    "(Manager) FailedInfo 内部警告: 找不到 Group {} (for Res {})",
                    group_id, res_id
                );
                return;
            }
        };

        // 3. 检查 Res ID 是否仍在 reservations 集合中 (表示从未提交过)
        if group.has_pending_reservation(res_id) {
            info!(
                "(Manager) 收到 Res {} (Group {}, Offset {}, Size {}) 的失败信息 (Agent Drop)，标记为失败",
                res_id, group_id, failed_info.offset, failed_info.size
            );
            // **修改点**: 只从待处理集合移除
            if group.remove_pending_reservation(res_id) {
                trace!(
                    "(Manager) Res {} 从 Group {} 的待处理集合中移除 (因失败)",
                    res_id,
                    group_id
                );
            } else {
                warn!(
                    "(Manager) FailedInfo: Res {} 在处理失败时尝试从待处理集合移除未找到",
                    res_id
                );
            }

            // 将失败信息添加到列表中
            group.add_failed_info(failed_info.clone());
            debug!(
                "(Manager) Res {} 的失败信息已添加到 Group {} 的 failed_infos ({} 项)",
                res_id,
                group_id,
                group.failed_infos.len()
            );

            // 4. 检查分组是否因此完成
            // **修改点**: 同样需要重新获取只读引用检查
            let should_check_completion = self
                .groups
                .get(&group_id)
                .map_or(false, |g| g.is_complete());

            if should_check_completion {
                info!(
                     "(Manager) Group {} (因 Res {} 失败) 已密封且所有活动预留已处理，准备检查和处理完成状态...",
                     group_id, res_id
                 );
                let group_id_to_process = group_id;
                match self
                    .check_and_process_completed_group(group_id_to_process)
                    .await
                {
                    Ok(processed) => {
                        error!(
                              "(Manager) 内部逻辑错误：check_and_process 在有失败信息时返回了 Ok({}) for Group {}",
                              processed, group_id_to_process);
                        // 如果意外 Ok，尝试移除
                        // if self.groups.remove(&group_id_to_process).is_some() {
                        //    warn!("...");
                        // }
                    }
                    Err(e) => {
                        info!(
                            "(Manager) Group {} (含失败 Res {}) 处理完成检查返回错误 (预期): {:?}",
                            group_id_to_process, res_id, e
                        );
                        warn!("(Manager) Group {} (含失败) 处理失败，状态已放回，保留以待 Finalize 处理", group_id_to_process);
                    }
                }
            } else {
                // 重新获取分组状态以打印日志
                if let Some(current_group_state) = self.groups.get(&group_id) {
                    trace!(
                        "(Manager) Group {} (收到 Res {} 失败信息后) 尚未完成 (剩余 reservations: {}, 失败: {}, 密封: {})",
                        group_id, res_id, current_group_state.reservations.len(), current_group_state.failed_infos.len(), current_group_state.is_sealed
                    );
                } else {
                    warn!("(Manager) FailedInfo 后记录日志时找不到分组 {}", group_id);
                }
            }
        } else {
            // Res ID 不在待处理集合中
            // **修改点**: 不需要检查元数据了，因为 find_group_for_reservation 找到了就说明元数据还在
            // 这意味着预留要么已提交，要么已被标记为失败。
            trace!(
                "(Manager) 收到 Res {} (Group {}) 的失败信息，但其不在待处理集合中 (可能已提交或已标记失败)，忽略此重复信息",
                res_id, group_id
            );
        }
    } // handle_failed_info 结束
} // impl Manager 结束

// 在 src/manager/handlers.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use super::*; // 导入父模块中的所有内容
    use crate::{
        manager::Manager, // 需要 Manager 结构体定义
        types::{
            CommitType, FailedGroupDataTransmission, FailedInfoRequest, FailedReservationInfo,
            Request, ReserveRequest, SubmitBytesRequest, SuccessfulGroupData,
        },
    };
    use bytes::Bytes;
    use std::{collections::HashMap, num::NonZeroUsize};
    use tokio::sync::{mpsc, oneshot};

    // 辅助函数：创建一个测试用的 Manager 实例
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

    // --- handle_reserve 测试 ---
    #[tokio::test]
    async fn test_handle_reserve_new_group() {
        let mut manager = new_test_manager(100);
        let size = NonZeroUsize::new(50).unwrap();
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = ReserveRequest { size, reply_tx };

        manager.handle_reserve(req);

        match reply_rx.await {
            Ok(Ok((res_id, offset, group_id))) => {
                assert_eq!(res_id, 0); // 第一个预留
                assert_eq!(offset, 0); // 第一个分配偏移
                assert_eq!(group_id, 0); // 第一个分组
                assert_eq!(manager.next_reservation_id, 1);
                assert_eq!(manager.next_allocation_offset, 50);
                assert_eq!(manager.active_group_id, Some(0));
                assert!(manager.groups.contains_key(&0));
                let group = manager.groups.get(&0).unwrap();
                assert_eq!(group.total_size, 50);
                assert!(!group.is_sealed);
            }
            _ => panic!("handle_reserve 应该成功并返回预留信息"),
        }
    }

    #[tokio::test]
    async fn test_handle_reserve_join_active_group_and_seal() {
        let mut manager = new_test_manager(80); // 密封阈值 80

        // 第一次预留，创建新组
        let size1 = NonZeroUsize::new(50).unwrap();
        let (reply_tx1, reply_rx1) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: size1,
            reply_tx: reply_tx1,
        });
        let (_res_id1, offset1, group_id1) = reply_rx1.await.unwrap().unwrap();
        assert_eq!(group_id1, 0);
        assert_eq!(manager.active_group_id, Some(0));
        assert!(!manager.groups.get(&0).unwrap().is_sealed); // 50 < 80

        // 第二次预留，加入活动组并触发密封
        let size2 = NonZeroUsize::new(40).unwrap(); // 50 + 40 = 90 >= 80
        let (reply_tx2, reply_rx2) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: size2,
            reply_tx: reply_tx2,
        });
        let (res_id2, offset2, group_id2) = reply_rx2.await.unwrap().unwrap();

        assert_eq!(res_id2, 1);
        assert_eq!(offset2, offset1 + 50); // offset1是0
        assert_eq!(group_id2, group_id1); // 加入了同一个组
        assert_eq!(manager.active_group_id, None); // 分组被密封，活动组ID清除
        assert!(manager.groups.get(&0).unwrap().is_sealed);
        assert_eq!(manager.groups.get(&0).unwrap().total_size, 90);
    }

    #[tokio::test]
    async fn test_handle_reserve_active_group_sealed_creates_new() {
        let mut manager = new_test_manager(50); // 密封阈值 50

        // 第一次预留，创建并密封组0
        let (reply_tx1, reply_rx1) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(60).unwrap(),
            reply_tx: reply_tx1,
        });
        let (_, _, group_id1) = reply_rx1.await.unwrap().unwrap();
        assert_eq!(group_id1, 0);
        assert!(manager.groups.get(&0).unwrap().is_sealed);
        assert_eq!(manager.active_group_id, None); // 因为新创建的组直接密封了

        // 第二次预留，应该创建新组1
        let (reply_tx2, reply_rx2) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(30).unwrap(),
            reply_tx: reply_tx2,
        });
        let (_, _, group_id2) = reply_rx2.await.unwrap().unwrap();
        assert_eq!(group_id2, 1); // 新的分组
        assert_eq!(manager.active_group_id, Some(1)); // 组1成为活动组
        assert!(!manager.groups.get(&1).unwrap().is_sealed);
    }

    // #[tokio::test]
    // async fn test_handle_reserve_reply_fails_cleanup() {
    //     let mut manager = new_test_manager(100);
    //     let size = NonZeroUsize::new(50).unwrap();
    //     let (reply_tx, reply_rx) = oneshot::channel();
    //
    //     // 关闭接收端，模拟发送回复失败
    //     drop(reply_rx);
    //
    //     let req = ReserveRequest { size, reply_tx };
    //     manager.handle_reserve(req);
    //
    //     // 由于回复失败，预留和可能创建的组应该被清理
    //     // 在此场景下，会创建一个新组，然后因为回复失败而清理
    //     assert_eq!(manager.next_reservation_id, 1); // ID 仍然增加了
    //     assert_eq!(manager.next_allocation_offset, 0); // 偏移量应该回滚了
    //     assert!(manager.groups.is_empty(), "因回复失败导致的分组应被清理");
    //     assert_eq!(manager.active_group_id, None, "活动组ID应为空");
    // }

    // --- handle_submit_bytes 测试 ---
    #[tokio::test]
    async fn test_handle_submit_bytes_success_single_commit() {
        let mut manager = new_test_manager(100);

        // 1. 先预留空间
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(20).unwrap(),
            reply_tx: reserve_reply_tx,
        });
        let (res_id, offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();

        // 2. 提交数据
        let data = Bytes::from(vec![1u8; 20]);
        let (submit_reply_tx, submit_reply_rx) = oneshot::channel();
        let submit_req = SubmitBytesRequest {
            reservation_id: res_id,
            absolute_offset: offset,
            group_id,
            data: CommitType::Single(data.clone()),
            reply_tx: submit_reply_tx,
        };
        manager.handle_submit_bytes(submit_req).await;

        assert!(submit_reply_rx.await.unwrap().is_ok()); // 提交应该成功

        let group = manager.groups.get(&group_id).unwrap();
        assert!(!group.reservations.contains(&res_id)); // 从待处理移除
        assert!(group.committed_data.contains_key(&offset));
        let (_r_id, r_size, r_chunks) = group.committed_data.get(&offset).unwrap();
        assert_eq!(*r_size, 20);
        assert_eq!(r_chunks.get(&0).unwrap(), &data); // 块的相对偏移为0
        assert!(!group.is_complete()); // 尚未密封，所以未完成
    }

    #[tokio::test]
    async fn test_handle_submit_bytes_completes_and_processes_group() {
        let (completed_tx, mut completed_rx) = mpsc::channel::<SuccessfulGroupData>(1);
        let (_request_tx, request_rx) = mpsc::channel::<Request>(1);
        let (failed_data_tx, _failed_data_rx) = mpsc::channel::<FailedGroupDataTransmission>(1);

        let mut manager = Manager {
            request_rx,
            completed_data_tx: completed_tx,
            failed_data_tx,
            groups: HashMap::new(),
            next_reservation_id: 0,
            next_group_id: 0,
            next_allocation_offset: 0,
            active_group_id: None,
            min_group_commit_size: 10, // 低阈值以确保密封
            is_finalizing: false,
        };

        // 预留 (size 20 > min_commit_size 10, 所以会创建并密封)
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(20).unwrap(),
            reply_tx: reserve_reply_tx,
        });
        let (res_id, offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();
        assert!(manager.groups.get(&group_id).unwrap().is_sealed);
        assert_eq!(manager.active_group_id, None); // 因为新组被密封

        // 提交数据
        let data = Bytes::from(vec![1u8; 20]);
        let (submit_reply_tx, submit_reply_rx) = oneshot::channel();
        let submit_req = SubmitBytesRequest {
            reservation_id: res_id,
            absolute_offset: offset,
            group_id,
            data: CommitType::Single(data.clone()),
            reply_tx: submit_reply_tx,
        };
        manager.handle_submit_bytes(submit_req).await;
        assert!(submit_reply_rx.await.unwrap().is_ok());

        // 检查分组是否已被处理并发送
        assert!(
            !manager.groups.contains_key(&group_id),
            "完成的分组应该被处理并移除"
        );
        match tokio::time::timeout(std::time::Duration::from_millis(100), completed_rx.recv()).await
        {
            Ok(Some((recv_offset, recv_data))) => {
                assert_eq!(recv_offset, offset);
                assert_eq!(*recv_data, data.to_vec());
            }
            _ => panic!("未在 completed_data_tx 上收到数据"),
        }
    }

    #[tokio::test]
    async fn test_handle_submit_bytes_offset_invalid() {
        let mut manager = new_test_manager(100);
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(20).unwrap(),
            reply_tx: reserve_reply_tx,
        });
        let (res_id, correct_offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();

        let data = Bytes::from(vec![1u8; 20]);
        let (submit_reply_tx, submit_reply_rx) = oneshot::channel();
        let submit_req = SubmitBytesRequest {
            reservation_id: res_id,
            absolute_offset: correct_offset + 1, // 错误的偏移
            group_id,
            data: CommitType::Single(data),
            reply_tx: submit_reply_tx,
        };
        manager.handle_submit_bytes(submit_req).await;

        match submit_reply_rx.await.unwrap() {
            Err(ManagerError::SubmitOffsetInvalid(..)) => { /* 成功 */ }
            _ => panic!("期望 SubmitOffsetInvalid 错误"),
        }
    }

    #[tokio::test]
    async fn test_handle_submit_bytes_size_mismatch() {
        let mut manager = new_test_manager(100);
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(20).unwrap(),
            reply_tx: reserve_reply_tx,
        }); // 预留20字节
        let (res_id, offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();

        let data = Bytes::from(vec![1u8; 10]); // 提交10字节
        let (submit_reply_tx, submit_reply_rx) = oneshot::channel();
        let submit_req = SubmitBytesRequest {
            reservation_id: res_id,
            absolute_offset: offset,
            group_id,
            data: CommitType::Single(data),
            reply_tx: submit_reply_tx,
        };
        manager.handle_submit_bytes(submit_req).await;

        match submit_reply_rx.await.unwrap() {
            Err(ManagerError::CommitSizeMismatch {
                reservation_id,
                expected,
                actual,
            }) => {
                assert_eq!(reservation_id, res_id);
                assert_eq!(expected, 20);
                assert_eq!(actual, 10);
            }
            _ => panic!("期望 CommitSizeMismatch 错误"),
        }
    }

    // --- handle_failed_info 测试 ---
    #[tokio::test]
    async fn test_handle_failed_info_marks_reservation_failed() {
        let mut manager = new_test_manager(100);
        // 1. 预留
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(30).unwrap(),
            reply_tx: reserve_reply_tx,
        });
        let (res_id, offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();

        // 2. 发送失败信息
        let failed_res_info = FailedReservationInfo {
            id: res_id,
            group_id,
            offset,
            size: 30,
        };
        let failed_req = FailedInfoRequest {
            info: failed_res_info.clone(),
        };
        manager.handle_failed_info(failed_req).await;

        let group = manager.groups.get(&group_id).unwrap();
        assert!(!group.reservations.contains(&res_id)); // 从待处理移除
        assert_eq!(group.failed_infos.len(), 1);
        assert_eq!(group.failed_infos[0], failed_res_info);
        assert!(!group.is_complete()); // 未密封
    }

    #[tokio::test]
    async fn test_handle_failed_info_completes_and_processes_failed_group() {
        let (_completed_tx, mut completed_rx) = mpsc::channel::<SuccessfulGroupData>(1);
        let (_request_tx, request_rx) = mpsc::channel::<Request>(1);
        let (failed_data_tx, _failed_data_rx) = mpsc::channel::<FailedGroupDataTransmission>(1); // 需要这个通道，但在此测试中不直接检查其输出

        let mut manager = Manager {
            request_rx,
            completed_data_tx: _completed_tx,
            failed_data_tx,
            groups: HashMap::new(),
            next_reservation_id: 0,
            next_group_id: 0,
            next_allocation_offset: 0,
            active_group_id: None,
            min_group_commit_size: 10,
            is_finalizing: false,
        };

        // 预留 (size 30 > min_commit_size 10, 会密封)
        let (reserve_reply_tx, reserve_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(30).unwrap(),
            reply_tx: reserve_reply_tx,
        });
        let (res_id, offset, group_id) = reserve_reply_rx.await.unwrap().unwrap();
        assert!(manager.groups.get(&group_id).unwrap().is_sealed);

        // 发送失败信息
        let failed_req = FailedInfoRequest {
            info: FailedReservationInfo {
                id: res_id,
                group_id,
                offset,
                size: 30,
            },
        };
        manager.handle_failed_info(failed_req).await;

        // 分组包含失败信息，check_and_process_completed_group 应该返回 Err
        // 并且分组应该保留在 manager.groups 中，等待 finalize 处理
        assert!(
            manager.groups.contains_key(&group_id),
            "包含失败信息的分组应保留以待 Finalize"
        );
        let group = manager.groups.get(&group_id).unwrap();
        assert!(group.is_complete()); // 已密封且无待处理
        assert_eq!(group.failed_infos.len(), 1);

        // 确保没有数据发送到成功通道
        match tokio::time::timeout(std::time::Duration::from_millis(50), completed_rx.recv()).await
        {
            Ok(Some(_)) => panic!("不应有数据发送到成功通道"),
            _ => { /* 预期超时或None */ }
        }
    }
}
