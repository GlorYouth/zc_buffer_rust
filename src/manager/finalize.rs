//! Manager Finalize 逻辑实现
//! Implementation of Manager Finalize logic

use super::{
    types::{
        AbsoluteOffset, FailedGroupData, FailedReservationInfo, FinalizeResult, GroupId,
        ReservationId,
    }, // 引入所有需要的类型
       // Import all necessary types
};
use crate::Manager;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::mem;
// 需要 mem::take
use tracing::{debug, error, info, trace, warn};

// ==================================
// Manager 实现块 - Finalize 逻辑
// ==================================
impl Manager {
    /// 内部 Finalize 逻辑
    /// 1. 标记 Manager 正在 Finalize。
    /// 2. 遍历所有剩余的分组。
    /// 3. 对每个分组：
    ///    a. 强制标记为 sealed。
    ///    b. 将所有仍在 `reservations` 集合中的预留视为失败，更新 `failed_infos`。
    ///    c. 调用 `check_and_process_completed_group` 尝试处理分组。
    ///    d. 如果 `check_and_process` 返回 `Ok(true)` (成功处理)，则从 Manager 中移除该分组。
    ///    e. 如果 `check_and_process` 返回 `Ok(false)` (未处理)，也移除（因为是空分组或状态错误）。
    ///    f. 如果 `check_and_process` 返回 `Err` (处理失败)，则构建 `FailedGroupData`。
    ///       i. 尝试将 `FailedGroupData` 发送到 `failed_data_tx` 通道。
    ///       ii. 如果发送失败，将 `FailedGroupData` 添加到 `FinalizeResult` 的 `failed` 列表中。
    ///       iii. 从 Manager 中移除该分组。
    /// 4. 返回包含所有未能成功发送到失败通道的失败分组信息的 `FinalizeResult`。
    pub(super) async fn finalize_internal(&mut self) -> Option<FinalizeResult> {
        // 防止重复调用 Finalize
        if self.is_finalizing {
            warn!("(Manager) Finalize 已在进行中，忽略重复调用");
            return None; // 已经在 finalize，直接返回 None 表示无需再次处理
        }
        info!("(Manager) 开始内部 Finalize...");
        self.is_finalizing = true; // 标记开始 Finalize
        self.active_group_id = None; // Finalize 期间不再有活动分组

        // 用于收集未能通过通道发送的失败分组数据
        let mut failed_groups_for_report: Vec<FailedGroupData> = Vec::new();

        // 获取所有分组 ID 的快照进行迭代，因为循环中会修改 self.groups
        let group_ids: Vec<GroupId> = self.groups.keys().cloned().collect();
        info!(
            "(Manager) Finalize: 将处理 {} 个剩余分组: {:?}",
            group_ids.len(),
            group_ids
        );

        for group_id in group_ids {
            // --- 准备阶段：获取分组并处理未提交的预留 ---
            // **修改点**: 仍然使用 get_mut，但后续处理会移除它
            let group = if let Some(g) = self.groups.get_mut(&group_id) {
                g
            } else {
                warn!(
                    "(Manager) Finalize: 迭代时找不到 Group ID {} (可能已被并发移除?)",
                    group_id
                );
                continue; // 跳过
            };

            // 1. 强制密封
            if !group.is_sealed {
                debug!("(Manager) Finalize: 强制密封 Group {}", group_id);
                group.seal();
            }

            // 2. 处理仍在 reservations 中的预留 (视为失败)
            // 使用 drain() 高效移除并处理
            let remaining_res_ids: Vec<ReservationId> = group.reservations.drain().collect();
            if !remaining_res_ids.is_empty() {
                warn!(
                    "(Manager) Finalize: Group {} 发现 {} 个未提交的预留: {:?}，将标记为失败",
                    group_id,
                    remaining_res_ids.len(),
                    remaining_res_ids
                );
                for res_id in remaining_res_ids {
                    if let Some((offset, size)) = group.get_reservation_metadata(res_id) {
                        // 只读获取
                        let failed_info = FailedReservationInfo {
                            id: res_id,
                            group_id,
                            offset,
                            size,
                        };
                        debug!(
                            "(Manager) Finalize: 为未提交的 Res {} 创建失败信息: {:?}",
                            res_id, failed_info
                        );
                        group.add_failed_info(failed_info); // 添加到失败列表
                    } else {
                        // 元数据也没有？这表示状态严重不一致
                        error!(
                             "(Manager) Finalize: 严重错误！未提交的 Res {} 在 Group {} 的元数据中也找不到！",
                             res_id, group_id
                         );
                        // 仍然尝试添加一个信息不全的失败记录？或者忽略？选择添加部分信息
                        group.add_failed_info(FailedReservationInfo {
                            id: res_id,
                            group_id,
                            offset: AbsoluteOffset::MAX,
                            size: 0, // 特殊值表示信息缺失
                        });
                    }
                }
                // 此时 group.reservations 应该为空
                debug!(
                    "(Manager) Finalize: Group {} 所有未提交预留已处理",
                    group_id
                );
            }

            // --- 处理阶段：调用 check_and_process ---
            let group_id_to_process = group_id;
            // **修改点**: check_and_process 现在会先 remove 分组
            let process_result = self
                .check_and_process_completed_group(group_id_to_process)
                .await;

            // --- 结果处理阶段 ---
            match process_result {
                Ok(processed) => {
                    if processed {
                        // check_and_process 成功表示分组已合并发送或为空分组
                        info!(
                             "(Manager) Finalize: Group {} 在 Finalize 期间成功处理 (已发送或为空)。",
                              group_id_to_process
                          );
                        // 成功处理的分组不进入 FinalizeResult 报告
                        // check_and_process 成功后，分组应该已经被移除或即将被移除
                    } else {
                        // check_and_process 返回 Ok(false) 表示分组未找到或状态不允许处理
                        warn!(
                             "(Manager) Finalize: Group {} 处理返回未处理 (可能已被移除或状态错误)。",
                             group_id_to_process
                         );
                    }
                    // 无论 processed 是 true 还是 false，只要是 Ok，都尝试从 Manager 中移除
                    if self.groups.remove(&group_id_to_process).is_some() {
                        debug!(
                            "(Manager) Finalize: Group {} (处理结果 Ok) 已从 Manager 移除。",
                            group_id_to_process
                        );
                    } else {
                        // 可能 check_and_process 内部或并发移除了
                        trace!(
                            "(Manager) Finalize: 尝试移除 Group {} (处理结果 Ok) 时未找到。",
                            group_id_to_process
                        );
                    }
                }
                Err(e) => {
                    // check_and_process 失败表示分组是失败分组（不连续、大小错误或包含失败预留）
                    warn!(
                        "(Manager) Finalize: Group {} 处理失败 ({:?})，判定为失败分组。",
                        group_id_to_process, e
                    );

                    // 必须重新获取 group 的可变引用来构建 FailedGroupData
                    if let Some(failed_group_state) = self.groups.get_mut(&group_id_to_process) {
                        // 构建 FailedGroupData
                        // 从失败分组状态中 take 数据
                        let committed_data_for_failure =
                            mem::take(&mut failed_group_state.committed_data);
                        let failed_reservation_infos =
                            mem::take(&mut failed_group_state.failed_infos);
                        // **修改点**: 同时 take 元数据
                        let _metadata_for_failure =
                            mem::take(&mut failed_group_state.reservation_metadata);

                        // 将 committed_data 转换为 group_chunks 格式 (全局偏移 -> Bytes)
                        let group_chunks_for_failure: BTreeMap<AbsoluteOffset, Bytes> =
                            committed_data_for_failure
                                .into_iter()
                                .flat_map(|(res_start_offset, (_res_id, _res_size, chunks))| {
                                    chunks
                                        .into_iter()
                                        .map(move |(chunk_relative_offset, bytes)| {
                                            (res_start_offset + chunk_relative_offset, bytes)
                                        })
                                })
                                .collect();

                        let failed_data = FailedGroupData {
                            group_id: group_id_to_process,                 // 使用克隆的 ID
                            group_chunks: group_chunks_for_failure, // 包含所有已提交但未成功合并的数据块
                            failed_reservations: failed_reservation_infos, // 包含所有记录的失败预留信息
                        };

                        debug!(
                             "(Manager) Finalize: 为失败 Group {} 构建 FailedGroupData ({} 块, {} 失败信息)",
                             group_id_to_process, failed_data.group_chunks.len(), failed_data.failed_reservations.len()
                         );

                        // 尝试发送失败数据到失败通道
                        if !self.failed_data_tx.is_closed() {
                            // 需要克隆数据，因为如果发送失败，还需要放入报告
                            if let Err(send_error) =
                                self.failed_data_tx.try_send(failed_data.clone())
                            {
                                // 使用 try_send 避免阻塞
                                error!(
                                     "(Manager) Finalize: 发送失败 Group {} 的 FailedGroupData 到通道失败: {}",
                                     group_id_to_process, send_error
                                 );
                                // 发送失败，将数据添加到报告列表
                                failed_groups_for_report.push(failed_data);
                                debug!("(Manager) Finalize: 失败 Group {} 数据已添加到最终报告 (因通道发送失败)", group_id_to_process);
                            } else {
                                info!("(Manager) Finalize: 失败 Group {} 的 FailedGroupData 已成功发送到失败通道。", group_id_to_process);
                                // 发送成功，则不包含在最终的报告中
                            }
                        } else {
                            warn!("(Manager) Finalize: 失败数据通道已关闭，无法发送 Group {} 的数据。", group_id_to_process);
                            // 通道关闭，将数据添加到报告列表
                            failed_groups_for_report.push(failed_data);
                            debug!("(Manager) Finalize: 失败 Group {} 数据已添加到最终报告 (因通道已关闭)", group_id_to_process);
                        }

                        // 清理处理完的失败分组 (无论发送是否成功，都从 Manager 移除)
                        if self.groups.remove(&group_id_to_process).is_some() {
                            debug!(
                                "(Manager) Finalize: 失败 Group {} 已从 Manager 移除。",
                                group_id_to_process
                            );
                        } else {
                            warn!(
                                "(Manager) Finalize: 尝试移除失败处理的 Group {} 时未找到。",
                                group_id_to_process
                            );
                        }
                    } else {
                        // 严重错误：处理失败后再次查找分组失败！无法生成失败报告。
                        error!(
                             "(Manager) Finalize: 处理失败后再次查找 Group {} 失败！无法生成失败报告，分组状态可能残留。",
                             group_id_to_process
                         );
                        // 即使找不到，也尝试调用 remove 以防万一
                        self.groups.remove(&group_id_to_process);
                    }
                } // End match process_result
            } // End loop group_ids
        }
        info!(
            "(Manager) Finalize 处理完成。最终报告将包含 {} 个未能通过通道发送的失败分组信息。",
            failed_groups_for_report.len()
        );

        // 重置 finalizing 标志 (如果需要 Manager 之后还能用的话，但通常 finalize 后就退出了)
        // self.is_finalizing = false; // 取决于设计，如果 finalize 后 Manager 退出，则不需要重置

        // 返回包含未发送失败数据的 FinalizeResult
        Some(FinalizeResult {
            failed: failed_groups_for_report,
        })
    }
}

// 在 src/manager/finalize.rs 文件末尾添加以下内容：

#[cfg(test)]
mod tests {
    use crate::{
        manager::Manager,
        types::{
            CommitType, FailedGroupDataTransmission, Request, ReserveRequest, SubmitBytesRequest,
            SuccessfulGroupData,
        },
    };
    use bytes::Bytes;
    use std::{collections::HashMap, num::NonZeroUsize};
    use tokio::sync::{mpsc, oneshot};

    // 辅助函数：创建一个测试用的 Manager 实例
    fn new_test_manager_for_finalize(
        min_group_commit_size: usize,
    ) -> (
        Manager,
        mpsc::Receiver<SuccessfulGroupData>,
        mpsc::Receiver<FailedGroupDataTransmission>,
    ) {
        let (_request_tx, request_rx) = mpsc::channel::<Request>(1);
        let (completed_data_tx, completed_data_rx) = mpsc::channel::<SuccessfulGroupData>(10);
        let (failed_data_tx, failed_data_rx) = mpsc::channel::<FailedGroupDataTransmission>(10);

        let manager = Manager {
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
        };
        (manager, completed_data_rx, failed_data_rx)
    }

    #[tokio::test]
    async fn test_finalize_internal_no_groups() {
        let (mut manager, _completed_rx, _failed_rx) = new_test_manager_for_finalize(100);
        let result = manager.finalize_internal().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_empty());
        assert!(manager.is_finalizing);
        assert!(manager.groups.is_empty());
    }

    #[tokio::test]
    async fn test_finalize_internal_with_uncommitted_reservation() {
        let (mut manager, _completed_rx, mut failed_rx) = new_test_manager_for_finalize(100);

        // 预留一个空间但不提交
        let (res_reply_tx, res_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(50).unwrap(),
            reply_tx: res_reply_tx,
        });
        let (res_id, offset, group_id) = res_reply_rx.await.unwrap().unwrap();

        assert!(manager.groups.contains_key(&group_id));

        let finalize_result_opt = manager.finalize_internal().await;
        assert!(
            manager.groups.is_empty(),
            "Finalize 后所有组都应被处理并移除"
        );

        // 检查失败通道
        match tokio::time::timeout(std::time::Duration::from_millis(100), failed_rx.recv()).await {
            Ok(Some(failed_group_data)) => {
                assert_eq!(failed_group_data.group_id, group_id);
                assert!(failed_group_data.group_chunks.is_empty()); // 没有提交数据
                assert_eq!(failed_group_data.failed_reservations.len(), 1);
                let failed_res = &failed_group_data.failed_reservations[0];
                assert_eq!(failed_res.id, res_id);
                assert_eq!(failed_res.offset, offset);
                assert_eq!(failed_res.size, 50);
            }
            _ => panic!("期望在失败通道收到一个失败分组数据"),
        }
        // FinalizeResult 报告应该为空，因为失败数据已通过通道发送
        assert!(finalize_result_opt.is_some() && finalize_result_opt.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_finalize_internal_with_committed_data_and_uncommitted() {
        let (mut manager, mut completed_rx, mut failed_rx) = new_test_manager_for_finalize(200); // 高阈值防止自动密封

        // 预留1 (将提交)
        let (res_reply_tx1, res_reply_rx1) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(30).unwrap(),
            reply_tx: res_reply_tx1,
        });
        let (res_id1, offset1, group_id) = res_reply_rx1.await.unwrap().unwrap();

        // 预留2 (不提交)
        let (res_reply_tx2, res_reply_rx2) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(20).unwrap(),
            reply_tx: res_reply_tx2,
        });
        let (res_id2, _offset2, group_id2) = res_reply_rx2.await.unwrap().unwrap();
        assert_eq!(group_id, group_id2); // 应该在同一个组

        // 提交预留1的数据
        let data1 = Bytes::from_static(b"committed_data_for_res1_0000000000");
        let (submit_reply_tx, submit_reply_rx) = oneshot::channel();
        manager
            .handle_submit_bytes(SubmitBytesRequest {
                reservation_id: res_id1,
                absolute_offset: offset1,
                group_id,
                data: CommitType::Single(data1.slice(0..30)), // 确保大小匹配
                reply_tx: submit_reply_tx,
            })
            .await;
        assert!(submit_reply_rx.await.unwrap().is_ok());

        let finalize_result_opt = manager.finalize_internal().await;
        assert!(manager.groups.is_empty());

        // 此分组包含已提交数据和未提交（失败）的预留，因此整个分组应视为失败
        match tokio::time::timeout(std::time::Duration::from_millis(100), failed_rx.recv()).await {
            Ok(Some(failed_group)) => {
                assert_eq!(failed_group.group_id, group_id);
                assert_eq!(failed_group.group_chunks.len(), 1); // res1 的数据块
                assert_eq!(failed_group.group_chunks.get(&offset1).unwrap().len(), 30);
                assert_eq!(failed_group.failed_reservations.len(), 1); // res2 的失败信息
                assert_eq!(failed_group.failed_reservations[0].id, res_id2);
            }
            _ => panic!("期望一个失败分组数据"),
        }
        // 不应有成功数据
        assert!(completed_rx.try_recv().is_err());
        assert!(finalize_result_opt.is_some() && finalize_result_opt.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_finalize_internal_failed_group_to_report_if_channel_closed() {
        let (mut manager, _completed_rx, failed_rx) = new_test_manager_for_finalize(100);
        drop(failed_rx); // 关闭失败数据通道的接收端

        // 预留一个空间但不提交
        let (res_reply_tx, res_reply_rx) = oneshot::channel();
        manager.handle_reserve(ReserveRequest {
            size: NonZeroUsize::new(50).unwrap(),
            reply_tx: res_reply_tx,
        });
        let (res_id, _offset, group_id) = res_reply_rx.await.unwrap().unwrap();

        let finalize_result_opt = manager.finalize_internal().await;
        assert!(manager.groups.is_empty());
        assert!(finalize_result_opt.is_some());
        let report = finalize_result_opt.unwrap();
        assert_eq!(report.failed_len(), 1); // 失败数据应在报告中
        let reported_failure = &report.failed[0];
        assert_eq!(reported_failure.group_id, group_id);
        assert_eq!(reported_failure.failed_reservations[0].id, res_id);
    }

    #[tokio::test]
    async fn test_finalize_internal_already_finalizing() {
        let (mut manager, _, _) = new_test_manager_for_finalize(100);
        manager.is_finalizing = true; // 手动设置为正在 finalize

        let result = manager.finalize_internal().await;
        assert!(result.is_none(), "如果已在 finalize，应返回 None");
        assert!(manager.is_finalizing); // 状态应保持
    }
}
