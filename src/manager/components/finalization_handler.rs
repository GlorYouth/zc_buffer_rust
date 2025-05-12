// src/manager/components/finalization_handler.rs
//! 定义了用于处理 Manager Finalize 期间所有剩余分组的 Trait (`FinalizationHandler`) 及其默认实现。

use crate::manager::components::group_data_processor::GroupDataProcessor;
use crate::manager::components::group_lifecycle::GroupLifecycleManager;
use crate::manager::operations::group_processing::try_process_taken_group_state; // 导入共享的分组处理逻辑
use crate::types::{ // 引入所需的类型
    AbsoluteOffset, FailedGroupData, FailedGroupDataTransmission, FailedReservationInfo,
    FinalizeResult, GroupId, ReservationId, SuccessfulGroupData,
};
use async_trait::async_trait; // 用于在 Trait 中定义异步方法
use bytes::Bytes; // 引入 Bytes 类型
use std::collections::BTreeMap; // 引入有序映射
use std::mem; // 引入 mem::take 用于获取字段所有权
use tokio::sync::mpsc; // 引入 MPSC 通道
use tracing::{debug, error, info, warn}; // 引入日志宏

/// 处理 Manager Finalize 阶段的 Trait。
///
/// 当 `ManagerActor` 收到 `Finalize` 请求或所有 `ZeroCopyHandle` 被 Drop 时，
/// 会调用此 Trait 的实现来处理所有当前存在于 `GroupLifecycleManager` 中的分组。
/// 目的是确保所有预留都有最终状态（成功或失败），并将结果（成功合并的数据或失败信息）
/// 发送给相应的消费者通道，同时生成一份包含无法发送的失败信息的 `FinalizeResult` 报告。
#[async_trait]
pub trait FinalizationHandler: Send + Sync + 'static { // 要求线程安全且静态生命周期
    /// 处理所有剩余的分组，执行清理和报告。
    ///
    /// 该方法会迭代 `GroupLifecycleManager` 中的所有分组 ID，并对每个分组执行以下操作：
    /// 1. **强制密封**: 确保分组处于密封状态，即使其大小未达到阈值。
    /// 2. **处理未完成预留**: 将分组状态中 `reservations` 集合内仍然存在的预留 ID 视为失败，
    ///    查找它们的元数据（偏移量和大小），创建 `FailedReservationInfo` 并添加到分组的 `failed_infos` 列表。
    /// 3. **获取分组状态**: 从 `GroupLifecycleManager` 中 `take` 出分组的状态 (`GroupState`)。
    /// 4. **尝试处理**: 调用共享的 `try_process_taken_group_state` 函数处理获取到的 `GroupState`。
    ///    - 如果成功处理（发送了成功数据或确定为空），则记录日志。
    ///    - 如果处理失败（例如内部包含失败信息），则判定为失败分组。
    /// 5. **处理失败分组**: 如果步骤 4 判定为失败，再次 `take` 该分组的状态（因为 `try_process...` 失败时会放回），
    ///    提取其已提交的数据块 (`committed_data`) 和失败预留信息 (`failed_infos`)，
    ///    构建 `FailedGroupData` 结构体。
    /// 6. **发送失败数据**: 尝试通过 `failed_data_tx` 通道发送构建好的 `FailedGroupData`。
    /// 7. **记录报告**: 如果发送失败（例如通道已关闭或阻塞），则将 `FailedGroupData` 添加到最终的 `FinalizeResult` 报告中。
    ///
    /// # Arguments
    /// * `&self` - FinalizationHandler 实例的引用 (通常无状态)。
    /// * `glm` - 对 `GroupLifecycleManager` 实现的可变引用，用于获取、修改和移除分组状态。
    /// * `processor` - 对 `GroupDataProcessor` 实现的引用，用于在处理成功分组时合并数据。
    /// * `completed_data_tx` - 对成功数据发送通道 (`mpsc::Sender<SuccessfulGroupData>`) 的引用。
    /// * `failed_data_tx` - 对失败数据发送通道 (`mpsc::Sender<FailedGroupDataTransmission>`) 的引用。
    ///
    /// # Returns
    /// * `FinalizeResult` - 包含所有处理失败且 **未能** 通过 `failed_data_tx` 发送出去的失败分组信息 (`FailedGroupData`) 的集合。
    async fn finalize_all_groups(
        &self, // 通常 FinalizationHandler 本身是无状态的
        glm: &mut (dyn GroupLifecycleManager + Send), // 需要可变引用来修改和 take 分组状态
        processor: &(dyn GroupDataProcessor + Send + Sync), // 需要 Sync 因为可能被并发访问
        completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
        failed_data_tx: &mpsc::Sender<FailedGroupDataTransmission>,
    ) -> FinalizeResult;
}

/// `FinalizationHandler` Trait 的默认实现。
/// 提供了一个标准的 Finalize 处理逻辑。
pub struct DefaultFinalizationHandler;

impl DefaultFinalizationHandler {
    /// 创建 `DefaultFinalizationHandler` 的新实例。
    pub fn new() -> Self {
        DefaultFinalizationHandler {}
    }
}

#[async_trait]
impl FinalizationHandler for DefaultFinalizationHandler {
    /// `DefaultFinalizationHandler` 对 `finalize_all_groups` 的具体实现。
    async fn finalize_all_groups(
        &self,
        glm: &mut (dyn GroupLifecycleManager + Send),
        processor: &(dyn GroupDataProcessor + Send + Sync),
        completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
        failed_data_tx: &mpsc::Sender<FailedGroupDataTransmission>,
    ) -> FinalizeResult {
        info!("(FinalizationHandler) 开始 Finalize 过程，处理所有剩余分组...");
        // 用于收集无法通过通道发送的失败分组信息
        let mut failed_groups_for_report: Vec<FailedGroupData> = Vec::new();
        // 获取当前 GroupLifecycleManager 中所有分组的 ID 列表
        // 注意：这里获取的是快照，如果在迭代过程中 GLM 被并发修改，某些 ID 可能失效
        let group_ids: Vec<GroupId> = glm.all_group_ids();
        info!(
            "(FinalizationHandler) Finalize: 发现 {} 个需要处理的分组: {:?}",
            group_ids.len(),
            group_ids
        );

        // 迭代处理每个分组 ID
        for group_id in group_ids {
            debug!("(FinalizationHandler) Finalize: 正在处理 Group {}", group_id);

            // 预检查：在尝试任何操作前，再次确认分组是否存在于 GLM 中
            // 这可以减少后续操作因分组已被移除而失败的可能性
            if glm.get_group_ref(group_id).is_none() {
                warn!(
                    "(FinalizationHandler) Finalize: 在处理开始时 Group ID {} 已不存在于 GLM，跳过。",
                    group_id
                );
                continue; // 处理下一个分组 ID
            }

            // --- 步骤 1: 强制密封分组 ---
            // 确保分组进入可以被处理的状态
            debug!("(FinalizationHandler) Finalize: 尝试强制密封 Group {}", group_id);
            if let Err(e) = glm.force_seal_group(group_id) {
                // 如果强制密封失败（例如分组已被移除），记录警告并跳过
                warn!(
                    "(FinalizationHandler) Finalize: 强制密封 Group {} 失败: {:?}。可能已被并发移除，跳过处理。",
                    group_id, e
                );
                continue;
            }
            debug!("(FinalizationHandler) Finalize: Group {} 已强制密封", group_id);

            // --- 步骤 2: 处理未完成的预留 (标记为失败) ---
            // 获取对分组状态的可变引用，以处理其内部的 `reservations` 和 `failed_infos`
            if let Some(group_mut_ref) = glm.get_group_mut(group_id) {
                // 使用 drain() 从 `reservations` 集合中移除所有剩余的 ID，并收集它们
                let remaining_res_ids: Vec<ReservationId> = group_mut_ref.reservations.drain().collect();

                // 如果存在未完成的预留
                if !remaining_res_ids.is_empty() {
                    warn!("(FinalizationHandler) Finalize: Group {} 发现 {} 个未提交/未完成的预留: {:?}，将标记为失败", group_id, remaining_res_ids.len(), remaining_res_ids);
                    // 遍历每个未完成的预留 ID
                    for res_id_to_fail in remaining_res_ids {
                        // 尝试从元数据中获取该预留的偏移量和大小
                        if let Some((offset, size)) = group_mut_ref.get_reservation_metadata(res_id_to_fail) {
                            // 如果找到元数据，创建 FailedReservationInfo 并添加到 failed_infos
                            debug!("(FinalizationHandler) Finalize: 为未完成的 Res {} (Offset: {}, Size: {}) 创建失败信息", res_id_to_fail, offset, size);
                            group_mut_ref.add_failed_info(FailedReservationInfo {
                                id: res_id_to_fail,
                                group_id, // 关联的分组 ID
                                offset,   // 预留的偏移量
                                size,     // 预留的大小
                            });
                        } else {
                            // 严重错误：预留存在于 `reservations` 中，但其元数据却丢失了！
                            // 这通常不应该发生，表明状态管理可能存在问题。
                            // 记录错误，并使用无效值创建失败信息以尽量保留记录。
                            error!("(FinalizationHandler) Finalize: 严重错误！未完成的 Res {} 在 Group {} 的元数据中也找不到！", res_id_to_fail, group_id);
                            group_mut_ref.add_failed_info(FailedReservationInfo {
                                id: res_id_to_fail,
                                group_id,
                                offset: AbsoluteOffset::MAX, // 使用无效偏移量
                                size: 0,                     // 使用无效大小
                            });
                        }
                    }
                }
            } else {
                // 如果在强制密封后无法获取可变引用，说明分组可能刚好被其他任务移除了
                warn!("(FinalizationHandler) Finalize: 获取 Group {} 以处理未提交预留时未找到。可能已被并发移除，尝试继续 take。", group_id);
                // 即使无法标记失败预留，仍然尝试继续执行 take 和处理
            }

            // --- 步骤 3: 从 GLM 中 `take` 分组状态 --- 
            // `take_group` 会尝试移除并返回 GroupState 的所有权
            debug!("(FinalizationHandler) Finalize: 尝试 take Group {} 的状态", group_id);
            let group_state_option = glm.take_group(group_id);

            // --- 步骤 4 & 5 & 6 & 7: 处理获取到的分组状态 --- 
            if let Some(taken_group_state) = group_state_option {
                debug!("(FinalizationHandler) Finalize: 成功 take Group {} 状态，调用 try_process_taken_group_state...", group_id);
                // 调用位于 manager::operations::group_processing 中的共享处理逻辑
                match try_process_taken_group_state(
                    group_id,
                    taken_group_state, // 移交 GroupState 的所有权
                    glm, // 传入 GLM 引用 (处理失败时可能需要放回状态)
                    processor, // 传入数据处理器引用
                    completed_data_tx, // 传入成功数据通道引用
                ).await // 处理逻辑是异步的
                {
                    Ok(processed_and_consumed) => {
                        // 处理成功
                        if processed_and_consumed {
                            // 分组已成功处理并发送（或为空）
                            info!("(FinalizationHandler) Finalize: Group {} 在 Finalize 期间被成功处理（数据已发送或为空）。", group_id);
                        } else {
                            // 理论上不应发生：try_process... 返回 Ok(false) 意味着状态被放回了，
                            // 但我们已经 take 了状态。这可能指示一个逻辑错误。
                            error!("(FinalizationHandler) Finalize: Group {} 处理意外返回 Ok(false) 状态！", group_id);
                            // 尽管意外，但继续下一个分组
                        }
                    }
                    Err(manager_error) => {
                        // 处理失败，分组状态已被 try_process... 放回 GLM
                        warn!("(FinalizationHandler) Finalize: Group {} 处理失败 ({:?})，判定为失败分组。将构建并发送失败数据。", group_id, manager_error);

                        // --- 步骤 5 (续): 再次 take 失败分组的状态以构建 FailedGroupData ---
                        if let Some(mut failed_group_state_after_processing_failure) =
                            glm.take_group(group_id)
                        {
                            debug!("(FinalizationHandler) Finalize: 成功 take 失败的 Group {} 状态以构建报告。", group_id);
                            // 从失败的分组状态中提取所需信息
                            // 使用 mem::take 获取字段所有权，避免克隆大型数据结构
                            let committed_data_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure.committed_data,
                            );
                            let failed_reservation_infos_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure.failed_infos,
                            );
                            // 元数据也取走并丢弃，确保 GroupState 清空
                            let _metadata_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure.reservation_metadata,
                            );

                            // 将 committed_data (按预留偏移组织) 转换为 group_chunks (按块绝对偏移组织)
                            let group_chunks_for_failure: BTreeMap<AbsoluteOffset, Bytes> =
                                committed_data_for_failure
                                    .into_iter()
                                    .flat_map(|(res_start_offset, (_res_id, _res_size, chunks))| {
                                        // `chunks` 的 Key 是块相对于预留 `res_start_offset` 的偏移 (usize)
                                        // 需要转换为块的绝对偏移 = res_start_offset + chunk_relative_offset
                                        let mut absolute_chunk_map = BTreeMap::new();
                                        for (chunk_relative_offset, bytes) in chunks {
                                            absolute_chunk_map.insert(
                                                res_start_offset + chunk_relative_offset, // 计算绝对偏移
                                                bytes,
                                            );
                                        }
                                        absolute_chunk_map // flat_map 需要返回一个迭代器
                                    })
                                    .collect(); // 收集成 BTreeMap<AbsoluteOffset, Bytes>

                            // 构建 FailedGroupData 结构体
                            let failed_data = FailedGroupData {
                                group_id,
                                group_chunks: group_chunks_for_failure, // 包含所有成功提交的块
                                failed_reservations: failed_reservation_infos_for_failure, // 包含所有失败预留的信息
                            };
                            debug!("(FinalizationHandler) Finalize: 为失败 Group {} 构建 FailedGroupData ({} 个块, {} 个失败信息)", group_id, failed_data.group_chunks.len(), failed_data.failed_reservations.len());

                            // --- 步骤 6 & 7: 尝试发送失败数据，否则加入报告 ---
                            // 检查失败数据通道是否仍然打开
                            if !failed_data_tx.is_closed() {
                                // 使用 try_send 进行非阻塞发送尝试
                                // 在 Finalize 期间，下游消费者可能已经关闭或停止处理，
                                // 使用阻塞的 send 可能导致 Manager 卡死。
                                if let Err(send_error) =
                                    failed_data_tx.try_send(failed_data.clone()) // 克隆数据以备加入报告
                                {
                                    error!("(FinalizationHandler) Finalize: 发送失败 Group {} 的 FailedGroupData 到通道失败 (可能已满或关闭): {}。将加入报告。", group_id, send_error);
                                    // 发送失败，将数据加入最终报告
                                    failed_groups_for_report.push(failed_data);
                                } else {
                                    info!("(FinalizationHandler) Finalize: 失败 Group {} 的 FailedGroupData 已成功发送到失败通道。", group_id);
                                }
                            } else {
                                warn!("(FinalizationHandler) Finalize: 失败数据通道已关闭，无法发送 Group {} 的数据。将加入报告。", group_id);
                                // 通道已关闭，将数据加入最终报告
                                failed_groups_for_report.push(failed_data);
                            }
                        } else {
                            // 严重错误：处理失败后无法再次 take 分组状态！
                            // 这可能意味着 GLM 的状态不一致，或者 take 逻辑有问题。
                            error!("(FinalizationHandler) Finalize: 严重错误！Group {} 处理失败后无法再次从 GLM take 其状态！无法生成该分组的失败报告。", group_id);
                        }
                    }
                }
            } else {
                // 如果在标记未完成预留为失败后，take_group 仍然失败，
                // 说明分组在标记失败和 take 之间被并发移除了。
                warn!("(FinalizationHandler) Finalize: 尝试 take Group {} 以最终处理时失败（在标记失败预留后）。可能已被并发移除，跳过。", group_id);
            }
        }

        // Finalize 循环结束
        info!("(FinalizationHandler) Finalize 处理完成。最终报告将包含 {} 个未能通过通道发送的失败分组信息。", failed_groups_for_report.len());
        // 返回 FinalizeResult，其中包含所有未能成功发送到失败通道的 FailedGroupData
        FinalizeResult {
            failed: failed_groups_for_report,
        }
    }
}
