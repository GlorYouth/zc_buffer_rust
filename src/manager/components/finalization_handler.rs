// src/manager/components/finalization_handler.rs
//! 定义了用于处理 Manager Finalize 期间所有剩余分组的 Trait。

use crate::manager::components::group_data_processor::GroupDataProcessor;
use crate::manager::components::group_lifecycle::GroupLifecycleManager;
use crate::manager::operations::group_processing::try_process_taken_group_state;
// 确保引入
use crate::types::{
    AbsoluteOffset, FailedGroupData, FailedGroupDataTransmission, FailedReservationInfo,
    FinalizeResult, GroupId, ReservationId, SuccessfulGroupData,
};
// 引入共享处理函数
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::mem;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[async_trait]
pub(crate) trait FinalizationHandler {
    /// 处理所有剩余的分组，执行必要的清理和报告。
    ///
    /// # Arguments
    /// * `glm` - GroupLifecycleManager 的可变引用。
    /// * `processor` - GroupDataProcessor 的引用。
    /// * `completed_data_tx` - 成功数据发送通道的引用。
    /// * `failed_data_tx` - 失败数据发送通道的引用。
    ///
    /// # Returns
    /// `FinalizeResult` 包含了那些未能通过 `failed_data_tx` 发送的失败分组信息。
    async fn finalize_all_groups(
        &self, // 通常 FinalizationHandler 本身是无状态的，但trait方法可能需要&mut self以备将来扩展
        glm: &mut (dyn GroupLifecycleManager + Send), // Send约束以匹配Manager字段
        processor: &(dyn GroupDataProcessor + Send + Sync),
        completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
        failed_data_tx: &mpsc::Sender<FailedGroupDataTransmission>,
    ) -> FinalizeResult;
}
/// `FinalizationHandler` Trait的默认实现。
pub(crate) struct DefaultFinalizationHandler;

impl DefaultFinalizationHandler {
    pub fn new() -> Self {
        DefaultFinalizationHandler {}
    }
}

#[async_trait]
impl FinalizationHandler for DefaultFinalizationHandler {
    async fn finalize_all_groups(
        &self,
        glm: &mut (dyn GroupLifecycleManager + Send),
        processor: &(dyn GroupDataProcessor + Send + Sync),
        completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
        failed_data_tx: &mpsc::Sender<FailedGroupDataTransmission>,
    ) -> FinalizeResult {
        info!("(FinalizationHandler) 开始处理所有剩余分组...");
        let mut failed_groups_for_report: Vec<FailedGroupData> = Vec::new();
        let group_ids: Vec<GroupId> = glm.all_group_ids();
        info!(
            "(FinalizationHandler) Finalize: 将处理 {} 个剩余分组: {:?}",
            group_ids.len(),
            group_ids
        );

        for group_id in group_ids {
            // 检查分组是否仍然存在于 GLM 中
            if glm.get_group_ref(group_id).is_none() {
                warn!(
                    "(FinalizationHandler) Finalize: 迭代时 Group ID {} 已不存在于 GLM，跳过。",
                    group_id
                );
                continue;
            }

            // 1. 强制密封
            if let Err(e) = glm.force_seal_group(group_id) {
                warn!(
                    "(FinalizationHandler) Finalize: 强制密封 Group {} 失败: {:?}, 跳过。",
                    group_id, e
                );
                continue;
            }

            // 2. 处理仍在 reservations 中的预留 (视为失败)
            // 需要 get_group_mut 来修改 reservations 和 failed_infos
            if let Some(group_mut_ref) = glm.get_group_mut(group_id) {
                let remaining_res_ids: Vec<ReservationId> =
                    group_mut_ref.reservations.drain().collect();
                if !remaining_res_ids.is_empty() {
                    warn!("(FinalizationHandler) Finalize: Group {} 发现 {} 个未提交的预留: {:?}，将标记为失败", group_id, remaining_res_ids.len(), remaining_res_ids);
                    for res_id_to_fail in remaining_res_ids {
                        // 从元数据获取偏移和大小
                        if let Some((offset, size)) =
                            group_mut_ref.get_reservation_metadata(res_id_to_fail)
                        {
                            group_mut_ref.add_failed_info(FailedReservationInfo {
                                id: res_id_to_fail,
                                group_id,
                                offset,
                                size,
                            });
                        } else {
                            // 如果元数据也没有，这是一个更严重的问题，但仍需记录失败
                            error!("(FinalizationHandler) Finalize: 严重错误！未提交的 Res {} 在 Group {} 的元数据中也找不到！", res_id_to_fail, group_id);
                            group_mut_ref.add_failed_info(FailedReservationInfo {
                                id: res_id_to_fail,
                                group_id,
                                offset: AbsoluteOffset::MAX,
                                size: 0,
                            });
                        }
                    }
                }
            } else {
                warn!("(FinalizationHandler) Finalize: 获取 Group {} 以处理未提交预留时未找到（可能已被并发移除）。", group_id);
                // 如果分组找不到了，尝试 take 它然后处理，如果 take 也失败，则跳过。
            }

            // 3. 从 GLM 中 `take` 分组状态以进行处理
            let group_state_option = glm.take_group(group_id);

            if let Some(taken_group_state) = group_state_option {
                // 4. 调用共享的处理函数
                match try_process_taken_group_state(
                    group_id,
                    taken_group_state,
                    glm,
                    processor,
                    completed_data_tx,
                )
                .await
                {
                    Ok(processed_and_consumed) => {
                        if processed_and_consumed {
                            info!("(FinalizationHandler) Finalize: Group {} 在 Finalize 期间成功处理 (已发送或为空)。", group_id);
                        } else {
                            // `try_process_taken_group_state` 返回 Ok(false) 的情况理论上不应在此发生，
                            // 因为我们已经 take 了 state。如果发生，说明逻辑有误。
                            warn!("(FinalizationHandler) Finalize: Group {} 处理返回未处理 (逻辑意外)。", group_id);
                        }
                    }
                    Err(_manager_error) => {
                        // _manager_error 包含了处理失败的原因
                        // try_process_taken_group_state 内部在 Err 时已将 group_state 放回 GLM。
                        // 现在我们需要再次 take 它来构建 FailedGroupData。
                        warn!("(FinalizationHandler) Finalize: Group {} 处理失败 ({:?})，判定为失败分组。", group_id, _manager_error);

                        if let Some(mut failed_group_state_after_processing_failure) =
                            glm.take_group(group_id)
                        {
                            // 确保 failed_group_state_after_processing_failure 包含了所有最新的信息
                            // 例如，如果 try_process_taken_group_state 的错误是 GroupContainsFailures，
                            // 那么 failed_infos 应该在 failed_group_state_after_processing_failure 中。

                            let committed_data_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure.committed_data,
                            );
                            let failed_reservation_infos_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure.failed_infos,
                            );
                            // 元数据也取走，即使它在 try_process_taken_group_state 中可能被放回过
                            let _metadata_for_failure = mem::take(
                                &mut failed_group_state_after_processing_failure
                                    .reservation_metadata,
                            );

                            let group_chunks_for_failure: BTreeMap<AbsoluteOffset, Bytes> =
                                committed_data_for_failure
                                    .into_iter()
                                    .flat_map(|(res_start_offset, (_res_id, _res_size, chunks))| {
                                        // chunks 的 key 是相对于预留的偏移 (usize)
                                        // 我们需要将它们转换为相对于分组起始的绝对偏移，但这需要知道分组的起始。
                                        // FailedGroupData 的 group_chunks key 是块的绝对起始偏移。
                                        // res_start_offset 本身就是预留的绝对起始偏移。
                                        // 而内层 BTreeMap 的 key 是块相对于该预留的偏移。
                                        // 所以块的绝对偏移 = res_start_offset + chunk_relative_offset
                                        // 但目前 chunks 的 key 就是 usize 相对偏移，所以可以直接用。
                                        //  **** 修正逻辑 ****
                                        // `committed_data` 的内层 BTreeMap 的 key 是 `usize`，代表块相对于其预留的偏移。
                                        // 所以，一个块的绝对偏移是 `res_start_offset` (预留的绝对偏移) + `chunk_relative_offset`。
                                        // `FailedGroupData.group_chunks` 的 key 是块的绝对偏移。
                                        let mut absolute_chunk_map = BTreeMap::new();
                                        for (chunk_relative_offset, bytes) in chunks {
                                            absolute_chunk_map.insert(
                                                res_start_offset + chunk_relative_offset,
                                                bytes,
                                            );
                                        }
                                        absolute_chunk_map // 返回 (绝对偏移, Bytes) 的迭代器
                                    })
                                    .collect();

                            let failed_data = FailedGroupData {
                                group_id,
                                group_chunks: group_chunks_for_failure,
                                failed_reservations: failed_reservation_infos_for_failure,
                            };
                            debug!("(FinalizationHandler) Finalize: 为失败 Group {} 构建 FailedGroupData ({} 块, {} 失败信息)", group_id, failed_data.group_chunks.len(), failed_data.failed_reservations.len());

                            if !failed_data_tx.is_closed() {
                                if let Err(send_error) =
                                    failed_data_tx.try_send(failed_data.clone())
                                {
                                    // 使用 try_send
                                    error!("(FinalizationHandler) Finalize: 发送失败 Group {} 的 FailedGroupData 到通道失败: {}", group_id, send_error);
                                    failed_groups_for_report.push(failed_data);
                                } else {
                                    info!("(FinalizationHandler) Finalize: 失败 Group {} 的 FailedGroupData 已成功发送到失败通道。", group_id);
                                }
                            } else {
                                warn!("(FinalizationHandler) Finalize: 失败数据通道已关闭，无法发送 Group {} 的数据。", group_id);
                                failed_groups_for_report.push(failed_data);
                            }
                        } else {
                            error!("(FinalizationHandler) Finalize: Group {} 处理失败后再次从GLM take Group失败！无法生成失败报告。", group_id);
                        }
                    }
                }
            } else {
                // 如果在标记未提交预留为失败后，take_group 失败，说明分组可能已被其他并发操作移除。
                warn!("(FinalizationHandler) Finalize: 尝试 take Group {} 以处理时未找到（在标记失败预留之后）。", group_id);
            }
        }
        info!("(FinalizationHandler) Finalize 处理完成。最终报告将包含 {} 个未能通过通道发送的失败分组信息。", failed_groups_for_report.len());
        FinalizeResult {
            failed: failed_groups_for_report,
        }
    }
}
