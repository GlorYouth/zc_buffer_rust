use crate::manager::components::group_data_processor::{
    GroupDataProcessor, ProcessedGroupOutcome, ProcessingError, ProcessorTaskError,
};
use crate::manager::components::group_lifecycle::GroupLifecycleManager;
use crate::manager::state::GroupState;
use crate::types::{GroupId, ReservationId};
use crate::{FailedReservationInfo, Manager, ManagerError, SuccessfulGroupData};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// 处理一个已从 GroupLifecycleManager `take` 出的分组状态。
///
/// 此函数会尝试验证、合并数据，并通过提供的通道发送成功或失败的结果。
///
/// # Arguments
/// * `group_id` - 分组ID。
/// * `mut group_state` - 从GLM取出的分组状态，此函数可能会消耗或修改它。
/// * `glm` - GroupLifecycleManager的引用，用于在处理失败时放回`group_state`。
/// * `processor` - GroupDataProcessor的引用，用于验证和合并数据。
/// * `completed_data_tx` - 成功数据发送通道。
///
/// # Returns
/// * `Ok(true)` - 分组成功处理并消耗 (数据已发送或为空分组)。
/// * `Ok(false)` - 分组由于某些原因未被处理（例如，预检查不通过，但此函数假设调用者已做了预检查）。
///                 实际上，此函数被调用时，分组总是会被尝试处理。
/// * `Err(ManagerError)` - 处理过程中发生错误，`group_state` 已通过 `glm` 被放回。
pub(crate) async fn try_process_taken_group_state(
    group_id: GroupId,
    mut group_state: GroupState, // 接收所有权，因为多数路径会消耗它或其内容
    glm: &mut dyn GroupLifecycleManager,
    processor: &(dyn GroupDataProcessor + Send + Sync), // Processor 也需要 Send + Sync
    completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
) -> Result<bool, ManagerError> {
    // true if consumed

    // 从 group_state 中提取数据。这些数据的所有权现在在这里。
    let current_committed_data = std::mem::take(&mut group_state.committed_data);
    let current_failed_infos = std::mem::take(&mut group_state.failed_infos);
    let current_reservation_metadata = std::mem::take(&mut group_state.reservation_metadata);

    info!(
        "(GroupProcessing) [try_process] 处理 Group {} ({}提交, {}失败, {}元数据)",
        group_id,
        current_committed_data.len(),
        current_failed_infos.len(),
        current_reservation_metadata.len()
    );

    // 1. 检查是否预先包含失败信息
    if !current_failed_infos.is_empty() {
        warn!(
            "(GroupProcessing) [try_process] Group {} 包含 {} 个失败预留，判定失败。",
            group_id,
            current_failed_infos.len()
        );
        // 将状态放回 GLM
        group_state.committed_data = current_committed_data;
        group_state.failed_infos = current_failed_infos;
        group_state.reservation_metadata = current_reservation_metadata;
        glm.insert_group(group_id, group_state);
        return Err(ManagerError::GroupContainsFailures(group_id));
    }

    // 2. 处理没有预先失败信息的分组
    if current_committed_data.is_empty() {
        debug!(
            "(GroupProcessing) [try_process] Group {} 无提交数据也无失败，视为空分组。",
            group_id
        );
        // 空分组被消耗，不放回 group_state
        return Ok(true);
    }

    // 3. 调用 GroupDataProcessor 处理
    // current_committed_data 的所有权转移给 processor
    match processor.process_group_data(
        group_id,
        current_committed_data,
        &current_reservation_metadata,
    ) {
        Ok(ProcessedGroupOutcome::Success(successful_data)) => {
            let (start_offset, ref payload) = successful_data;
            let data_size = payload.len();
            info!(
                "(GroupProcessing) [try_process] Group {} 数据由Processor成功合并，准备发送。",
                group_id
            );

            if let Err(send_err) = completed_data_tx.send(successful_data).await {
                error!(
                    "(GroupProcessing) [try_process] Group {} 发送成功数据失败: {}",
                    group_id, send_err
                );
                // 发送失败，committed_data 已被 processor 消耗。
                // group_state.committed_data 保持为空。
                group_state.failed_infos.push(FailedReservationInfo {
                    // failed_infos 之前是空的
                    id: ReservationId::MAX, // 特殊ID表示发送失败
                    group_id,
                    offset: start_offset,
                    size: data_size,
                });
                group_state.reservation_metadata = current_reservation_metadata; // 元数据放回
                glm.insert_group(group_id, group_state);
                Err(ManagerError::SendCompletionFailed(group_id))
            } else {
                info!(
                    "(GroupProcessing) [try_process] Group {} 成功发送给消费者。",
                    group_id
                );
                Ok(true) // 分组成功处理并消耗
            }
        }
        Ok(ProcessedGroupOutcome::Empty) => {
            debug!(
                "(GroupProcessing) [try_process] Group {} 由Processor处理后视为空。",
                group_id
            );
            Ok(true) // 分组成功处理并消耗
        }
        Err(processor_task_error) => {
            let ProcessorTaskError {
                error: processing_err,
                committed_data_on_failure,
            } = processor_task_error;
            warn!(
                "(GroupProcessing) [try_process] Group {} 数据由Processor处理失败: {:?}.",
                group_id, processing_err
            );

            // 将从 ProcessorTaskError 中获取的 committed_data 放回
            group_state.committed_data = committed_data_on_failure;
            group_state.reservation_metadata = current_reservation_metadata;
            // group_state.failed_infos 仍然是空的
            glm.insert_group(group_id, group_state);

            // 将 ProcessingError 转换为 ManagerError
            match processing_err {
                ProcessingError::NotContiguous(gid, exp, act) => {
                    Err(ManagerError::GroupNotContiguous(gid, exp, act))
                }
                ProcessingError::SizeMismatch(gid, exp, act) => {
                    Err(ManagerError::GroupSizeMismatch(gid, exp, act))
                }
                ProcessingError::OffsetMismatch(gid, exp, act) => {
                    Err(ManagerError::GroupOffsetMismatch(gid, exp, act))
                }
                ProcessingError::MergeFailure(gid, exp, act) => {
                    Err(ManagerError::MergeSizeMismatch(gid, exp, act))
                }
                ProcessingError::AttemptToProcessEmptyCommittedData(gid) => {
                    error!("(GroupProcessing) [try_process] Processor接到空 committed data for group {}, 这不应发生。", gid);
                    Err(ManagerError::Internal(format!(
                        "Processor error: attempt to process empty committed data for group {}",
                        gid
                    )))
                }
            }
        }
    }
}

impl Manager {
    pub(crate) async fn check_and_process_completed_group(
        &mut self,
        group_id: GroupId,
    ) -> Result<bool, ManagerError> {
        // 1. 预检查 (与之前相同)
        let can_process_and_exists = self
            .group_lifecycle_manager
            .get_group_ref(group_id)
            .map_or(false, |g_ref| g_ref.is_complete());

        if !can_process_and_exists {
            warn!(
                "(Manager) [check_and_process] 尝试处理的分组 {} 不存在或未完成。",
                group_id
            );
            return Ok(false);
        }

        // 2. 从 GLM 中 `take` 分组状态 (与之前相同)
        let mut group_state = match self.group_lifecycle_manager.take_group(group_id) {
            Some(state) => state,
            None => {
                error!("(Manager) [check_and_process] 在尝试从GLM take分组 {} 时发现其已被移除 (预检查后)", group_id);
                return Ok(false);
            }
        };

        // 3. 提取数据
        let committed_data_map = std::mem::take(&mut group_state.committed_data); // mut以便后续可能放回
        let failed_reservation_infos = std::mem::take(&mut group_state.failed_infos);
        let reservation_metadata = std::mem::take(&mut group_state.reservation_metadata); // 这个也会在失败时放回

        info!(
            "(Manager) [check_and_process] 检查取出的分组 {} 状态 (含 {} 提交项, {} 失败项, {} 元数据项)",
            group_id, committed_data_map.len(), failed_reservation_infos.len(), reservation_metadata.len()
        );

        // 4a. 检查是否已包含失败信息
        if !failed_reservation_infos.is_empty() {
            warn!(
                "(Manager) [check_and_process] 分组 {} 包含 {} 个失败预留，判定为处理失败。",
                group_id,
                failed_reservation_infos.len()
            );
            group_state.committed_data = committed_data_map; // 放回原始提交数据
            group_state.failed_infos = failed_reservation_infos;
            group_state.reservation_metadata = reservation_metadata;
            self.group_lifecycle_manager
                .insert_group(group_id, group_state);
            debug!(
                "(Manager) [check_and_process] 已将失败分组 {} 的状态放回 GLM",
                group_id
            );
            return Err(ManagerError::GroupContainsFailures(group_id));
        }

        // 4b. 处理没有预先失败信息的分组
        if committed_data_map.is_empty() {
            debug!(
                "(Manager) [check_and_process] 分组 {} 既无提交数据也无失败信息，视为空分组处理。",
                group_id
            );
            // 注意：此时 group_state 的 failed_infos 是空的
            // reservation_metadata 和 committed_data_map 也是空的（或已被 take）
            // 分组状态已被消耗，不放回
            return Ok(true);
        }

        // 4c. 调用 GroupDataProcessor
        // committed_data_map 在这里所有权转移给 processor
        match self.group_data_processor.process_group_data(
            group_id,
            committed_data_map,
            &reservation_metadata,
        ) {
            Ok(ProcessedGroupOutcome::Success(successful_data)) => {
                info!("(Manager) [check_and_process] 分组 {} 数据由 Processor 成功处理并合并，准备发送...", group_id);
                let (start_offset_for_send_failure, ref data_payload_for_send_failure) =
                    successful_data;
                let data_size_for_send_failure = data_payload_for_send_failure.len();

                if let Err(e) = self.completed_data_tx.send(successful_data).await {
                    error!(
                        "(Manager) [check_and_process] 发送完成的分组 {} 数据失败: {}",
                        group_id, e
                    );
                    // 发送失败，committed_data 已被 processor 消耗。
                    // group_state.committed_data 保持为空。
                    group_state.failed_infos.push(FailedReservationInfo {
                        // failed_infos 之前是空的
                        id: ReservationId::MAX,
                        group_id,
                        offset: start_offset_for_send_failure,
                        size: data_size_for_send_failure,
                    });
                    group_state.reservation_metadata = reservation_metadata; // 元数据放回
                    self.group_lifecycle_manager
                        .insert_group(group_id, group_state);
                    Err(ManagerError::SendCompletionFailed(group_id))
                } else {
                    info!(
                        "(Manager) [check_and_process] 分组 {} 已成功发送给消费者。",
                        group_id
                    );
                    Ok(true) // 分组状态已被消耗
                }
            }
            Ok(ProcessedGroupOutcome::Empty) => {
                debug!(
                    "(Manager) [check_and_process] 分组 {} 由 Processor 处理后视为空。",
                    group_id
                );
                Ok(true) // 分组状态已被消耗
            }
            Err(processor_task_error) => {
                // Processor 内部发生验证或合并错误
                let ProcessorTaskError {
                    error: processing_error,
                    committed_data_on_failure,
                } = processor_task_error;
                warn!("(Manager) [check_and_process] 分组 {} 数据由 Processor 处理失败: {:?}. 此分组处理失败。", group_id, processing_error);

                // 将从 ProcessorTaskError 中获取的 committed_data 放回
                group_state.committed_data = committed_data_on_failure;
                group_state.reservation_metadata = reservation_metadata;
                // group_state.failed_infos 仍然是空的（因为预检查时是空的，Processor 不直接修改它）
                self.group_lifecycle_manager
                    .insert_group(group_id, group_state);

                // 将 ProcessingError 转换为 ManagerError
                match processing_error {
                    ProcessingError::NotContiguous(gid, exp, act) => {
                        Err(ManagerError::GroupNotContiguous(gid, exp, act))
                    }
                    ProcessingError::SizeMismatch(gid, exp, act) => {
                        Err(ManagerError::GroupSizeMismatch(gid, exp, act))
                    }
                    ProcessingError::OffsetMismatch(gid, exp, act) => {
                        Err(ManagerError::GroupOffsetMismatch(gid, exp, act))
                    }
                    ProcessingError::MergeFailure(gid, exp, act) => {
                        Err(ManagerError::MergeSizeMismatch(gid, exp, act))
                    }
                    ProcessingError::AttemptToProcessEmptyCommittedData(gid) => {
                        // 这是一个内部逻辑问题，转换为 Internal ManagerError
                        error!("(Manager) [check_and_process] Processor 接到空 committed data for group {}, 这不应发生。", gid);
                        Err(ManagerError::Internal(format!(
                            "Processor error: attempt to process empty committed data for group {}",
                            gid
                        )))
                    }
                }
            }
        }
    }
}
