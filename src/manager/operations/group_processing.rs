//! 包含处理已完成或失败的数据分组 (`GroupState`) 的核心逻辑。
//! 主要提供 `try_process_taken_group_state` 函数，该函数由 `ManagerActor` 的
//! `check_and_process_completed_group` 方法和 `FinalizationHandler` 调用。

use crate::manager::components::group_data_processor::{
    GroupDataProcessor, ProcessedGroupOutcome, ProcessingError, ProcessorTaskError,
};
use crate::manager::components::group_lifecycle::GroupLifecycleManager;
use crate::manager::components::ManagerComponents;
use crate::manager::state::GroupState;
use crate::types::{GroupId, ReservationId};
use crate::{FailedReservationInfo, ManagerActor, ManagerError, SuccessfulGroupData};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// 尝试处理一个已从 `GroupLifecycleManager` `take` 出的分组状态 (`GroupState`)。
///
/// 这个函数是处理分组的核心逻辑，负责：
/// 1. 检查分组是否已包含失败信息 (`failed_infos`)。
/// 2. 如果没有失败信息且提交数据 (`committed_data`) 为空，则视为空分组。
/// 3. 如果没有失败信息且有提交数据，则调用 `GroupDataProcessor` 进行验证和合并。
/// 4. 根据 `GroupDataProcessor` 的结果：
///    - 如果成功合并 (`Ok(Success)`)，则尝试通过 `completed_data_tx` 发送成功数据。
///    - 如果处理器视为空 (`Ok(Empty)`)，则视为成功处理。
///    - 如果处理器返回错误 (`Err`)，则将错误转换为 `ManagerError`。
/// 5. 在处理失败（包括预检查失败、处理器失败、发送失败）的情况下，
///    将（可能已被修改的）`GroupState` 通过 `glm.insert_group` 放回 `GroupLifecycleManager` 中，
///    以便后续 Finalize 处理。
///
/// # Arguments
/// * `group_id` - 正在处理的分组的 ID。
/// * `group_state` - 从 `GroupLifecycleManager` 中 `take` 出来的分组状态。 **此函数会获得其所有权**。
/// * `glm` - 对实现了 `GroupLifecycleManager` Trait 的组件的可变引用，用于在失败时放回 `group_state`。
/// * `processor` - 对实现了 `GroupDataProcessor` Trait 的组件的引用，用于处理提交的数据。
/// * `completed_data_tx` - 用于发送成功合并数据的 MPSC 通道 Sender。
///
/// # Returns
/// * `Ok(true)`: 分组被成功处理并 **消耗** (数据已发送，或者分组为空)。`group_state` 不会被放回。
/// * `Ok(false)`: (理论上不应从此函数返回) 表示分组由于某种原因未被处理。
/// * `Err(ManagerError)`: 处理过程中发生错误。`group_state` **已被** 放回 `glm` 中。
///   具体的 `ManagerError` 类型指示了失败的原因（例如 `GroupContainsFailures`, `SendCompletionFailed`, 或来自处理器的错误）。
pub(crate) async fn try_process_taken_group_state(
    group_id: GroupId,
    mut group_state: GroupState, // 接收所有权，因为需要修改或消耗它
    glm: &mut dyn GroupLifecycleManager, // 可变引用，用于 insert_group
    processor: &(dyn GroupDataProcessor + Send + Sync), // 需要 Send + Sync 以支持并发
    completed_data_tx: &mpsc::Sender<SuccessfulGroupData>,
) -> Result<bool, ManagerError> { // 返回 Result<consumed, error>
    trace!("(GroupProcessing) [try_process] 开始处理取出的 Group {} 状态", group_id);

    // --- 从 group_state 中提取字段所有权，以便在需要时放回 --- 
    // 使用 mem::take 获取字段的所有权，将 group_state 中的字段替换为空集合。
    // 这样做是为了避免在后续处理中（特别是调用 processor）意外地持有对 group_state 的引用。
    let current_committed_data = std::mem::take(&mut group_state.committed_data);
    let current_failed_infos = std::mem::take(&mut group_state.failed_infos);
    let current_reservation_metadata = std::mem::take(&mut group_state.reservation_metadata);
    // group_state.reservations 和 group_state.is_sealed 通常在 take 时已被处理或不再相关

    debug!(
        "(GroupProcessing) [try_process] 处理 Group {} (包含 {} 提交项, {} 失败项, {} 元数据项)",
        group_id,
        current_committed_data.len(),
        current_failed_infos.len(),
        current_reservation_metadata.len()
    );

    // --- 步骤 1: 检查分组是否已包含失败信息 --- 
    if !current_failed_infos.is_empty() {
        warn!(
            "(GroupProcessing) [try_process] Group {} 包含 {} 个失败预留，判定为处理失败。将状态放回 GLM。",
            group_id,
            current_failed_infos.len()
        );
        // 将提取出的字段放回 group_state
        group_state.committed_data = current_committed_data;
        group_state.failed_infos = current_failed_infos;
        group_state.reservation_metadata = current_reservation_metadata;
        // 将 group_state 重新插入 GLM
        glm.insert_group(group_id, group_state);
        trace!("(GroupProcessing) [try_process] 失败的 Group {} 状态已放回 GLM。", group_id);
        // 返回包含失败信息的错误
        return Err(ManagerError::GroupContainsFailures(group_id));
    }

    // --- 步骤 2: 处理没有预先失败信息的分组 --- 
    // 检查是否有已提交的数据
    if current_committed_data.is_empty() {
        debug!(
            "(GroupProcessing) [try_process] Group {} 无提交数据也无失败信息，视为空分组并消耗。",
            group_id
        );
        // 空分组被成功处理并消耗，无需放回任何状态
        // (group_state 对象在此作用域结束时被丢弃)
        return Ok(true); // 返回 true 表示已消耗
    }

    // --- 步骤 3: 调用 GroupDataProcessor 处理提交的数据 --- 
    // 此时，current_failed_infos 为空，current_committed_data 不为空。
    // 将 current_committed_data 的所有权转移给 processor。
    trace!("(GroupProcessing) [try_process] Group {} 有提交数据且无失败，调用 GroupDataProcessor...", group_id);
    match processor.process_group_data(
        group_id,
        current_committed_data, // 移交所有权
        &current_reservation_metadata, // 元数据传递引用
    ) {
        // --- 情况 3a: Processor 成功处理并合并数据 --- 
        Ok(ProcessedGroupOutcome::Success(successful_data)) => {
            let (start_offset, ref payload) = successful_data;
            let data_size = payload.len();
            info!(
                "(GroupProcessing) [try_process] Group {} 数据由 Processor 成功合并 (Offset: {}, Size: {})，尝试发送...",
                group_id, start_offset, data_size
            );

            // --- 步骤 3a.i: 尝试发送成功合并的数据 --- 
            if let Err(send_err) = completed_data_tx.send(successful_data).await {
                // 发送失败！
                error!(
                    "(GroupProcessing) [try_process] Group {} 发送成功合并的数据到通道失败: {}。将状态标记为失败并放回 GLM。",
                    group_id, send_err
                );
                // 需要将 group_state 放回 GLM，并标记为失败。
                // committed_data 已被 processor 消耗，不能放回。
                // failed_infos 之前是空的，现在需要添加一个表示发送失败的条目。
                group_state.failed_infos.push(FailedReservationInfo {
                    id: ReservationId::MAX, // 使用特殊 ID 表示整个分组发送失败
                    group_id,
                    offset: start_offset, // 记录原分组的起始偏移
                    size: data_size,      // 记录原分组的大小
                });
                // 将元数据放回
                group_state.reservation_metadata = current_reservation_metadata;
                // 将标记为失败的 group_state 重新插入 GLM
                glm.insert_group(group_id, group_state);
                trace!("(GroupProcessing) [try_process] 发送失败的 Group {} 状态已放回 GLM。", group_id);
                // 返回发送失败错误
                Err(ManagerError::SendCompletionFailed(group_id))
            } else {
                // 发送成功！
                info!(
                    "(GroupProcessing) [try_process] Group {} 成功合并的数据已发送给消费者。分组处理完成并消耗。",
                    group_id
                );
                // 分组成功处理并消耗，group_state 在此丢弃
                Ok(true) // 返回 true 表示已消耗
            }
        }
        // --- 情况 3b: Processor 认为分组为空 (例如，只包含元数据但无实际提交) --- 
        Ok(ProcessedGroupOutcome::Empty) => {
            debug!(
                "(GroupProcessing) [try_process] Group {} 由 Processor 处理后视为空分组并消耗。",
                group_id
            );
            // 分组被成功处理并消耗，group_state 在此丢弃
            Ok(true) // 返回 true 表示已消耗
        }
        // --- 情况 3c: Processor 处理失败 --- 
        Err(processor_task_error) => {
            // 从错误中解构出具体的 ProcessingError 和可能未被消耗的 committed_data
            let ProcessorTaskError {
                error: processing_err,
                committed_data_on_failure,
            } = processor_task_error;
            warn!(
                "(GroupProcessing) [try_process] Group {} 数据由 Processor 处理失败: {:?}。将状态放回 GLM。",
                group_id, processing_err
            );

            // 将 Processor 返回的 (可能未消耗的) committed_data 放回 group_state
            group_state.committed_data = committed_data_on_failure;
            // 将元数据放回 group_state
            group_state.reservation_metadata = current_reservation_metadata;
            // failed_infos 仍然是空的
            // 将包含原始（或部分）数据的 group_state 重新插入 GLM
            glm.insert_group(group_id, group_state);
            trace!("(GroupProcessing) [try_process] Processor 处理失败的 Group {} 状态已放回 GLM。", group_id);

            // 将 ProcessingError 转换为相应的 ManagerError 并返回
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
                    // 合并失败通常也是内部错误，但有特定错误类型
                    Err(ManagerError::MergeSizeMismatch(gid, exp, act))
                }
                ProcessingError::AttemptToProcessEmptyCommittedData(gid) => {
                    // 这个错误表示调用此函数或 Processor 实现存在逻辑问题
                    error!("(GroupProcessing) [try_process] 内部逻辑错误：Processor 报告为 Group {} 处理了空的 committed_data！", gid);
                    Err(ManagerError::Internal(format!(
                        "Processor logic error: attempt to process empty committed data for group {}",
                        gid
                    )))
                }
            }
        }
    }
}

impl<C: ManagerComponents> ManagerActor<C> {
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
