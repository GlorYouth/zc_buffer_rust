use crate::manager::components::group_lifecycle::GroupLifecycleManager;
use crate::manager::components::reservation_allocators::ReservationAllocator;
use crate::manager::components::ManagerComponents;
use crate::types::{CommitType, FailedInfoRequest, ReserveRequest, SubmitBytesRequest};
use crate::{ManagerActor, ManagerError};
use std::collections::BTreeMap;
use tracing::{debug, error, info, trace, warn};

impl<C: ManagerComponents> ManagerActor<C> {
    /// 处理预留 (Reserve) 请求。
    ///
    /// 此函数负责为客户端的预留请求分配偏移量和 ID，
    /// 并将其关联到一个合适的数据分组 (Group)。
    /// 如果需要，它会创建新的分组，并根据策略（如大小阈值）密封分组。
    /// 最后，将预留的详细信息回复给客户端。
    ///
    /// # 参数
    /// * `req`: 包含预留大小和回复通道的 `ReserveRequest`。
    pub(crate) fn handle_reserve(&mut self, req: ReserveRequest) {
        // 从请求中获取预留的大小。
        let size = usize::from(req.size);
        // 从预留分配器获取下一个可用的预留 ID 和全局偏移量。
        let (reservation_id, offset) = self.reservation_allocator.next_reservation_details(size);

        // 使用 GroupLifecycleManager 来查找或创建分组，并获取目标分组ID及是否是新创建的。
        // 这个过程会将预留信息（ID, offset, size）添加到目标分组的元数据中。
        let (target_group_id, created_new_group) = self
            .group_lifecycle_manager
            .find_or_create_group_for_reservation(reservation_id, offset, size);

        // 如果预留被添加到了一个已存在的分组 (即不是新创建的)，
        // 或者即使是新创建的分组（find_or_create_group_for_reservation内部可能已因大小达到阈值而密封），
        // 都需要检查是否达到了密封阈值。
        // find_or_create_group_for_reservation 内部的 internal_create_new_group 已经处理了新创建即密封的情况。
        // 对于加入现有分组的情况，我们需要在这里显式检查并密封。
        if !created_new_group {
            // 尝试根据阈值密封目标分组。
            match self
                .group_lifecycle_manager
                .seal_group_if_threshold_reached(target_group_id)
            {
                Ok(sealed_now) => {
                    // 如果分组是刚刚被此操作密封的。
                    if sealed_now {
                        debug!(
                            "(Manager) 活动分组 {} 在加入 Res {} 后被密封。",
                            target_group_id, reservation_id
                        );
                        // 注意：seal_group_if_threshold_reached 内部成功密封时，
                        // 会调用 clear_active_group_id_if_matches 来清除活动分组 ID。
                    }
                }
                Err(e) => {
                    // 理论上分组此时应该存在，因为 find_or_create_group_for_reservation 要么找到了它，要么创建了它。
                    // 如果在这里找不到分组，说明状态出现了严重的不一致。
                    error!(
                        "(Manager) 尝试密封分组 {} 时发生错误: {:?}. 回滚预留。",
                        target_group_id, e
                    );
                    // 回滚偏移量分配，释放之前分配的空间。
                    self.reservation_allocator.rollback_offset_allocation(size);
                    // 不需要手动清理分组内的预留，因为：
                    // 1. 预留尚未成功通知客户端，客户端不会基于此预留进行操作。
                    // 2. 分组状态可能因错误而不一致，清理可能引入更多问题。错误回复会通知客户端失败。
                    let _ = req.reply_tx.send(Err(ManagerError::Internal(format!(
                        "处理分组 {} 时内部错误: {:?}",
                        target_group_id, e
                    ))));
                    return; // 发生错误，提前返回。
                }
            }
        }

        // 如果成功创建了一个新的分组。
        if created_new_group {
            // 更新 Manager 的活动分组 ID 为这个新创建的分组。
            // 这使得后续的预留请求优先尝试放入这个分组。
            self.group_lifecycle_manager
                .update_active_group_after_creation(target_group_id);
        }

        // 向客户端发送成功回复，包含预留 ID、分配的偏移量和目标分组 ID。
        if req
            .reply_tx
            .send(Ok((reservation_id, offset, target_group_id)))
            .is_err()
        {
            // 如果发送回复失败（通常意味着客户端已经断开连接或不再监听）。
            error!("(Manager) 发送 Reserve 回复失败 for Res {}", reservation_id);
            // 需要回滚此操作造成的状态变更。
            // 1. 回滚偏移量分配器中的偏移量。
            self.reservation_allocator.rollback_offset_allocation(size);
            // 2. 从 GroupLifecycleManager 管理的分组状态中清理掉这个无效的预留记录。
            //    这很重要，因为即使客户端不再关心，这个预留记录依然存在于分组中，
            //    如果不清理，可能会导致分组永远无法完成（is_complete 无法为 true）。
            self.group_lifecycle_manager
                .cleanup_reservation_in_group(reservation_id, target_group_id);
        } else {
            // 成功发送回复。
            trace!(
                "(Manager) 已成功回复 Reserve 请求 for Res {} (ID={}, Offset={}, Size={}, Group={})",
                reservation_id, reservation_id, offset, size, target_group_id
            );
        }
    }

    /// 处理提交数据块 (SubmitBytes) 请求。
    ///
    /// 此函数负责接收客户端提交的数据块，并将其与之前的预留关联起来。
    /// 它会执行多项检查，包括分组是否存在、预留是否存在、偏移量是否匹配、
    /// 数据大小是否与预留一致、是否重复提交等。
    /// 验证通过后，数据会被存入对应的分组状态中。
    /// 提交成功后，会检查该分组是否已完成（所有预留都已提交或失败），
    /// 如果完成，则会触发异步的分组处理流程。
    ///
    /// # 参数
    /// * `req`: 包含预留 ID、分组 ID、绝对偏移量、提交数据以及回复通道的 `SubmitBytesRequest`。
    pub(crate) async fn handle_submit_bytes(&mut self, req: SubmitBytesRequest) {
        let group_id = req.group_id;
        let reservation_id_for_log = req.reservation_id; // 缓存预留ID，用于日志记录，避免所有权问题

        // --- 1. 同步操作部分：校验和准备数据 ---
        // 这部分逻辑如果出错，会直接发送错误回复并返回，不会进入后续的异步处理。
        // 使用内部块来管理这部分逻辑的成功与否。
        let sync_processing_successful = {
            // 获取目标分组的可变引用。
            let group = match self.group_lifecycle_manager.get_group_mut(group_id) {
                Some(g) => g, // 成功获取分组
                None => {
                    // 如果找不到指定 ID 的分组。
                    error!(
                        "(Manager) SubmitBytes 错误: 找不到 Group {} (for Res {})",
                        group_id, req.reservation_id
                    );
                    // 向客户端发送分组未找到的错误。
                    let _ = req
                        .reply_tx
                        .send(Err(ManagerError::GroupNotFound(group_id)));
                    return; // 关键错误，直接返回。
                }
            };

            // --- 2. 预检查 (Validation) ---
            // 从分组元数据中获取该预留的预期偏移量和大小。
            let (expected_offset, expected_size) =
                match group.get_reservation_metadata(req.reservation_id) {
                    Some(meta) => meta, // 成功找到预留元数据
                    None => {
                        // 如果在分组的元数据中找不到对应的预留 ID。
                        error!(
                            "(Manager) SubmitBytes 错误: Res {} 不在 Group {} 的元数据中",
                            req.reservation_id, group_id
                        );
                        // 向客户端发送预留未找到的错误。
                        let _ = req
                            .reply_tx
                            .send(Err(ManagerError::ReservationNotFound(req.reservation_id)));
                        return; // 关键错误，直接返回。
                    }
                };

            // 检查提交请求中的绝对偏移量是否与预留时分配的偏移量一致。
            if req.absolute_offset != expected_offset {
                error!(
                    "(Manager) SubmitBytes 错误: Res {} 提交偏移 {} 与预留偏移 {} 不匹配",
                    req.reservation_id, req.absolute_offset, expected_offset
                );
                // 向客户端发送偏移量无效的错误。
                let _ = req.reply_tx.send(Err(ManagerError::SubmitOffsetInvalid(
                    req.reservation_id,
                    req.absolute_offset,
                    expected_offset..(expected_offset + expected_size), // 告知期望的范围
                )));
                return; // 关键错误，直接返回。
            }

            // 检查该偏移量是否已经有提交的数据（防止重复提交）。
            if group.has_committed_data(req.absolute_offset) {
                warn!(
                    "(Manager) SubmitBytes 警告: Res {} (Offset {}) 似乎已被提交",
                    req.reservation_id, req.absolute_offset
                );
                // 向客户端发送重复提交的错误。
                let _ = req
                    .reply_tx
                    .send(Err(ManagerError::AlreadyCommitted(req.reservation_id)));
                return; // 逻辑错误，直接返回。
            }

            // --- 3. 处理提交的数据并进行大小校验 ---
            let mut actual_submitted_size = 0; // 用于累计实际提交的总大小
            // 使用 BTreeMap 存储数据块，键是块相对于预留起始位置的偏移量。
            // 这有助于按顺序处理分块提交，并能支持乱序或部分提交（如果未来需要）。
            let mut reservation_chunks = BTreeMap::new();

            match req.data {
                // 处理单块提交的情况。
                CommitType::Single(bytes) => {
                    actual_submitted_size = bytes.len();
                    // 校验提交的大小是否与预留时声明的大小完全一致。
                    if actual_submitted_size != expected_size {
                        error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 单块提交大小 {} 与预留大小 {} 不符", req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size);
                        // 向客户端发送大小不匹配的错误。
                        let _ = req.reply_tx.send(Err(ManagerError::CommitSizeMismatch {
                            reservation_id: req.reservation_id,
                            expected: expected_size,
                            actual: actual_submitted_size,
                        }));
                        return; // 关键错误，直接返回。
                    }
                    // 将数据块插入 BTreeMap，相对偏移为 0。
                    reservation_chunks.insert(0, bytes);
                    debug!(
                        "(Manager) Res {} (Offset {}) 收到单块提交，大小 {}",
                        req.reservation_id, req.absolute_offset, actual_submitted_size
                    );
                }
                // 处理分块提交的情况。
                CommitType::Chunked(chunks) => {
                    let mut current_chunk_relative_offset = 0; // 追踪当前块相对于预留的起始偏移
                    for chunk in chunks {
                        let len = chunk.len();
                        // 检查当前块加上之前的块的总大小是否超过了预留的总大小。
                        if current_chunk_relative_offset + len > expected_size {
                            error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交超出预留大小 {} (当前块偏移 {}, 长度 {})", req.reservation_id, req.absolute_offset, expected_size, current_chunk_relative_offset, len);
                            // 向客户端发送提交范围无效的错误。
                            let _ = req.reply_tx.send(Err(ManagerError::SubmitRangeInvalid(
                                req.reservation_id,
                                req.absolute_offset + current_chunk_relative_offset, // 计算出问题的全局偏移
                                len,
                                expected_offset..(expected_offset + expected_size), // 告知期望的范围
                            )));
                            return; // 关键错误，直接返回。
                        }
                        // 将数据块和其相对偏移存入 BTreeMap。
                        reservation_chunks.insert(current_chunk_relative_offset, chunk);
                        // 更新下一个块的相对偏移。
                        current_chunk_relative_offset += len;
                        // 累加实际提交的大小。
                        actual_submitted_size += len;
                    }
                    // 校验所有分块提交的总大小是否与预留时声明的大小完全一致。
                    if actual_submitted_size != expected_size {
                        error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交总大小 {} 与预留大小 {} 不符", req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size);
                        // 向客户端发送大小不匹配的错误。
                        let _ = req.reply_tx.send(Err(ManagerError::CommitSizeMismatch {
                            reservation_id: req.reservation_id,
                            expected: expected_size,
                            actual: actual_submitted_size,
                        }));
                        return; // 关键错误，直接返回。
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

            // --- 4. 更新分组状态：将提交的数据存入，并移除预留 ID ---
            // 将验证并处理好的数据块添加到分组的 committed_data 集合中。
            // 使用预留的绝对起始偏移量作为键，方便后续按偏移排序和处理。
            group.add_committed_data(
                req.absolute_offset, // Key: 预留的绝对起始偏移
                req.reservation_id,  // 关联的预留 ID
                expected_size,       // 存储期望的大小（用于可能的校验）
                reservation_chunks,  // 存储数据块 (BTreeMap<relative_offset, Bytes>)
            );
            trace!(
                "(Manager) Res {} (Offset {}) 数据已存入 Group {} 的 committed_data ({} 项)",
                req.reservation_id,
                req.absolute_offset,
                group_id,
                group.committed_data.len() // 记录当前已提交项的数量
            );

            // 从分组的待处理预留集合 (reservations) 中移除这个已经成功提交的预留 ID。
            // 这是判断分组是否完成的关键步骤。
            if !group.remove_pending_reservation(req.reservation_id) {
                // 如果移除失败（理论上不应该发生，因为前面已经校验过预留存在）。
                // 这可能表示状态不一致，但通常不阻塞流程，仅作警告。
                warn!(
                    "(Manager) SubmitBytes: Res {} 在提交成功后尝试从待处理集合移除时未找到",
                    req.reservation_id
                );
            }

            true // 返回 true，表示同步处理部分成功完成。
        };

        // 如果同步处理失败（即上面的块中途 return 了），则 sync_processing_successful 不会是 true。
        // 这里的检查是为了代码逻辑的完整性，理论上失败时已经提前返回。
        if !sync_processing_successful {
            // 此处不应到达，因为失败时已经 return
            return;
        }

        // --- 5. 发送成功回复给客户端 ---
        // 在所有同步校验和状态更新完成后，尽快回复客户端告知提交成功。
        // 后续的分组完成检查和处理是异步的，不应阻塞客户端的回复。
        if req.reply_tx.send(Ok(())).is_err() {
            // 如果发送成功回复失败（例如客户端已断开）。
            error!(
                "(Manager) 发送 SubmitBytes 成功回复失败 for Res {}",
                reservation_id_for_log // 使用之前缓存的 ID
            );
            // 此时状态已经改变（数据已提交到 group state），执行回滚非常复杂且可能不可靠。
            // 常见的策略是依赖后续的 Finalize 逻辑（如果客户端重连或有超时机制）
            // 或者依赖 check_and_process_completed_group 在分组完成后进行最终处理。
            // 即使无法通知客户端，只要分组状态正确，Manager 就能保证数据最终被处理或清理。
        } else {
            // 成功发送回复。
            trace!(
                "(Manager) SubmitBytes 成功回复已发送 for Res {}",
                reservation_id_for_log
            );
        }

        // --- 6. 异步检查分组是否完成 ---
        // 在发送回复之后，检查当前分组是否已经完成（即分组已密封且所有预留都已处理完毕）。
        // 使用 get_group_ref 获取只读引用进行检查，避免阻塞其他操作。
        let group_is_complete_for_processing = self
            .group_lifecycle_manager
            .get_group_ref(group_id) // 获取只读引用
            .map_or(false, |g| g.is_complete()); // 检查分组是否存在且完成

        // 如果分组已完成。
        if group_is_complete_for_processing {
            info!(
                "(Manager) Group {} 已密封且所有活动预留已提交/处理，准备检查和处理完成状态...",
                group_id
            );
            // 调用异步函数来处理已完成的分组。
            // 这个函数会负责数据的聚合、持久化等操作。
            match self.check_and_process_completed_group(group_id).await {
                Ok(processed) => {
                    // `processed` 为 true 表示分组成功处理并被移除。
                    if processed {
                        info!("(Manager) Group {} 成功处理完成。", group_id);
                    } else {
                        // `processed` 为 false 通常意味着在 check_and_process 函数内部检查时，
                        // 分组状态已不再满足处理条件（例如被并发移除或状态改变）。
                        warn!("(Manager) Group {} 处理完成检查返回未处理 (可能已被并发移除或状态已改变)", group_id);
                    }
                }
                Err(e) => {
                    // 如果处理过程中发生错误。
                    // check_and_process_completed_group 内部设计为在出错时尝试将 group_state 放回 GLM，
                    // 以便后续可以通过 Finalize 机制重试或清理。
                    error!("(Manager) Group {} 处理完成时失败: {:?}", group_id, e);
                    warn!(
                        "(Manager) Group {} 处理失败，状态已放回GLM，保留以待 Finalize 处理",
                        group_id
                    );
                }
            }
        } else {
            // 如果分组尚未完成。
            // 尝试获取当前分组状态以记录日志。
            if let Some(current_group_state) = self.group_lifecycle_manager.get_group_ref(group_id)
            {
                trace!(
                    "(Manager) Group {} 尚未完成 (剩余 reservations: {}, 密封: {})",
                    group_id,
                    current_group_state.reservations.len(), // 剩余待处理预留数量
                    current_group_state.is_sealed           // 分组是否已密封
                );
            } else {
                // 如果在记录日志时分组找不到了，可能是被并发的操作（如 Finalize 或其他 SubmitBytes 触发的完成处理）移除了。
                warn!(
                    "(Manager) SubmitBytes 后记录日志时找不到分组 {} (可能已被处理或移除)",
                    group_id
                );
            }
        }
    }

    /// 处理 Agent Drop 时发送的失败信息。
    ///
    /// 当持有预留的 Agent (客户端逻辑单元) 意外终止或关闭连接时，
    /// 它应该（尽力）通知 Manager 关于哪些预留失败了。
    /// 此函数处理这些失败通知，将相应的预留标记为失败状态，
    /// 并同样检查这是否导致了分组的完成。
    ///
    /// # 参数
    /// * `req`: 包含失败预留详细信息的 `FailedInfoRequest`。
    pub(crate) async fn handle_failed_info(&mut self, req: FailedInfoRequest) {
        let failed_info = req.info; // 获取失败信息结构体
        let res_id = failed_info.id;       // 失败的预留 ID
        let group_id = failed_info.group_id; // 相关的分组 ID

        // --- 同步操作部分 ---
        let mut reservation_was_pending = false; // 标记这个失败信息是否对应一个真实待处理的预留

        // 获取目标分组的可变引用。
        if let Some(group) = self.group_lifecycle_manager.get_group_mut(group_id) {
            // 检查这个预留 ID 是否确实存在于分组的待处理集合中。
            if group.has_pending_reservation(res_id) {
                reservation_was_pending = true; // 确认这是一个有效的、待处理的预留失败
                info!(
                    "(Manager) 收到 Res {} (Group {}) 的失败信息 (Agent Drop)，标记为失败",
                    res_id, group_id
                );
                // 从待处理集合中移除该预留 ID。
                group.remove_pending_reservation(res_id);
                // 将失败信息添加到分组的 failed_infos 集合中，用于记录和可能的后续处理。
                group.add_failed_info(failed_info.clone()); // 注意需要 clone
            } else {
                // 如果收到的失败信息对应的预留 ID 已经不在待处理集合中
                // （可能已经被提交、或者之前已经被标记为失败、或者是一个无效/过时的通知），
                // 则忽略这个失败信息。
                trace!(
                    "(Manager) 收到 Res {} (Group {}) 的失败信息，但其不在待处理集合中，忽略",
                    res_id,
                    group_id
                );
            }
        } else {
            // 如果找不到对应的分组 ID。
            // 这可能是因为分组已经被成功处理并移除，或者是一个完全无效的请求。
            warn!(
                "(Manager) FailedInfo 警告: 找不到 Group {} (for Res {})",
                group_id, res_id
            );
            return; // 分组不存在，无法处理，直接返回。
        }

        // --- 异步检查分组是否完成 ---
        // 仅当这个失败信息确实对应一个之前待处理的预留时，才需要检查分组是否因此完成。
        // 如果忽略了失败信息（因为它无效或重复），则无需检查。
        if reservation_was_pending {
            // 检查分组是否已完成（密封且无待处理预留）。
            let group_is_complete_for_processing = self
                .group_lifecycle_manager
                .get_group_ref(group_id) // 获取只读引用
                .map_or(false, |g| g.is_complete()); // 检查是否存在且完成

            if group_is_complete_for_processing {
                info!(
                    "(Manager) Group {} (因 Res {} 失败) 已密封且所有活动预留已处理，准备检查...",
                    group_id, res_id
                );
                // 调用异步函数处理（尝试处理）这个包含失败信息的分组。
                match self.check_and_process_completed_group(group_id).await {
                    Ok(processed) => {
                        // 重要：check_and_process_completed_group 设计上，如果分组包含任何失败信息 (failed_infos)，
                        // 它不应该认为处理"成功"并返回 Ok(true)。因为它通常需要特殊处理（如记录、告警、或保留以便手动干预）。
                        // 它应该返回 Err 或 Ok(false) 表示未处理/处理失败。
                        // 因此，如果在这里收到了 Ok(true)，意味着可能存在逻辑错误。
                        // 一种可能的情况是：最后一个pending的reservation失败了，同时group恰好没有任何failed_info（这与我们刚添加了failed_info矛盾）
                        // 或者 check_and_process 的逻辑未能正确处理包含失败信息的分组。
                        error!("(Manager) 内部逻辑错误：check_and_process 在有失败信息时返回了 Ok({}) for Group {}", processed, group_id);
                    }
                    Err(e) => {
                        // 如果 check_and_process 返回错误，这在有失败信息的情况下是预期的行为。
                        info!(
                            "(Manager) Group {} (含失败 Res {}) 处理完成检查返回错误 (预期): {:?}",
                            group_id, res_id, e
                        );
                        // 同样，check_and_process 内部会尝试将 group_state 放回 GLM。
                        warn!("(Manager) Group {} (含失败) 处理失败，状态已放回GLM，保留以待 Finalize 处理", group_id);
                    }
                }
            } else {
                // 如果分组在标记失败后仍未完成。
                // 尝试获取当前分组状态以记录日志。
                if let Some(current_group_state) =
                    self.group_lifecycle_manager.get_group_ref(group_id)
                {
                    trace!("(Manager) Group {} (收到 Res {} 失败信息后) 尚未完成 (剩余 reservations: {}, 失败: {}, 密封: {})",
                           group_id,
                           res_id,
                           current_group_state.reservations.len(), // 剩余待处理预留
                           current_group_state.failed_infos.len(), // 当前失败信息数量
                           current_group_state.is_sealed);       // 是否已密封
                }
                // 此处无需处理分组找不到的情况，因为外层 if reservation_was_pending 已保证分组存在。
            }
        }
    }
}
