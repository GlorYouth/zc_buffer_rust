use crate::types::{CommitType, FailedInfoRequest, ReserveRequest, SubmitBytesRequest};
use crate::{Manager, ManagerError};
use std::collections::BTreeMap;
use tracing::{debug, error, info, trace, warn};

impl Manager {
    /// 处理预留 (Reserve) 请求
    pub(crate) fn handle_reserve(&mut self, req: ReserveRequest) {
        let size = usize::from(req.size);
        let (reservation_id, offset) = self.reservation_allocator.next_reservation_details(size);

        // 使用 GroupLifecycleManager 来查找或创建分组，并获取目标分组ID及是否是新创建的
        let (target_group_id, created_new_group) = self
            .group_lifecycle_manager
            .find_or_create_group_for_reservation(reservation_id, offset, size);

        // 如果预留被添加到了一个已存在的分组 (即不是新创建的)，
        // 或者即使是新创建的分组（find_or_create_group_for_reservation内部可能已因大小达到阈值而密封），
        // 都需要检查是否达到了密封阈值。
        // find_or_create_group_for_reservation 内部的 internal_create_new_group 已经处理了新创建即密封的情况。
        // 对于加入现有分组的情况，我们需要在这里显式检查并密封。
        if !created_new_group {
            match self
                .group_lifecycle_manager
                .seal_group_if_threshold_reached(target_group_id)
            {
                Ok(sealed_now) => {
                    if sealed_now {
                        debug!(
                            "(Manager) 活动分组 {} 在加入 Res {} 后被密封。",
                            target_group_id, reservation_id
                        );
                        // seal_group_if_threshold_reached 内部会调用 clear_active_group_id_if_matches
                    }
                }
                Err(e) => {
                    // 理论上分组此时应该存在，如果不存在则是一个严重错误
                    error!(
                        "(Manager) 尝试密封分组 {} 时发生错误: {:?}. 回滚预留。",
                        target_group_id, e
                    );
                    self.reservation_allocator.rollback_offset_allocation(size);
                    // 不需要手动清理分组内的预留，因为预留尚未成功通知客户端，且分组状态可能不一致
                    let _ = req.reply_tx.send(Err(ManagerError::Internal(format!(
                        "处理分组 {} 时内部错误: {:?}",
                        target_group_id, e
                    ))));
                    return;
                }
            }
        }

        // 如果创建了新分组，需要更新活动分组ID
        if created_new_group {
            self.group_lifecycle_manager
                .update_active_group_after_creation(target_group_id);
        }

        // 回复客户端
        if req
            .reply_tx
            .send(Ok((reservation_id, offset, target_group_id)))
            .is_err()
        {
            error!("(Manager) 发送 Reserve 回复失败 for Res {}", reservation_id);
            // 回滚偏移量分配
            self.reservation_allocator.rollback_offset_allocation(size);
            // 使用 group_lifecycle_manager 清理分组内的这个无效预留
            self.group_lifecycle_manager
                .cleanup_reservation_in_group(reservation_id, target_group_id);
        } else {
            trace!(
                "(Manager) 已成功回复 Reserve 请求 for Res {} (ID={}, Offset={}, Size={}, Group={})",
                reservation_id, reservation_id, offset, size, target_group_id
            );
        }
    }

    /// 处理提交数据块 (SubmitBytes) 请求
    pub(crate) async fn handle_submit_bytes(&mut self, req: SubmitBytesRequest) {
        let group_id = req.group_id;
        let reservation_id_for_log = req.reservation_id; // 用于日志

        // 1. 同步操作部分：校验和准备数据
        // 这部分逻辑如果出错，会直接发送回复并返回，不会进入后续的异步处理。
        // 我们将这部分逻辑放在一个内部块中，以便清晰地处理其结果。
        let sync_processing_successful = {
            let group = match self.group_lifecycle_manager.get_group_mut(group_id) {
                Some(g) => g,
                None => {
                    error!(
                        "(Manager) SubmitBytes 错误: 找不到 Group {} (for Res {})",
                        group_id, req.reservation_id
                    );
                    let _ = req
                        .reply_tx
                        .send(Err(ManagerError::GroupNotFound(group_id)));
                    return; // 直接返回，不继续执行
                }
            };

            // 2. 预检查 (Validation)
            let (expected_offset, expected_size) =
                match group.get_reservation_metadata(req.reservation_id) {
                    Some(meta) => meta,
                    None => {
                        error!(
                            "(Manager) SubmitBytes 错误: Res {} 不在 Group {} 的元数据中",
                            req.reservation_id, group_id
                        );
                        let _ = req
                            .reply_tx
                            .send(Err(ManagerError::ReservationNotFound(req.reservation_id)));
                        return;
                    }
                };

            if req.absolute_offset != expected_offset {
                error!(
                    "(Manager) SubmitBytes 错误: Res {} 提交偏移 {} 与预留偏移 {} 不匹配",
                    req.reservation_id, req.absolute_offset, expected_offset
                );
                let _ = req.reply_tx.send(Err(ManagerError::SubmitOffsetInvalid(
                    req.reservation_id,
                    req.absolute_offset,
                    expected_offset..(expected_offset + expected_size),
                )));
                return;
            }

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

            // 3. 处理提交的数据并进行大小校验
            let mut actual_submitted_size = 0;
            let mut reservation_chunks = BTreeMap::new(); // Key: 块相对于预留的偏移, Value: Bytes

            match req.data {
                CommitType::Single(bytes) => {
                    actual_submitted_size = bytes.len();
                    if actual_submitted_size != expected_size {
                        error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 单块提交大小 {} 与预留大小 {} 不符", req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size);
                        let _ = req.reply_tx.send(Err(ManagerError::CommitSizeMismatch {
                            reservation_id: req.reservation_id,
                            expected: expected_size,
                            actual: actual_submitted_size,
                        }));
                        return;
                    }
                    reservation_chunks.insert(0, bytes); // 单块提交，相对偏移为 0
                    debug!(
                        "(Manager) Res {} (Offset {}) 收到单块提交，大小 {}",
                        req.reservation_id, req.absolute_offset, actual_submitted_size
                    );
                }
                CommitType::Chunked(chunks) => {
                    let mut current_chunk_relative_offset = 0;
                    for chunk in chunks {
                        let len = chunk.len();
                        if current_chunk_relative_offset + len > expected_size {
                            error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交超出预留大小 {} (当前块偏移 {}, 长度 {})", req.reservation_id, req.absolute_offset, expected_size, current_chunk_relative_offset, len);
                            let _ = req.reply_tx.send(Err(ManagerError::SubmitRangeInvalid(
                                req.reservation_id,
                                req.absolute_offset + current_chunk_relative_offset,
                                len,
                                expected_offset..(expected_offset + expected_size),
                            )));
                            return;
                        }
                        reservation_chunks.insert(current_chunk_relative_offset, chunk);
                        current_chunk_relative_offset += len;
                        actual_submitted_size += len;
                    }
                    if actual_submitted_size != expected_size {
                        error!("(Manager) SubmitBytes 错误: Res {} (Offset {}) 分块提交总大小 {} 与预留大小 {} 不符", req.reservation_id, req.absolute_offset, actual_submitted_size, expected_size);
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

            // 4. 更新分组状态：将提交的数据存入，并移除预留 ID
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

            if !group.remove_pending_reservation(req.reservation_id) {
                warn!(
                    "(Manager) SubmitBytes: Res {} 在提交成功后尝试从待处理集合移除时未找到",
                    req.reservation_id
                );
            }

            true // 表示同步处理成功
        };

        // 如果同步处理失败（已经在上面发送了错误回复并返回了），则 sync_processing_successful 不会是 true
        if !sync_processing_successful {
            // 此处不应到达，因为失败时已经 return
            return;
        }

        // 5. 发送成功回复给客户端
        if req.reply_tx.send(Ok(())).is_err() {
            // 发送回复失败，可能是客户端已放弃
            error!(
                "(Manager) 发送 SubmitBytes 成功回复失败 for Res {}",
                reservation_id_for_log
            );
            // 状态已经改变（数据已提交到group_state），回滚困难。
            // Manager 会依赖后续的 Finalize 逻辑来处理这种情况，
            // 或者如果分组因此完成但无法通知客户端，它仍然会被 check_and_process_completed_group 尝试处理。
        } else {
            trace!(
                "(Manager) SubmitBytes 成功回复已发送 for Res {}",
                reservation_id_for_log
            );
        }

        // --- 6. 异步检查分组是否完成 ---
        // （这部分逻辑与之前的版本基本一致，是在发送回复之后执行的）
        let group_is_complete_for_processing = self
            .group_lifecycle_manager
            .get_group_ref(group_id)
            .map_or(false, |g| g.is_complete());

        if group_is_complete_for_processing {
            info!(
                "(Manager) Group {} 已密封且所有活动预留已提交/处理，准备检查和处理完成状态...",
                group_id
            );
            match self.check_and_process_completed_group(group_id).await {
                Ok(processed) => {
                    if processed {
                        info!("(Manager) Group {} 成功处理完成。", group_id);
                    } else {
                        // 未处理通常意味着分组在检查时已不存在或状态不符合（尽管预检查应该捕获了 is_complete）
                        warn!("(Manager) Group {} 处理完成检查返回未处理 (可能已被并发移除或状态已改变)", group_id);
                    }
                }
                Err(e) => {
                    // 错误时，check_and_process_completed_group 内部会尝试将 group_state 放回 GLM
                    error!("(Manager) Group {} 处理完成时失败: {:?}", group_id, e);
                    warn!(
                        "(Manager) Group {} 处理失败，状态已放回GLM，保留以待 Finalize 处理",
                        group_id
                    );
                }
            }
        } else {
            // 分组尚未完成，记录日志
            if let Some(current_group_state) = self.group_lifecycle_manager.get_group_ref(group_id)
            {
                trace!(
                    "(Manager) Group {} 尚未完成 (剩余 reservations: {}, 密封: {})",
                    group_id,
                    current_group_state.reservations.len(),
                    current_group_state.is_sealed
                );
            } else {
                // 如果分组此时找不到了，可能是被并发的 finalize 或其他操作移除了
                warn!(
                    "(Manager) SubmitBytes 后记录日志时找不到分组 {} (可能已被处理或移除)",
                    group_id
                );
            }
        }
    }

    /// 处理 Agent Drop 时发送的失败信息
    pub(crate) async fn handle_failed_info(&mut self, req: FailedInfoRequest) {
        let failed_info = req.info;
        let res_id = failed_info.id;
        let group_id = failed_info.group_id;

        // 同步操作部分
        let mut reservation_was_pending = false;
        if let Some(group) = self.group_lifecycle_manager.get_group_mut(group_id) {
            if group.has_pending_reservation(res_id) {
                reservation_was_pending = true;
                info!(
                    "(Manager) 收到 Res {} (Group {}) 的失败信息 (Agent Drop)，标记为失败",
                    res_id, group_id
                );
                group.remove_pending_reservation(res_id); // 从待处理移除
                group.add_failed_info(failed_info.clone());
            } else {
                trace!(
                    "(Manager) 收到 Res {} (Group {}) 的失败信息，但其不在待处理集合中，忽略",
                    res_id,
                    group_id
                );
            }
        } else {
            warn!(
                "(Manager) FailedInfo 警告: 找不到 Group {} (for Res {})",
                group_id, res_id
            );
            return; // 分组不存在，无法处理
        }

        // 如果预留确实是待处理并被标记为失败了，才检查分组是否完成
        if reservation_was_pending {
            let group_is_complete_for_processing = self
                .group_lifecycle_manager
                .get_group_ref(group_id)
                .map_or(false, |g| g.is_complete());

            if group_is_complete_for_processing {
                info!(
                    "(Manager) Group {} (因 Res {} 失败) 已密封且所有活动预留已处理，准备检查...",
                    group_id, res_id
                );
                match self.check_and_process_completed_group(group_id).await {
                    Ok(processed) => {
                        // 如果有失败信息，check_and_process 理论上应返回Err或Ok(false)
                        // 如果返回 Ok(true)，可能意味着分组变空且无失败信息（这与我们的假设矛盾）
                        error!("(Manager) 内部逻辑错误：check_and_process 在有失败信息时返回了 Ok({}) for Group {}", processed, group_id);
                    }
                    Err(e) => {
                        info!(
                            "(Manager) Group {} (含失败 Res {}) 处理完成检查返回错误 (预期): {:?}",
                            group_id, res_id, e
                        );
                        warn!("(Manager) Group {} (含失败) 处理失败，状态已放回GLM，保留以待 Finalize 处理", group_id);
                    }
                }
            } else {
                if let Some(current_group_state) =
                    self.group_lifecycle_manager.get_group_ref(group_id)
                {
                    trace!("(Manager) Group {} (收到 Res {} 失败信息后) 尚未完成 (剩余 reservations: {}, 失败: {}, 密封: {})",
                           group_id, res_id, current_group_state.reservations.len(), current_group_state.failed_infos.len(), current_group_state.is_sealed);
                }
            }
        }
    }
}
