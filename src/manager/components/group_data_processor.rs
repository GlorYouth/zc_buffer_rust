// src/manager/components/group_data_processor.rs
//! 定义了用于处理、验证和合并已完成分组数据的 Trait (`GroupDataProcessor`) 及其默认实现。

use crate::types::{AbsoluteOffset, GroupId, ReservationId, SuccessfulGroupData}; // 引入类型
use bytes::Bytes; // 引入 Bytes
use std::collections::{BTreeMap, HashMap}; // 引入集合类型
use thiserror::Error; // 用于定义自定义错误类型
use tracing::{debug, error, info, trace, warn}; // 引入日志宏

/// `GroupDataProcessor` 处理已提交分组数据时可能发生的具体错误枚举。
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ProcessingError {
    /// 错误：分组内的数据块在绝对偏移量上不连续。
    /// 包含期望的下一个偏移量和实际遇到的偏移量。
    #[error("分组 {0} 数据不连续: 期望偏移 {1}, 实际遇到的预留偏移 {2}")]
    NotContiguous(GroupId, AbsoluteOffset, AbsoluteOffset),

    /// 错误：根据提交数据计算出的总大小与根据元数据计算出的预期总大小不匹配。
    #[error("分组 {0} 大小不匹配: 元数据预期总大小 {1}, 提交计算得到 {2}")]
    SizeMismatch(GroupId, usize, usize),

    /// 错误：根据提交数据计算出的分组起始偏移量与根据元数据计算出的预期起始偏移量不匹配。
    #[error("分组 {0} 起始偏移不匹配: 元数据预期最小偏移 {1:?}, 提交计算得到 {2:?}")]
    OffsetMismatch(GroupId, Option<AbsoluteOffset>, Option<AbsoluteOffset>),

    /// 错误：数据合并后的最终字节长度与验证阶段确定的预期总大小不符。
    /// 这通常指示合并逻辑本身存在问题。
    #[error("分组 {0} 数据合并后大小校验失败: 预期 {1}, 实际合并得到 {2}")]
    MergeFailure(GroupId, usize, usize),

    /// 逻辑错误：尝试处理一个空的 `committed_data` BTreeMap。
    /// `process_group_data` 的调用者应确保在调用前检查 `committed_data` 是否为空。
    #[error("尝试验证或处理一个空的 committed_data map (逻辑错误) for group {0}")]
    AttemptToProcessEmptyCommittedData(GroupId),
}

/// `GroupDataProcessor` 处理一个分组后的可能结果。
pub enum ProcessedGroupOutcome {
    /// 分组数据成功验证并通过处理（通常是合并）。
    /// 包含处理后的结果 (`SuccessfulGroupData`)，即 (起始偏移量, 合并后的数据 Box<[u8]>)。
    Success(SuccessfulGroupData),
    /// 分组有效，但处理后发现没有实际的数据需要发送（例如，分组只包含元数据但没有提交的数据）。
    /// 这种情况下不需要发送任何数据。
    Empty,
}

/// 当 `GroupDataProcessor` 的 `process_group_data` 方法失败时返回的错误类型。
///
/// 这个结构体包装了具体的 `ProcessingError`，并可能包含处理失败时
/// 尚未被消耗的原始 `committed_data`，以便调用者可以将其用于失败报告。
#[derive(Debug)]
pub struct ProcessorTaskError {
    /// 具体的处理错误类型。
    pub error: ProcessingError,
    /// 如果错误发生在数据合并之前（例如验证失败），这里会包含调用时传入的原始 `committed_data`。
    /// 调用者可以使用这些数据来构建 `FailedGroupData`。
    /// 如果错误发生在合并期间或之后，此 BTreeMap 将为空，因为数据已被合并逻辑消耗。
    pub committed_data_on_failure:
        BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
}

/// 定义处理已完成分组数据的核心逻辑的 Trait。
///
/// 实现此 Trait 的组件负责接收一个分组的所有已成功提交的数据块 (`committed_data`)
/// 和相关的元数据 (`reservation_metadata`)，执行验证（如连续性、大小校验），
/// 并在验证通过后将数据块合并成一个连续的 `Bytes` 或 `Box<[u8]>`。
pub trait GroupDataProcessor: Send + Sync + 'static { // 要求线程安全和静态生命周期
    /// 处理一个分组的已提交数据。
    ///
    /// 接收分组 ID、该分组所有已提交的数据块（按预留起始偏移排序）以及所有预留的元数据。
    ///
    /// **注意：** `committed_data` 参数按值传递，表示此方法会获得其所有权。
    /// 如果处理成功，数据会被合并并包含在 `Ok(ProcessedGroupOutcome::Success)` 中返回。
    /// 如果处理失败：
    ///   - 若失败发生在验证阶段，原始的 `committed_data` 会被包含在 `Err(ProcessorTaskError)` 中返回。
    ///   - 若失败发生在合并阶段，`committed_data` 已被消耗，`Err(ProcessorTaskError)` 中将包含一个空的 BTreeMap。
    ///
    /// # Arguments
    /// * `group_id` - 正在处理的分组 ID。
    /// * `committed_data` - 包含该分组所有已成功提交数据块的 BTreeMap，键是预留的起始绝对偏移量。
    /// * `reservation_metadata` - 包含该分组所有预留（无论状态）元数据的 HashMap。
    ///
    /// # Returns
    /// * `Result<ProcessedGroupOutcome, ProcessorTaskError>`:
    ///   - `Ok(ProcessedGroupOutcome::Success(data))`：处理成功，`data` 是 `(AbsoluteOffset, Box<[u8]>)`。
    ///   - `Ok(ProcessedGroupOutcome::Empty)`：分组有效但为空，无需发送数据。
    ///   - `Err(ProcessorTaskError)`：处理失败，包含错误详情和可能未被消耗的 `committed_data`。
    fn process_group_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>, // 接收所有权
        reservation_metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<ProcessedGroupOutcome, ProcessorTaskError>;
}

/// `GroupDataProcessor` Trait 的默认实现。
/// 提供基本的验证和合并逻辑。
pub struct DefaultGroupDataProcessor;

impl DefaultGroupDataProcessor {
    /// 创建 `DefaultGroupDataProcessor` 的新实例。
    pub fn new() -> Self {
        DefaultGroupDataProcessor {}
    }

    /// 验证分组数据的连续性和总大小是否与元数据匹配。
    ///
    /// **注意：** 此方法接收 `committed_data` 的 **引用**，不会消耗数据。
    ///
    /// # Arguments
    /// * `group_id` - 分组 ID。
    /// * `committed_data` - 对已提交数据 BTreeMap 的引用。
    /// * `metadata` - 对预留元数据 HashMap 的引用。
    ///
    /// # Returns
    /// * `Result<(AbsoluteOffset, usize), ProcessingError>`:
    ///   - `Ok((start_offset, total_size))`：验证成功，返回计算出的分组起始偏移量和总大小。
    ///   - `Err(ProcessingError)`：验证失败，返回具体的错误类型。
    fn validate_group_continuity_and_size(
        &self,
        group_id: GroupId,
        committed_data: &BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>, // 接收引用
        metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<(AbsoluteOffset, usize), ProcessingError> {
        // 防御性检查：不应验证空的 committed_data
        if committed_data.is_empty() {
            error!(
                "(DefaultGroupDataProcessor) [validate] 逻辑错误：分组 {} 尝试验证空的 committed_data map。",
                group_id
            );
            // 返回特定错误，表明调用逻辑有问题
            return Err(ProcessingError::AttemptToProcessEmptyCommittedData(group_id));
        }

        // 初始化计算变量
        let mut calculated_total_size = 0; // 根据提交数据计算的总大小
        let mut current_expected_offset: Option<AbsoluteOffset> = None; // 期望的下一个预留的起始偏移
        let mut actual_group_start_offset: Option<AbsoluteOffset> = None; // 实际遇到的第一个预留的起始偏移

        // 遍历已提交数据（BTreeMap 保证按预留起始偏移排序）
        for (&res_start_offset, &(res_id, res_size, _)) in committed_data {
            // 记录第一个遇到的偏移作为实际起始偏移
            if actual_group_start_offset.is_none() {
                actual_group_start_offset = Some(res_start_offset);
                trace!("(DefaultGroupDataProcessor) [validate] Group {} 发现起始预留 Res {} @ Offset {} (Size {})", group_id, res_id, res_start_offset, res_size);
            }

            // 检查连续性：当前预留的起始偏移是否等于上一个预留结束的偏移
            if let Some(expected) = current_expected_offset {
                if res_start_offset != expected {
                    // 数据不连续
                    error!(
                        "(DefaultGroupDataProcessor) [validate] 分组 {} 数据不连续！Res {} (Offset {}) 处中断。期望 {}, 实际 {}",
                        group_id, res_id, res_start_offset, expected, res_start_offset
                    );
                    return Err(ProcessingError::NotContiguous(
                        group_id,
                        expected,
                        res_start_offset,
                    ));
                }
            }
            // 累加计算总大小
            calculated_total_size += res_size;
            // 更新期望的下一个预留的起始偏移
            current_expected_offset = Some(res_start_offset + res_size);
        }

        // --- 与元数据进行交叉验证 ---
        // 从元数据计算预期的总大小
        let expected_total_size_from_meta: usize =
            metadata.values().map(|&(_offset, size)| size).sum();
        // 从元数据计算预期的分组起始偏移（所有预留偏移中的最小值）
        let expected_group_start_offset_from_meta =
            metadata.values().map(|&(offset, _size)| offset).min();

        // 比较计算的总大小与元数据的预期总大小
        if calculated_total_size != expected_total_size_from_meta {
            error!(
                "(DefaultGroupDataProcessor) [validate] 分组 {} 大小不匹配！元数据预期 {}, 提交计算得到 {}",
                group_id, expected_total_size_from_meta, calculated_total_size
            );
            trace!(
                "(DefaultGroupDataProcessor) [validate] 相关元数据 for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ProcessingError::SizeMismatch(
                group_id,
                expected_total_size_from_meta,
                calculated_total_size,
            ));
        }

        // 比较计算的起始偏移与元数据的预期起始偏移
        if actual_group_start_offset != expected_group_start_offset_from_meta {
            error!(
                "(DefaultGroupDataProcessor) [validate] 分组 {} 起始偏移不匹配！元数据预期 {:?}, 提交计算得到 {:?}",
                group_id, expected_group_start_offset_from_meta, actual_group_start_offset
            );
            trace!(
                "(DefaultGroupDataProcessor) [validate] 相关元数据 for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ProcessingError::OffsetMismatch(
                group_id,
                expected_group_start_offset_from_meta,
                actual_group_start_offset,
            ));
        }

        // 所有验证通过
        info!(
            "(DefaultGroupDataProcessor) [validate] Group {} 验证通过 (起始 Offset {:?}, 总 Size {})",
            group_id, actual_group_start_offset, calculated_total_size
        );
        // 返回计算出的起始偏移（unwrap 安全，因为 committed_data 非空）和总大小
        Ok((actual_group_start_offset.unwrap(), calculated_total_size))
    }

    /// 合并分组的所有已提交数据块到一个 `Vec<u8>` 中。
    ///
    /// **注意：** 此方法接收 `committed_data` 的 **所有权** 并会消耗它。
    ///
    /// # Arguments
    /// * `group_id` - 分组 ID。
    /// * `committed_data` - 包含已提交数据的 BTreeMap (将被消耗)。
    /// * `expected_total_size` - 从验证阶段得到的预期总大小，用于容量预分配和最终校验。
    ///
    /// # Returns
    /// * `Result<Vec<u8>, ProcessingError>`:
    ///   - `Ok(merged_data)`：合并成功，返回包含所有数据的 `Vec<u8>`。
    ///   - `Err(ProcessingError::MergeFailure)`：合并失败（最终大小不符）。
    fn merge_committed_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<
            AbsoluteOffset,
            (ReservationId, usize, BTreeMap<usize, Bytes>), // 注意内层 Key 是 usize
        >,
        expected_total_size: usize,
    ) -> Result<Vec<u8>, ProcessingError> {
        trace!(
            "(DefaultGroupDataProcessor) [merge] 开始合并 Group {} 数据，预期总大小 {}",
            group_id,
            expected_total_size
        );
        // 预分配 Vec 容量以提高效率
        let mut merged_data_vec = Vec::with_capacity(expected_total_size);

        // 遍历 committed_data (按预留偏移排序)
        // `committed_data` 在迭代过程中被消耗
        for (_res_offset, (res_id, _res_size, reservation_chunks)) in committed_data {
            // 遍历当前预留的所有数据块 (按块相对偏移排序)
            for (chunk_relative_offset, chunk_bytes) in reservation_chunks {
                trace!("(DefaultGroupDataProcessor) [merge] Group {} 追加来自 Res {} 的块 (RelOffset: {}, Size: {})", group_id, res_id, chunk_relative_offset, chunk_bytes.len());
                // 将块数据追加到结果 Vec 中
                merged_data_vec.extend_from_slice(&chunk_bytes);
            }
        }

        // 最终校验：合并后的 Vec 长度是否等于预期总大小
        if merged_data_vec.len() != expected_total_size {
            error!(
                "(DefaultGroupDataProcessor) [merge] 分组 {} 合并后大小 {} 与预期大小 {} 不符！数据可能丢失或重复。",
                group_id,
                merged_data_vec.len(),
                expected_total_size
            );
            // 返回合并失败错误
            Err(ProcessingError::MergeFailure(
                group_id,
                expected_total_size,
                merged_data_vec.len(),
            ))
        } else {
            // 合并成功
            trace!(
                "(DefaultGroupDataProcessor) [merge] Group {} 数据合并成功，得到 {} 字节",
                group_id,
                merged_data_vec.len()
            );
            Ok(merged_data_vec)
        }
    }
}

// 为 DefaultGroupDataProcessor 实现 GroupDataProcessor Trait
impl GroupDataProcessor for DefaultGroupDataProcessor {
    /// `DefaultGroupDataProcessor` 对 `process_group_data` 的实现。
    fn process_group_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
        reservation_metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<ProcessedGroupOutcome, ProcessorTaskError> {
        info!("(DefaultGroupDataProcessor) [process_group_data] 开始处理 Group {}", group_id);
        // 首先检查传入的 committed_data 是否为空
        if committed_data.is_empty() {
            debug!("(DefaultGroupDataProcessor) [process_group_data] Group {} 的 committed_data 为空，判定为 Empty 分组。", group_id);
            // 如果为空，直接返回 Empty 结果，无需进一步处理
            return Ok(ProcessedGroupOutcome::Empty);
        }

        // --- 步骤 1: 验证数据连续性和大小 (使用引用，不消耗 committed_data) ---
        match self.validate_group_continuity_and_size(
            group_id,
            &committed_data, // 传递引用
            reservation_metadata,
        ) {
            Ok((start_offset, total_size)) => {
                // 验证成功，获取了起始偏移和总大小
                info!("(DefaultGroupDataProcessor) [process_group_data] Group {} 数据验证成功 (Offset: {}, Size: {})，准备合并...", group_id, start_offset, total_size);

                // --- 步骤 2: 合并数据 (消耗 committed_data) ---
                match self.merge_committed_data(group_id, committed_data, total_size) {
                    Ok(data_to_send_vec) => {
                        // 合并成功，将 Vec<u8> 转换为 Box<[u8]> (更适合跨线程传递)
                        let successful_data: SuccessfulGroupData = (start_offset, data_to_send_vec.into());
                        info!("(DefaultGroupDataProcessor) [process_group_data] Group {} 数据合并成功，准备发送。", group_id);
                        // 返回成功结果
                        Ok(ProcessedGroupOutcome::Success(successful_data))
                    }
                    Err(merge_err) => {
                        // 合并失败
                        error!("(DefaultGroupDataProcessor) [process_group_data] Group {} 数据合并失败: {:?}", group_id, merge_err);
                        // 因为 merge_committed_data 消耗了 committed_data，
                        // 所以在返回错误时，committed_data_on_failure 字段为空 BTreeMap。
                        Err(ProcessorTaskError {
                            error: merge_err,
                            committed_data_on_failure: BTreeMap::new(), // 数据已被消耗
                        })
                    }
                }
            }
            Err(validation_err) => {
                // 验证失败
                warn!(
                    "(DefaultGroupDataProcessor) [process_group_data] Group {} 数据验证失败: {:?}",
                    group_id, validation_err
                );
                // 因为 validate_group_continuity_and_size 使用的是引用，
                // 所以原始的 committed_data 未被消耗，可以包含在错误中返回给调用者。
                Err(ProcessorTaskError {
                    error: validation_err,
                    committed_data_on_failure: committed_data, // 返回原始数据
                })
            }
        }
    }
}
