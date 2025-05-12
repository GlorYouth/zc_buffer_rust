// src/manager/group_data_processor.rs

use crate::types::{AbsoluteOffset, GroupId, ReservationId, SuccessfulGroupData};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;
// 需要引入 thiserror
use tracing::{debug, error, info, trace, warn};
// 确保 tracing 宏可用

/// GroupDataProcessor 处理已提交分组数据时可能发生的具体错误。
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ProcessingError {
    #[error("分组 {0} 数据不连续: 期望偏移 {1}, 实际偏移 {2}")]
    NotContiguous(GroupId, AbsoluteOffset, AbsoluteOffset),

    #[error("分组 {0} 大小不匹配: 元数据期望 {1}, 提交计算得到 {2}")]
    SizeMismatch(GroupId, usize, usize),

    #[error("分组 {0} 起始偏移不匹配: 元数据期望 {1:?}, 提交计算得到 {2:?}")]
    OffsetMismatch(GroupId, Option<AbsoluteOffset>, Option<AbsoluteOffset>),

    #[error("分组 {0} 数据合并后大小校验失败: 期望 {1}, 实际 {2}")]
    MergeFailure(GroupId, usize, usize),

    #[error("尝试验证或处理一个空的 committed_data map (逻辑错误) for group {0}")]
    AttemptToProcessEmptyCommittedData(GroupId),
}

/// GroupDataProcessor 处理分组后的结果。
#[derive(Debug)]
pub enum ProcessedGroupOutcome {
    /// 分组数据成功验证和合并。
    Success(SuccessfulGroupData),
    /// 分组有效但没有实际数据需要发送。
    Empty,
}

/// 当 GroupDataProcessor 的 process_group_data 方法失败时返回的错误类型。
/// 包含具体的处理错误以及在验证失败时需要返回的原始 committed_data。
#[derive(Debug)]
pub struct ProcessorTaskError {
    pub error: ProcessingError,
    /// 如果错误发生在合并之前（例如验证失败），这里会包含原始的 committed_data。
    /// 如果错误发生在合并期间或之后，此 BTreeMap 将为空，因为数据已被消耗。
    pub committed_data_on_failure:
        BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
}

/// `GroupDataProcessor` Trait
/// 职责: 处理已提交数据的验证、合并和分发决策。
pub trait GroupDataProcessor: Send + Sync + 'static {
    fn process_group_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>, // 接收所有权
        reservation_metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<ProcessedGroupOutcome, ProcessorTaskError>; // 返回新的错误类型
}

/// `GroupDataProcessor` Trait 的默认实现。
pub struct DefaultGroupDataProcessor;

impl DefaultGroupDataProcessor {
    pub fn new() -> Self {
        DefaultGroupDataProcessor {}
    }

    // validate_group_continuity_and_size 接收的是引用，不消耗 committed_data
    fn validate_group_continuity_and_size(
        &self,
        group_id: GroupId,
        committed_data: &BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
        metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<(AbsoluteOffset, usize), ProcessingError> {
        // ... 内部逻辑与之前相同，返回 ProcessingError ...
        if committed_data.is_empty() {
            error!(
                "(DefaultGroupDataProcessor) [validate] 分组 {} 尝试验证空的 committed_data map。",
                group_id
            );
            return Err(ProcessingError::AttemptToProcessEmptyCommittedData(
                group_id,
            ));
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
                        "(DefaultGroupDataProcessor) [validate] 分组 {} 数据不连续！Res {} (Offset {}) 处中断。期望 {}, 实际 {}",
                        group_id, _id, res_start_offset, expected, res_start_offset
                    );
                    return Err(ProcessingError::NotContiguous(
                        group_id,
                        expected,
                        res_start_offset,
                    ));
                }
            }
            calculated_total_size += *res_size;
            current_expected_offset = Some(res_start_offset + *res_size);
        }

        let expected_total_size_from_meta: usize =
            metadata.values().map(|&(_offset, size)| size).sum();
        let expected_group_start_offset_from_meta =
            metadata.values().map(|&(offset, _size)| offset).min();

        if calculated_total_size != expected_total_size_from_meta {
            error!(
                "(DefaultGroupDataProcessor) [validate] 分组 {} 大小不匹配！元数据期望 {}, 提交计算得到 {}",
                group_id, expected_total_size_from_meta, calculated_total_size
            );
            trace!(
                "(DefaultGroupDataProcessor) [validate] Metadata for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ProcessingError::SizeMismatch(
                group_id,
                expected_total_size_from_meta,
                calculated_total_size,
            ));
        }

        if actual_group_start_offset != expected_group_start_offset_from_meta {
            error!(
                "(DefaultGroupDataProcessor) [validate] 分组 {} 起始偏移不匹配！元数据期望 {:?}, 提交计算得到 {:?}",
                group_id, expected_group_start_offset_from_meta, actual_group_start_offset
            );
            trace!(
                "(DefaultGroupDataProcessor) [validate] Metadata for group {}: {:?}",
                group_id,
                metadata
            );
            return Err(ProcessingError::OffsetMismatch(
                group_id,
                expected_group_start_offset_from_meta,
                actual_group_start_offset,
            ));
        }

        info!(
            "(DefaultGroupDataProcessor) [validate] Group {} 验证通过 (Offset {:?}, Size {})",
            group_id, actual_group_start_offset, calculated_total_size
        );
        Ok((actual_group_start_offset.unwrap(), calculated_total_size))
    }

    // merge_committed_data 消耗 committed_data
    fn merge_committed_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<
            AbsoluteOffset,
            (ReservationId, usize, BTreeMap<AbsoluteOffset, Bytes>),
        >,
        expected_total_size: usize,
    ) -> Result<Vec<u8>, ProcessingError> {
        // ... 内部逻辑与之前相同，返回 ProcessingError ...
        trace!(
            "(DefaultGroupDataProcessor) [merge] 开始合并 Group {} 数据，预期总大小 {}",
            group_id,
            expected_total_size
        );
        let mut merged_data_vec = Vec::with_capacity(expected_total_size);

        for (_res_offset, (_res_id, _res_size, reservation_chunks)) in committed_data {
            // committed_data 在此被消耗
            for (_chunk_relative_offset, chunk_bytes) in reservation_chunks {
                merged_data_vec.extend_from_slice(&chunk_bytes);
            }
        }

        if merged_data_vec.len() != expected_total_size {
            error!(
                "(DefaultGroupDataProcessor) [merge] 分组 {} 合并后大小 {} 与预期大小 {} 不符！",
                group_id,
                merged_data_vec.len(),
                expected_total_size
            );
            Err(ProcessingError::MergeFailure(
                group_id,
                expected_total_size,
                merged_data_vec.len(),
            ))
        } else {
            trace!(
                "(DefaultGroupDataProcessor) [merge] Group {} 数据合并成功，得到 {} 字节",
                group_id,
                merged_data_vec.len()
            );
            Ok(merged_data_vec)
        }
    }
}

impl GroupDataProcessor for DefaultGroupDataProcessor {
    fn process_group_data(
        &self,
        group_id: GroupId,
        committed_data: BTreeMap<AbsoluteOffset, (ReservationId, usize, BTreeMap<usize, Bytes>)>,
        reservation_metadata: &HashMap<ReservationId, (AbsoluteOffset, usize)>,
    ) -> Result<ProcessedGroupOutcome, ProcessorTaskError> {
        if committed_data.is_empty() {
            debug!("(DefaultGroupDataProcessor) [process_group_data] 分组 {} 的 committed_data 为空，视为空分组。", group_id);
            return Ok(ProcessedGroupOutcome::Empty);
        }

        // 1. 验证 (不消耗 committed_data)
        match self.validate_group_continuity_and_size(
            group_id,
            &committed_data,
            reservation_metadata,
        ) {
            Ok((start_offset, total_size)) => {
                info!("(DefaultGroupDataProcessor) [process_group_data] 分组 {} 数据连续且大小正确，准备合并...", group_id);
                // 2. 合并数据 (committed_data 在这里被消耗)
                match self.merge_committed_data(group_id, committed_data, total_size) {
                    Ok(data_to_send_vec) => {
                        let successful_data: SuccessfulGroupData =
                            (start_offset, data_to_send_vec.into_boxed_slice());
                        Ok(ProcessedGroupOutcome::Success(successful_data))
                    }
                    Err(merge_err) => {
                        error!("(DefaultGroupDataProcessor) [process_group_data] 分组 {} 数据合并失败: {:?}", group_id, merge_err);
                        // 合并失败时，committed_data 已被消耗，所以返回空的 BTreeMap
                        Err(ProcessorTaskError {
                            error: merge_err,
                            committed_data_on_failure: BTreeMap::new(), // 数据已被 merge_committed_data 消耗
                        })
                    }
                }
            }
            Err(validation_err) => {
                warn!(
                    "(DefaultGroupDataProcessor) [process_group_data] 分组 {} 数据验证失败: {:?}",
                    group_id, validation_err
                );
                // 验证失败时，committed_data 未被消耗，可以返回
                Err(ProcessorTaskError {
                    error: validation_err,
                    committed_data_on_failure: committed_data, // 返回原始数据
                })
            }
        }
    }
}
