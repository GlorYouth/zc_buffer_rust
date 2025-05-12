// 声明模块 (Declare modules)
mod agent; // agent 模块
mod error;
mod handle;
// 将manager模块改为公开，以便测试可以访问
pub mod defaults;
pub mod manager;
pub mod types;

use std::num::NonZeroUsize;
use tokio::sync::mpsc;
// 公开导出需要被外部 (如 main 函数) 使用的类型
pub use agent::{ChunkAgent, SingleAgent, SubmitAgent};
// 导出 Agent 类型
pub use error::{BufferError, ManagerError};
// 导出错误类型
pub use handle::ZeroCopyHandle;
// 导出 Handle
pub use manager::ManagerActor;
// 导出 Manager Actor
pub use types::{
    FailedGroupData,
    FailedGroupDataTransmission,
    FailedReservationInfo,
    FinalizeResult,
    SuccessfulGroupData, // 导出主要的数据类型
};
// 导出默认的 ManagerComponents
pub use manager::ManagerComponents;

// 导出组件相关的trait，让测试能够实现它们
pub use manager::components::{
    FinalizationHandler, GroupDataProcessor, GroupLifecycleManager, ReservationAllocator,
};

// 导出处理相关的错误和结果类型
pub use manager::components::group_data_processor::{
    ProcessedGroupOutcome, ProcessingError, ProcessorTaskError,
};

use crate::manager::components::ComponentsBuilder;

/// 中文注释：使用ZST组件标记来启动ManagerActor的辅助函数。
///
/// # Arguments
/// * `_components_token`: 一个ZST实例，其类型实现了 `ManagerComponents` Trait。用于类型推断。
/// * `channel_buffer_size`: 用于内部通道的缓冲区大小。
/// * `config`: 对应 `MC::Config` 类型的配置实例。
///
/// # Returns
/// 与 `ManagerActor::spawn` 相同的返回类型。
pub fn _spawn_manager_with_components_token<MC: ManagerComponents>(
    _components_token: MC, // 中文注释：ZST实例，仅用于类型推断 `MC`
    channel_buffer_size: NonZeroUsize,
    config: &<MC::CB as ComponentsBuilder<MC>>::BuilderConfig,
) -> (
    ZeroCopyHandle,
    mpsc::Receiver<SuccessfulGroupData>,
    mpsc::Receiver<FailedGroupDataTransmission>,
) {
    // 中文注释：调用原始的 ManagerActor::spawn，此时 MC 类型已经通过 _components_token 推断出来
    ManagerActor::<MC>::spawn(channel_buffer_size, config)
}
