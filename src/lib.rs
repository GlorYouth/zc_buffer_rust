/// 声明模块 (Declare modules)
mod agent; // 负责处理数据块和提交的 Agent 模块
mod error; // 定义错误类型的模块
mod handle; // 定义与 ManagerActor 交互的 Handle 模块

// 公开 manager 模块，以便外部（如测试）可以访问其内容
pub mod defaults; // 定义默认配置和行为的模块
pub mod manager; // Manager Actor 及其组件的模块
pub mod types; // 定义核心数据结构和类型的模块

use std::num::NonZeroUsize;
use tokio::sync::mpsc;

// 公开导出需要被外部 (如 main 函数) 使用的类型
pub use agent::{ChunkAgent, SingleAgent, SubmitAgent}; // 导出 Agent 相关类型
pub use error::{BufferError, ManagerError}; // 导出错误类型
pub use handle::ZeroCopyHandle; // 导出与 ManagerActor 交互的 Handle
pub use manager::ManagerActor; // 导出 Manager Actor

// 导出核心数据类型
pub use types::{
    FailedGroupData,            // 处理失败的数据组信息
    FailedGroupDataTransmission, // 传输失败的数据组信息
    FailedReservationInfo,      // 预留失败的信息
    FinalizeResult,             // 最终化操作的结果
    SuccessfulGroupData,        // 成功处理的数据组信息
};

// 导出默认的 ManagerComponents 实现
pub use manager::ManagerComponents;

// 导出组件相关的 trait，允许外部实现自定义组件，也方便测试
pub use manager::components::{
    FinalizationHandler, // 最终化处理器 Trait
    GroupDataProcessor,  // 数据组处理器 Trait
    GroupLifecycleManager, // 数据组生命周期管理器 Trait
    ReservationAllocator, // 预留空间分配器 Trait
};

// 导出数据处理相关的错误和结果类型
pub use manager::components::group_data_processor::{
    ProcessedGroupOutcome, // 处理后的数据组结果
    ProcessingError,       // 处理过程中可能发生的错误
    ProcessorTaskError,    // 处理器任务执行错误
};

use crate::manager::components::ComponentsBuilder;

/// 使用 ZST (Zero-Sized Type) 组件标记来启动 `ManagerActor` 的辅助函数。
///
/// 这个函数提供了一种便捷的方式来启动 `ManagerActor`，
/// 通过传递一个零大小类型 (ZST) 的实例来指定要使用的组件集合 (`ManagerComponents`)。
/// ZST 本身不携带任何数据，仅用于在编译时传递类型信息。
///
/// # Arguments
///
/// * `_components_token`: 一个 ZST 实例，其类型实现了 `ManagerComponents` Trait。
///   这个参数主要用于编译时类型推断，确定 `ManagerActor` 应使用哪套组件实现。
///   参数名以下划线开头，表示该参数在运行时未使用。
/// * `channel_buffer_size`: 用于 `ManagerActor` 内部 MPSC 通道的缓冲区大小。
///   这是一个 `NonZeroUsize` 类型，确保缓冲区大小至少为 1。
/// * `config`: `ManagerComponents` 关联的 `ComponentsBuilder` 所需的配置信息。
///   类型为 `<MC::CB as ComponentsBuilder<MC>>::BuilderConfig`。
///
/// # Returns
///
/// 返回一个元组，包含：
/// * `ZeroCopyHandle`: 用于与新启动的 `ManagerActor` 交互的句柄。
/// * `mpsc::Receiver<SuccessfulGroupData>`: 用于接收成功处理的数据组信息的通道接收端。
/// * `mpsc::Receiver<FailedGroupDataTransmission>`: 用于接收传输失败的数据组信息的通道接收端。
///
/// 这与直接调用 `ManagerActor::<MC>::spawn` 的返回值相同。
pub fn _spawn_manager_with_components_token<MC: ManagerComponents>(
    _components_token: MC, // ZST 实例，仅用于类型推断 `MC`
    channel_buffer_size: NonZeroUsize,
    config: &<MC::CB as ComponentsBuilder<MC>>::BuilderConfig,
) -> (
    ZeroCopyHandle,
    mpsc::Receiver<SuccessfulGroupData>,
    mpsc::Receiver<FailedGroupDataTransmission>,
) {
    // 调用 ManagerActor::spawn 函数。
    // 泛型参数 `MC` (ManagerComponents 的具体实现类型) 已经通过 `_components_token` 参数的类型推断出来。
    // 这个函数将根据指定的组件类型、通道缓冲区大小和配置来创建并运行一个新的 ManagerActor 实例。
    ManagerActor::<MC>::spawn(channel_buffer_size, config)
}
