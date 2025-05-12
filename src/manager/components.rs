//! Manager 核心组件的定义与导出
//!
//! 这个模块定义了构成 `ManagerActor` 核心功能的各个组件的 Trait，
//! 并提供了一个默认的组件实现集合 (`DefaultManagerComponents`)。
//! 它还重新导出了各个子模块中的 Trait 和默认实现，方便外部使用。
//!
//! 主要内容包括：
//! - 定义 `ManagerComponents` Trait，用于组合不同的组件实现。
//! - 定义 `DefaultManagerComponents` 结构体，提供一套默认的组件类型。
//! - 声明和导出子模块 (如 `components_builder`, `finalization_handler` 等)。
//! - 重新导出子模块中的关键 Trait 和结构体。

// --- 子模块声明 ---
mod components_builder; // 定义如何构建组件集合
pub mod finalization_handler; // 定义 Finalize 逻辑的 Trait 和实现
pub mod group_data_processor; // 定义处理已提交数据的 Trait 和实现
pub mod group_lifecycle; // 定义管理分组生命周期的 Trait 和实现
pub mod reservation_allocators; // 定义预留空间分配逻辑的 Trait 和实现
mod spawn_info_provider; // 定义启动时打印配置信息的 Trait 和实现

// --- 公开导出子模块中的关键 Trait 和默认实现 ---
// 导出 ComponentsBuilder Trait 和其默认实现
pub use components_builder::{ComponentsBuilder, DefaultComponentsBuilder};
// 导出 FinalizationHandler Trait 和其默认实现
pub use finalization_handler::{DefaultFinalizationHandler, FinalizationHandler};
// 导出 GroupDataProcessor Trait 和其默认实现
pub use group_data_processor::{DefaultGroupDataProcessor, GroupDataProcessor};
// 导出 GroupLifecycleManager Trait 和其默认实现
pub use group_lifecycle::{DefaultGroupLifecycleManager, GroupLifecycleManager};
// 导出 ReservationAllocator Trait 和其默认实现
pub use reservation_allocators::{DefaultReservationAllocator, ReservationAllocator};
// 导出 SpawnInfoProvider Trait 和其默认实现
pub use spawn_info_provider::{DefaultSpawnInfoProvider, SpawnInfoProvider};

// 导出 DefaultComponentsBuilder 所需的配置参数类型
pub use components_builder::MinGroupCommitSizeParam;

/// `ManagerActor` 核心组件的集合 Trait。
///
/// 这个 Trait 定义了一套关联类型，代表了 `ManagerActor` 运行所需的各个核心逻辑组件。
/// 通过实现这个 Trait 并指定不同的关联类型，可以灵活地替换 `ManagerActor` 的内部行为，
/// 例如使用不同的预留分配策略、分组管理逻辑或数据处理方式。
///
/// `ManagerActor` 通过泛型参数 `C: ManagerComponents` 来接收具体的组件集合。
pub trait ManagerComponents: 'static + Sized { // 要求是静态生命周期且大小已知
    /// 预留空间分配器 (`Reservation Allocator`) 的具体类型。
    /// 负责处理 `Reserve` 请求，分配 `ReservationId` 和 `AbsoluteOffset`。
    type RA: ReservationAllocator;
    /// 分组生命周期管理器 (`Group Lifecycle Manager`) 的具体类型。
    /// 负责创建、跟踪和密封 `BufferGroup`。
    type GLM: GroupLifecycleManager;
    /// 已提交数据处理器 (`Group Data Processor`) 的具体类型。
    /// 负责接收和合并提交的数据块。
    type GDP: GroupDataProcessor;
    /// Finalize 操作处理器 (`Finalization Handler`) 的具体类型。
    /// 负责处理 `Finalize` 请求和相关的清理逻辑。
    type FH: FinalizationHandler;

    /// 组件构建器 (`Components Builder`) 的具体类型。
    /// 负责根据配置构建上述所有组件的实例。
    /// 关联类型 `Self` 指向实现了 `ManagerComponents` 的类型自身。
    type CB: ComponentsBuilder<Self>;

    /// 启动信息提供者 (`Spawn Info Provider`) 的具体类型。
    /// 负责在 Manager 启动时记录配置信息。
    /// 通常是一个零大小类型 (ZST)。
    type SIP: SpawnInfoProvider<Self::CB, Self>;
}

/// 默认的 Manager 组件集合。
///
/// 这个结构体提供了一套预定义的默认组件实现，
/// 作为 `ManagerComponents` Trait 的一个具体实现。
/// 当用户不需要自定义组件时，可以使用这个默认集合来方便地启动 `ManagerActor`。
pub struct DefaultManagerComponents;

// 为 DefaultManagerComponents 实现 ManagerComponents Trait
impl ManagerComponents for DefaultManagerComponents {
    /// 使用默认的预留分配器。
    type RA = DefaultReservationAllocator;
    /// 使用默认的分组生命周期管理器。
    type GLM = DefaultGroupLifecycleManager;
    /// 使用默认的数据处理器。
    type GDP = DefaultGroupDataProcessor;
    /// 使用默认的 Finalize 处理器。
    type FH = DefaultFinalizationHandler;

    /// 使用默认的组件构建器。
    type CB = DefaultComponentsBuilder;
    /// 使用默认的启动信息提供者。
    type SIP = DefaultSpawnInfoProvider;
}
