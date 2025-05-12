mod components_builder;
pub mod finalization_handler;
pub mod group_data_processor;
pub mod group_lifecycle;
pub mod reservation_allocators;
mod spawn_info_provider;

pub use components_builder::{ComponentsBuilder, DefaultComponentsBuilder};
pub use finalization_handler::{DefaultFinalizationHandler, FinalizationHandler};
pub use group_data_processor::{DefaultGroupDataProcessor, GroupDataProcessor};
pub use group_lifecycle::{DefaultGroupLifecycleManager, GroupLifecycleManager};
pub use reservation_allocators::{DefaultReservationAllocator, ReservationAllocator};
pub use spawn_info_provider::{DefaultSpawnInfoProvider, SpawnInfoProvider};

pub use components_builder::MinGroupCommitSizeParam;

/// Manager的核心组件配置 Trait
///
/// 这个Trait允许用户通过关联类型自定义ManagerActor内部使用的各个组件的具体实现。
pub trait ManagerComponents: 'static + Sized {
    /// 预留分配器的具体类型
    type RA: ReservationAllocator;
    /// 分组生命周期管理器的具体类型
    type GLM: GroupLifecycleManager;
    /// 已提交数据处理器的具体类型
    type GDP: GroupDataProcessor;
    /// 最终处理程序的具体类型
    type FH: FinalizationHandler;

    type CB: ComponentsBuilder<Self>;

    type SIP: SpawnInfoProvider<Self::CB, Self>;
}

pub struct DefaultManagerComponents;

impl ManagerComponents for DefaultManagerComponents {
    type RA = DefaultReservationAllocator;
    type GLM = DefaultGroupLifecycleManager;
    type GDP = DefaultGroupDataProcessor;
    type FH = DefaultFinalizationHandler;

    type CB = DefaultComponentsBuilder;
    type SIP = DefaultSpawnInfoProvider;
}
