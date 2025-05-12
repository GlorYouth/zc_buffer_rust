//! Manager 组件构建器 Trait (`ComponentsBuilder`) 及其默认实现。
//!
//! 这个模块定义了 `ComponentsBuilder` Trait，负责根据配置实例化一套完整的 Manager 核心组件。
//! 它使得组件的创建过程与 `ManagerActor` 本身解耦，并允许通过不同的 Builder 实现来定制组件的创建方式。

use crate::defaults::{
    DefaultFinalizationHandler, DefaultGroupDataProcessor, DefaultGroupLifecycleManager,
    DefaultManagerComponents, DefaultReservationAllocator,
};
use crate::ManagerComponents;

/// 用于构建 `ManagerActor` 核心组件集合的 Trait。
///
/// 这个 Trait 定义了一个标准的接口，用于根据给定的配置 (`BuilderConfig`) 创建一套完整的 Manager 组件实例。
/// 它通过泛型参数 `C: ManagerComponents` 关联到具体的组件类型集合，确保创建出的组件与 `ManagerActor` 的期望相符。
///
/// 通过实现这个 Trait，可以自定义组件的初始化过程，例如注入不同的依赖或使用不同的配置参数。
pub trait ComponentsBuilder<C: ManagerComponents>: 'static {
    /// 构建这套组件所需的配置参数类型。
    /// 不同的 Builder 可能需要不同的配置信息。
    type BuilderConfig;

    /// 根据提供的配置，构建并返回一套完整的 Manager 组件实例。
    ///
    /// 返回的元组包含按照 `ManagerComponents` Trait 定义顺序排列的组件实例：
    /// `(ReservationAllocator, GroupLifecycleManager, GroupDataProcessor, FinalizationHandler)`
    /// 这些组件的具体类型由泛型参数 `C` (实现了 `ManagerComponents` 的类型) 决定。
    ///
    /// # Arguments
    /// * `builder_config` - 一个对 `BuilderConfig` 类型实例的引用，包含构建组件所需的配置。
    ///
    /// # Returns
    /// 一个包含四个组件实例的元组 `(C::RA, C::GLM, C::GDP, C::FH)`。
    fn build(builder_config: &Self::BuilderConfig) -> (C::RA, C::GLM, C::GDP, C::FH);
}

/// 默认的组件构建器。
///
/// 这个结构体实现了 `ComponentsBuilder<DefaultManagerComponents>` Trait，
/// 用于创建一套默认的 Manager 组件 (`DefaultReservationAllocator`, `DefaultGroupLifecycleManager` 等)。
pub struct DefaultComponentsBuilder;

/// `DefaultComponentsBuilder` 所需的配置参数类型别名。
/// 当前默认构建器只需要一个参数：最小分组提交大小。
pub type MinGroupCommitSizeParam = usize;

// 为 DefaultComponentsBuilder 实现 ComponentsBuilder Trait，并指定目标组件集合为 DefaultManagerComponents
impl ComponentsBuilder<DefaultManagerComponents> for DefaultComponentsBuilder {
    /// 默认构建器所需的配置类型，即最小分组提交大小。
    type BuilderConfig = MinGroupCommitSizeParam;

    /// 构建默认组件集合的实现。
    ///
    /// # Arguments
    /// * `builder_config` - 对最小分组提交大小 (`usize`) 的引用。
    ///
    /// # Returns
    /// 一个包含默认组件实例的元组。
    fn build(
        builder_config: &Self::BuilderConfig,
    ) -> (
        <DefaultManagerComponents as ManagerComponents>::RA,
        <DefaultManagerComponents as ManagerComponents>::GLM,
        <DefaultManagerComponents as ManagerComponents>::GDP,
        <DefaultManagerComponents as ManagerComponents>::FH,
    ) {
        let min_group_commit_size_param = *builder_config;
        let ra = DefaultReservationAllocator::new();
        let glm = DefaultGroupLifecycleManager::new(min_group_commit_size_param);
        let gdp = DefaultGroupDataProcessor::new();
        let fh = DefaultFinalizationHandler::new();
        (ra, glm, gdp, fh)
    }
}
