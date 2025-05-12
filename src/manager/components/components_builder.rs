use crate::defaults::{
    DefaultFinalizationHandler, DefaultGroupDataProcessor, DefaultGroupLifecycleManager,
    DefaultManagerComponents, DefaultReservationAllocator,
};
use crate::ManagerComponents;

/// 用于构建 ManagerActor 核心组件的 Trait。
/// 它依赖于实现了 `ManagerComponents` 的类型来获取组件的具体类型信息。
pub trait ComponentsBuilder<C: ManagerComponents>: 'static {
    /// 构建组件所需的配置类型，通常与 `C::Config` 相同。
    type BuilderConfig;

    /// 一个方法来创建所有组件。
    /// 返回的组件类型由泛型参数 `C`（ManagerComponents 的实现类型）来决定。
    fn build(builder_config: &Self::BuilderConfig) -> (C::RA, C::GLM, C::GDP, C::FH);
}

pub struct DefaultComponentsBuilder;
pub type MinGroupCommitSizeParam = usize;

impl ComponentsBuilder<DefaultManagerComponents> for DefaultComponentsBuilder {
    type BuilderConfig = MinGroupCommitSizeParam;

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
        let gdp = DefaultGroupDataProcessor::new(); // <--- 初始化 Processor
        let fh = DefaultFinalizationHandler::new(); // 初始化
        (ra, glm, gdp, fh)
    }
}
