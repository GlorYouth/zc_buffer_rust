use crate::defaults::DefaultManagerComponents;
use crate::manager::components::components_builder::{ComponentsBuilder, DefaultComponentsBuilder};
use crate::ManagerComponents;
use std::num::NonZeroUsize;
use tracing::info;

pub trait SpawnInfoProvider<B: ComponentsBuilder<C>, C: ManagerComponents>: 'static {
    fn info(chan_size: NonZeroUsize, _config: &B::BuilderConfig) {
        info!("(Manager) 任务已启动。通道缓冲区: {}", chan_size,);
    }
}

pub struct DefaultSpawnInfoProvider;

impl SpawnInfoProvider<DefaultComponentsBuilder, DefaultManagerComponents>
    for DefaultSpawnInfoProvider
{
    fn info(
        chan_size: NonZeroUsize,
        _config: &<DefaultComponentsBuilder as ComponentsBuilder<DefaultManagerComponents>>::BuilderConfig,
    ) {
        info!(
            "(Manager) 任务已启动。通道缓冲区: {}, 最小分组提交大小: {}",
            chan_size, _config,
        );
    }
}
