//! 定义了在 Manager 启动时提供配置信息的 Trait (`SpawnInfoProvider`) 及其默认实现。
//! 这主要用于在 Manager Actor 任务开始运行时记录关键的配置参数到日志中。

use crate::defaults::DefaultManagerComponents; // 引入默认组件集合类型
use crate::manager::components::components_builder::{ComponentsBuilder, DefaultComponentsBuilder}; // 引入构建器 Trait 和默认实现
use crate::ManagerComponents; // 引入 ManagerComponents Trait
use std::num::NonZeroUsize; // 引入 NonZeroUsize 用于通道大小
use tracing::info; // 引入 info! 日志宏

/// `SpawnInfoProvider` Trait
///
/// 定义了一个接口，用于在 `ManagerActor` 启动时记录其配置信息。
/// 这个 Trait 通常由零大小类型 (Zero-Sized Type, ZST) 实现，因为它主要包含静态方法。
///
/// 泛型参数：
/// - `B`: 实现了 `ComponentsBuilder<C>` 的类型，提供了构建器的配置类型 `BuilderConfig`。
/// - `C`: 实现了 `ManagerComponents` 的类型。
pub trait SpawnInfoProvider<B: ComponentsBuilder<C>, C: ManagerComponents>: 'static {
    /// 记录 Manager 启动时的配置信息。
    /// 默认实现只记录通道缓冲区大小。
    /// 实现者可以重写此方法以记录更多特定于配置的信息。
    ///
    /// # Arguments
    /// * `chan_size` - 用于 Manager 内部 MPSC 通道的缓冲区大小。
    /// * `_config` - 对用于构建组件的配置 (`B::BuilderConfig`) 的引用。
    fn info(chan_size: NonZeroUsize, _config: &B::BuilderConfig) {
        // 默认只记录通道大小
        info!("(Manager) 任务已启动。通道缓冲区大小: {}", chan_size);
    }
}

/// `SpawnInfoProvider` Trait 的默认实现。
/// 这是一个零大小类型 (ZST)。
pub struct DefaultSpawnInfoProvider;

// 为 DefaultSpawnInfoProvider 实现 SpawnInfoProvider Trait
// 指定关联的 Builder 是 DefaultComponentsBuilder，组件集合是 DefaultManagerComponents
impl SpawnInfoProvider<DefaultComponentsBuilder, DefaultManagerComponents>
    for DefaultSpawnInfoProvider
{
    /// `DefaultSpawnInfoProvider` 的 `info` 方法实现。
    /// 它会记录通道缓冲区大小和 `DefaultComponentsBuilder` 所需的配置参数（即最小分组提交大小）。
    ///
    /// # Arguments
    /// * `chan_size` - 通道缓冲区大小。
    /// * `_config` - 对 `DefaultComponentsBuilder` 配置 (`MinGroupCommitSizeParam`，即 `usize`) 的引用。
    fn info(
        chan_size: NonZeroUsize,
        // 从关联的 Builder 类型获取配置类型
        _config: &<DefaultComponentsBuilder as ComponentsBuilder<DefaultManagerComponents>>::BuilderConfig,
    ) {
        // 记录通道大小和最小分组提交大小
        info!(
            "(Manager) 任务已启动。通道缓冲区大小: {}, 最小分组提交大小: {}",
            chan_size, // 通道大小
            _config,   // 最小分组提交大小 (usize)
        );
    }
}
