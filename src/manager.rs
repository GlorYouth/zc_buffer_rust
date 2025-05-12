//! Manager Actor 的主模块定义和核心事件循环
//!
//! 包含 Manager 结构体定义、启动函数 (`spawn`)、主事件循环 (`run`)
//! 以及请求分发逻辑 (`handle_request`)。
//! 具体的请求处理逻辑、辅助函数和 Finalize 逻辑被分别放在子模块中。

// 声明子模块 (Declare submodules)
mod actor;
// 将子模块设为公开 (Make submodules public)
pub mod components;
mod operations;
pub mod state;

pub use actor::ManagerActor;

pub use components::{
    DefaultFinalizationHandler, DefaultGroupDataProcessor, DefaultGroupLifecycleManager,
    DefaultManagerComponents, DefaultReservationAllocator, ManagerComponents,
};

pub use components::MinGroupCommitSizeParam as _MinGroupCommitSizeParam;

/// 中文注释：一个宏，用于简化 `ManagerActor` 的启动调用。
///
/// 支持使用默认组件或自定义组件（通过ZST标记）进行启动。
///
/// **形式一：使用默认组件**
/// ```
/// let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
/// rt.block_on(async move {
///     use zc_buffer::spawn_manager;
///     let min_group_commit_size = 100;  // 或自定义组件对应的配置类型实例
///     let buffer_size = std::num::NonZeroUsize::new(128).unwrap();
///     let (handle, completed_rx, failed_rx) = spawn_manager!(default, buffer_size, &min_group_commit_size);
/// })
/// ```
///
/// **形式二：使用自定义组件 (通过 `build_manager_components!` 创建的ZST)**
/// ```ignore
/// let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
/// rt.block_on(async move {
///     use zc_buffer::{spawn_manager,build_manager_components,ManagerComponents};
///     let custom_components_zst = ... {
///         //reservation_allocator: MyCustomRA,
///     };
///     let min_group_commit_size = 100;  // 或自定义组件对应的配置类型实例
///     let buffer_size = std::num::NonZeroUsize::new(128).unwrap();
///     let (handle, completed_rx, failed_rx) =
///         spawn_manager!(custom_components_zst, buffer_size, &min_group_commit_size);
/// })
/// ```
#[macro_export]
macro_rules! spawn_manager {
    // 中文注释：形式一：使用默认组件启动。
    // 参数: default 关键字, 通道缓冲大小, Manager配置的引用。
    (default, $channel_buffer_size:expr, $config:expr) => {
        // 中文注释：直接调用 ManagerActor::spawn，并明确指定 DefaultManagerComponents。
        $crate::ManagerActor::<$crate::defaults::DefaultManagerComponents>::spawn(
            $channel_buffer_size,
            $config
        )
    };

    // 中文注释：形式二：使用自定义组件的ZST实例启动。
    // 参数: ZST实例表达式, 通道缓冲大小, Manager配置的引用。
    ($components_zst_expr:expr, $channel_buffer_size:expr, $config:expr) => {
        // 中文注释：调用之前定义的辅助函数 `spawn_manager_with_components_token`。
        // 该函数通过ZST实例推断组件类型。
        $crate::_spawn_manager_with_components_token( // 假设辅助函数位于 crate::manager 模块下
            $components_zst_expr,
            $channel_buffer_size,
            $config
        )
    };
}
