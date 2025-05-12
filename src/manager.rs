//! 定义 `ManagerActor` 及其核心逻辑。
//!
//! 这个模块是整个零拷贝缓冲库的核心，负责管理缓冲区的状态、处理来自
//! `ZeroCopyHandle` 的请求（如预留空间、提交数据、最终化），以及与配置的
//! 组件（分配器、生命周期管理器、数据处理器、最终化处理器）进行交互。
//!
//! 主要包含：
//! - `ManagerActor` 结构体定义。
//! - 启动 `ManagerActor` 的 `spawn` 函数。
//! - 核心的异步事件循环 `run` 方法。
//! - 请求分发逻辑 `handle_request`。
//! - 用于简化启动的 `spawn_manager!` 宏。
//!
//! 具体的实现细节分布在子模块中：
//! - `actor`: 包含 `ManagerActor` 结构体和其 `impl` 块。
//! - `components`: 定义了 `ManagerActor` 依赖的各种组件 trait 及其默认实现。
//! - `operations`: 包含处理具体请求（如 reserve, submit, finalize）的函数。
//! - `state`: 定义了 `ManagerActor` 内部使用的状态结构体。

// 声明子模块
mod actor;
// `ManagerActor` 结构体和 `impl` 块
pub mod components;
// 定义组件 trait 和默认实现
mod operations;
// 处理具体操作的函数
pub mod state;
// 定义 Manager 内部状态结构

// 公开导出 ManagerActor 结构体，以便用户可以指定泛型参数
pub use actor::ManagerActor;

// 公开导出默认组件和 ManagerComponents trait
pub use components::{
    DefaultFinalizationHandler,
    DefaultGroupDataProcessor,
    DefaultGroupLifecycleManager,
    DefaultManagerComponents,
    DefaultReservationAllocator,
    ManagerComponents, // 导出核心组件 Trait
};

// 公开导出内部使用的参数类型（重命名以提示其内部性）
pub use components::MinGroupCommitSizeParam as _MinGroupCommitSizeParam;

/// 一个宏，用于简化 `ManagerActor` 的启动过程。
///
/// 这个宏提供了两种方式来启动 `ManagerActor`：
/// 1. 使用库提供的默认组件 (`DefaultManagerComponents`)。
/// 2. 使用用户自定义的组件集合（通过一个实现了 `ManagerComponents` 的 ZST 标记类型指定）。
///
/// # 使用默认组件
///
/// ```rust
/// # use zc_buffer::{spawn_manager, defaults::DefaultManagerComponents, manager::components::ComponentsBuilder};
/// # use std::num::NonZeroUsize;
/// # use tokio::runtime::Runtime;
/// #
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     let rt = Runtime::new()?;
/// #     rt.block_on(async {
/// // 使用 `default` 关键字指定使用默认组件。
/// // `config` 参数需要是 `DefaultManagerComponents` 的 `ComponentsBuilder` 配置类型。
/// // 对于默认实现，配置是 `MinGroupCommitSizeParam` (通常是一个 `usize`)。
/// let min_group_commit_size = 100usize;
/// let buffer_size = NonZeroUsize::new(128).unwrap();
///
/// // 调用宏启动 ManagerActor
/// let (handle, completed_rx, failed_rx) =
///     spawn_manager!(default, buffer_size, &min_group_commit_size);
/// #     });
/// #     Ok(())
/// # }
/// ```
///
/// # 使用自定义组件
///
/// ```rust,ignore
/// use zc_buffer::{spawn_manager, ManagerComponents, build_manager_components, components::*};
/// use std::num::NonZeroUsize;
/// use tokio::runtime::Runtime;
///
/// // 假设你定义了自己的组件实现
/// struct MyAllocator;
/// impl ReservationAllocator for MyAllocator { /* ... */ }
/// // ... 其他自定义组件 ...
///
/// // 使用 build_manager_components! (或手动实现) 创建一个 ZST 标记类型
/// build_manager_components! {
///     struct MyComponents;
///     // 指定自定义组件，未指定的将使用默认实现
///     reservation_allocator: MyAllocator,
///     // 如果自定义组件有不同的配置类型，也需要在此指定或实现 ComponentsBuilder
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     let rt = Runtime::new()?;
/// #     rt.block_on(async {
/// // `config` 需要是 `MyComponents` 的 `ComponentsBuilder` 配置类型。
/// // 假设自定义组件也使用 usize 作为配置。
/// let custom_config = 100usize;
/// let buffer_size = NonZeroUsize::new(128).unwrap();
///
/// // 传入 ZST 实例 (MyComponents) 来指定组件集
/// let (handle, completed_rx, failed_rx) =
///     spawn_manager!(MyComponents, buffer_size, &custom_config);
/// #     });
/// #     Ok(())
/// # }
/// ```
///
/// # 参数
///
/// - **第一个参数**: `default` 关键字或一个实现了 `ManagerComponents` 的 ZST 实例表达式。
/// - **第二个参数**: `channel_buffer_size` (`NonZeroUsize`)，用于 Manager 内部 MPSC 通道的缓冲区大小。
/// - **第三个参数**: `config` (引用)，对应于所选 `ManagerComponents` 的 `ComponentsBuilder` 配置类型。
///
/// # 返回值
///
/// 与 `ManagerActor::spawn` 或 `_spawn_manager_with_components_token` 相同：
/// `(ZeroCopyHandle, mpsc::Receiver<SuccessfulGroupData>, mpsc::Receiver<FailedGroupDataTransmission>)`
#[macro_export]
macro_rules! spawn_manager {
    // 模式一：匹配 `default` 关键字
    // $channel_buffer_size: 接收一个表达式，计算结果是通道缓冲区大小 (NonZeroUsize)
    // $config: 接收一个表达式，计算结果是对配置的引用
    (default, $channel_buffer_size:expr, $config:expr) => {
        // 直接调用 ManagerActor::spawn，并显式指定使用默认组件集合
        // `$crate` 指向包含此宏定义的 crate (即 zc_buffer 本身)
        $crate::ManagerActor::<$crate::defaults::DefaultManagerComponents>::spawn(
            $channel_buffer_size,
            $config
        )
    };

    // 模式二：匹配一个组件 ZST 实例表达式
    // $components_zst_expr: 接收一个表达式，计算结果是一个实现了 ManagerComponents 的 ZST 实例
    // $channel_buffer_size: 同上
    // $config: 同上
    ($components_zst_expr:expr, $channel_buffer_size:expr, $config:expr) => {
        // 调用位于 crate 根作用域下的辅助函数 `_spawn_manager_with_components_token`
        // 这个辅助函数使用 ZST 实例的类型来推断 ManagerActor 需要使用的组件类型。
        $crate::_spawn_manager_with_components_token(
            $components_zst_expr,
            $channel_buffer_size,
            $config
        )
    };
}
