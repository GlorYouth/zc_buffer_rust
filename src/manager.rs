//! Manager Actor 的主模块定义和核心事件循环
//!
//! 包含 Manager 结构体定义、启动函数 (`spawn`)、主事件循环 (`run`)
//! 以及请求分发逻辑 (`handle_request`)。
//! 具体的请求处理逻辑、辅助函数和 Finalize 逻辑被分别放在子模块中。

// 声明子模块 (Declare submodules)
mod actor;
mod allocators;
mod components;
mod operations;
mod state;

pub use actor::Manager;
