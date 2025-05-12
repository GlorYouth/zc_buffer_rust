//! Manager 操作逻辑模块
//!
//! 这个模块组织了 `ManagerActor` 处理各种请求和内部操作的具体实现逻辑。
//!
//! 它包含以下子模块：
//! - `request_handlers`: 处理来自 `ZeroCopyHandle` 的外部请求（如 Reserve, Submit, FailedInfo）。
//! - `group_processing`: 处理与 `BufferGroup` 相关的内部操作（如密封分组、合并数据）。

// 公开 group_processing 模块，因为它可能包含 ManagerActor 需要直接调用的辅助函数或类型
pub(crate) mod group_processing;
// request_handlers 模块设为 crate 内可见，其处理逻辑通过 ManagerActor 的方法暴露
mod request_handlers;
