// 声明模块 (Declare modules)
mod agent; // agent 模块
mod error;
mod handle;
mod manager;
mod types;

// 公开导出需要被外部 (如 main 函数) 使用的类型
pub use agent::{ChunkAgent, SingleAgent, SubmitAgent}; // 导出 Agent 类型
pub use error::{BufferError, ManagerError}; // 导出错误类型
pub use handle::ZeroCopyHandle; // 导出 Handle
pub use manager::Manager; // 导出 Manager Actor
pub use types::{
    AbsoluteOffset,
    FailedGroupData,
    FailedGroupDataTransmission,
    FailedReservationInfo,
    FinalizeResult,
    SuccessfulGroupData, // 导出主要的数据类型
};
