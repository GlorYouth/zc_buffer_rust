//! 定义和导出库的默认组件实现。
//! 这个模块旨在提供一套可以直接使用的标准组件，简化库的基本使用。

pub use crate::manager::{ // 从 `crate::manager` 模块中公开导出以下默认组件：
    DefaultFinalizationHandler,  // 默认的最终化处理器
    DefaultGroupDataProcessor,   // 默认的数据组处理器
    DefaultGroupLifecycleManager, // 默认的数据组生命周期管理器
    DefaultManagerComponents,     // 默认的 Manager 组件集合类型
    DefaultReservationAllocator, // 默认的预留空间分配器
    _MinGroupCommitSizeParam,    // 内部使用的最小组提交大小参数
};
