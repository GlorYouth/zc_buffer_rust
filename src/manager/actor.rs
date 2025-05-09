use crate::manager::allocators::{DefaultReservationAllocator, ReservationAllocator};
use crate::manager::components::finalization_handler::{
    DefaultFinalizationHandler, FinalizationHandler,
};
use crate::manager::components::group_data_processor::{
    DefaultGroupDataProcessor, GroupDataProcessor,
};
use crate::manager::components::group_lifecycle::{
    DefaultGroupLifecycleManager, GroupLifecycleManager,
};
use crate::types::Request;
use crate::{
    FailedGroupDataTransmission, FinalizeResult, ManagerError, SuccessfulGroupData, ZeroCopyHandle,
};
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Manager Actor 结构体定义
pub struct Manager {
    /// 接收来自 Handle 的请求的 MPSC 通道接收端
    request_rx: mpsc::Receiver<Request>,
    /// 发送成功合并的分组数据给消费者的 MPSC 通道发送端
    pub(crate) completed_data_tx: mpsc::Sender<SuccessfulGroupData>,
    /// 发送失败分组数据给消费者的 MPSC 通道发送端
    pub(crate) failed_data_tx: mpsc::Sender<FailedGroupDataTransmission>,

    /// 预留分配器，用于管理 ReservationId 和 AbsoluteOffset
    pub(crate) reservation_allocator: Box<dyn ReservationAllocator + Send>,
    /// 分组生命周期管理器
    pub(crate) group_lifecycle_manager: Box<dyn GroupLifecycleManager + Send>,
    /// 已提交数据处理器
    pub(crate) group_data_processor: Box<dyn GroupDataProcessor + Send + Sync>, // Processor 也需要 Sync
    /// 新增, Sync for async method
    finalization_handler: Box<dyn FinalizationHandler + Send + Sync>, // 新增, Sync for async method

    /// 标记 Manager 是否正在执行 Finalize 操作。
    pub(crate) is_finalizing: bool,
}

// Manager 的实现块
impl Manager {
    /// 启动 Manager Actor 任务
    pub fn spawn(
        channel_buffer_size: NonZeroUsize,
        min_group_commit_size_param: usize,
    ) -> (
        ZeroCopyHandle,
        mpsc::Receiver<SuccessfulGroupData>,
        mpsc::Receiver<FailedGroupDataTransmission>,
    ) {
        let chan_size = usize::from(channel_buffer_size);
        let (request_tx, request_rx) = mpsc::channel(chan_size);
        let (completed_data_tx, completed_data_rx) = mpsc::channel(chan_size);
        let (failed_data_tx, failed_data_rx) = mpsc::channel(chan_size);

        let reservation_allocator = Box::new(DefaultReservationAllocator::new());
        let group_lifecycle_manager = Box::new(DefaultGroupLifecycleManager::new(
            min_group_commit_size_param,
        ));
        let group_data_processor = Box::new(DefaultGroupDataProcessor::new()); // <--- 初始化 Processor
        let finalization_handler = Box::new(DefaultFinalizationHandler::new()); // 初始化

        let manager = Manager {
            request_rx,
            completed_data_tx,
            failed_data_tx,
            reservation_allocator,
            group_lifecycle_manager,
            group_data_processor,
            finalization_handler, // 设置
            is_finalizing: false,
        };

        let handle = ZeroCopyHandle::new(request_tx);
        tokio::spawn(manager.run());
        info!(
            "(Manager) 任务已启动。通道缓冲区: {}, 最小分组提交大小: {}",
            chan_size, min_group_commit_size_param
        );
        (handle, completed_data_rx, failed_data_rx)
    }

    /// Manager 的主事件循环
    async fn run(mut self) {
        info!("(Manager) 事件循环开始。");
        loop {
            // 使用 tokio::select! 监听请求通道
            tokio::select! {
                // 接收来自 Handle 的请求
                maybe_request = self.request_rx.recv() => {
                    match maybe_request {
                        Some(request) => {
                            trace!("(Manager) 收到请求: {:?}", request);
                            // 处理请求，如果 handle_request 返回 false，则停止循环
                            if !self.handle_request(request).await {
                                info!("(Manager) handle_request 指示停止事件循环 (通常在 Finalize 后)。");
                                break; // 退出循环
                            }
                        }
                        None => {
                            // 请求通道关闭，表示所有 Handle 都已 Drop，触发 Finalize
                            info!("(Manager) 请求通道已关闭 (Handle 可能已 Drop)，开始 Finalize...");
                            let _finalize_report = self.finalize_internal().await; // 调用 finalize 逻辑
                            // Finalize 报告内容已在 finalize_internal 中记录或发送
                            info!("(Manager) Finalize (因通道关闭) 完成。");
                            break; // 退出循环
                        }
                    }
                }
                // 可以添加其他 select 分支，例如定时器或外部关闭信号
                // _ = tokio::time::sleep(Duration::from_secs(60)) => {
                //     // 定时任务，例如检查超时等
                // }
            }
        }
        info!("(Manager) 事件循环结束。正在关闭数据通道...");
        // 显式关闭数据通道 (虽然 Drop 会自动关闭，但显式调用更清晰)
        // drop(self.completed_data_tx); // Sender 在 Drop 时会自动关闭通道
        // drop(self.failed_data_tx);
        info!("(Manager) 数据通道已关闭。Manager 任务退出。");
    }

    /// 处理单个请求的核心逻辑分发
    /// 将具体处理委托给 `handlers` 和 `finalize` 子模块中的方法。
    /// 返回 `true` 继续运行，`false` 停止事件循环。
    async fn handle_request(&mut self, request: Request) -> bool {
        // 检查是否正在 Finalize
        if self.is_finalizing {
            // 如果正在 Finalize，只处理重复的 Finalize 请求，拒绝其他所有请求
            match request {
                Request::Finalize { reply_tx } => {
                    warn!("(Manager) Finalizing 状态下收到重复的 Finalize 请求，忽略并回复 None");
                    // 告知调用者 Finalize 已在进行或已完成
                    let _ = reply_tx.send(None);
                }
                Request::Reserve(req) => {
                    warn!("(Manager) Finalizing 状态，拒绝 Reserve 请求");
                    let _ = req.reply_tx.send(Err(ManagerError::ManagerFinalizing));
                }
                Request::SubmitBytes(req) => {
                    warn!("(Manager) Finalizing 状态，拒绝 SubmitBytes 请求");
                    let _ = req.reply_tx.send(Err(ManagerError::ManagerFinalizing));
                }
                Request::FailedInfo(req) => {
                    // Agent Drop 时发送，如果 Manager 正在 Finalize，此信息可能已无意义或已被处理
                    warn!(
                        "(Manager) Finalizing 状态，忽略 FailedInfo 请求 for Res {}",
                        req.info.id
                    );
                }
            }
            // 即使收到请求，只要在 Finalizing 状态，就应该继续运行等待 Finalize 完成，所以返回 true
            return true;
        }

        // 正常状态下的请求分发，调用子模块中的处理函数
        match request {
            Request::Reserve(req) => {
                // 调用 handlers 模块处理 Reserve 请求
                self.handle_reserve(req); // handle_reserve 是同步的
            }
            Request::SubmitBytes(req) => {
                // 调用 handlers 模块处理 SubmitBytes 请求
                self.handle_submit_bytes(req).await;
            }
            Request::FailedInfo(req) => {
                // 调用 handlers 模块处理 Agent Drop 时的 FailedInfo 请求
                self.handle_failed_info(req).await;
            }
            Request::Finalize { reply_tx } => {
                info!("(Manager) 收到 Finalize 请求，开始执行...");
                // 调用 finalize 模块处理 Finalize 请求
                let finalize_result: Option<FinalizeResult> = self.finalize_internal().await; // finalize_internal 是异步的

                // 记录 Finalize 报告的生成情况
                if let Some(ref report) = finalize_result {
                    info!(
                        "(Manager) Finalize 执行完成，生成报告（含 {} 个需报告的失败组信息）。",
                        report.failed_len()
                    );
                } else {
                    // finalize_internal 在已经在 finalize 时会返回 None
                    info!("(Manager) Finalize 执行完成，但未生成新报告 (可能内部 Finalize 已被触发或无需报告)。");
                }

                // 发送 Finalize 结果给调用者
                if reply_tx.send(finalize_result).is_err() {
                    error!("(Manager) 发送 Finalize 结果给调用者失败 (通道可能已关闭)");
                }
                // Finalize 请求是终止信号，返回 false 停止事件循环
                return false;
            }
        }
        // 处理完常规请求后继续运行
        true
    }

    // `finalize_internal` 现在主要委托给 FinalizationHandler
    async fn finalize_internal(&mut self) -> Option<FinalizeResult> {
        if self.is_finalizing {
            warn!("(Manager) Finalize 已在进行中，忽略重复调用");
            return None;
        }
        info!("(Manager) 开始内部 Finalize...");
        self.is_finalizing = true;

        // 注意：这里需要传递对 Manager 子组件的可变或不可变引用。
        // self.finalization_handler 是 Box<dyn Trait>，所以 &*self.finalization_handler 是 &dyn Trait。
        // 对于需要 &mut self 的 Trait 方法，需要 &mut *self.finalization_handler。
        // finalize_all_groups 设计为接收 &self，但操作的是传入的可变组件。
        let result = self
            .finalization_handler
            .finalize_all_groups(
                &mut *self.group_lifecycle_manager, // 传递可变引用
                &*self.group_data_processor,        // 传递不可变引用
                &self.completed_data_tx,
                &self.failed_data_tx,
            )
            .await;

        // is_finalizing 保持 true，因为 Manager 循环将基于 handle_request 的返回值停止。
        // 或者在这里设置 is_finalizing = false 如果 finalize 是可重入的（但当前设计不是）。
        info!("(Manager) 内部 Finalize 调用 FinalizationHandler 完成。");
        Some(result)
    }
}
