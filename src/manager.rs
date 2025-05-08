//! Manager Actor 的主模块定义和核心事件循环
//!
//! 包含 Manager 结构体定义、启动函数 (`spawn`)、主事件循环 (`run`)
//! 以及请求分发逻辑 (`handle_request`)。
//! 具体的请求处理逻辑、辅助函数和 Finalize 逻辑被分别放在子模块中。

// 声明子模块 (Declare submodules)
mod finalize;
mod handlers;
mod helpers;

// 引入必要的类型和库 (Import necessary types and libraries)
use crate::error::ManagerError;
use crate::handle::ZeroCopyHandle; // 引入公开的 Handle
use crate::types::{
    self, // 引入 types 模块本身以便限定类型
    AbsoluteOffset,
    FailedGroupDataTransmission, // 引入失败分组传输类型
    FinalizeResult,              // 引入 Finalize 结果类型
    GroupId,
    GroupState,
    Request,
    ReservationId,
    ReserveRequest,
    SubmitBytesRequest,
    SuccessfulGroupData,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn}; // 引入 tracing

/// Manager Actor 结构体定义
pub struct Manager {
    /// 接收来自 Handle 的请求的 MPSC 通道接收端
    request_rx: mpsc::Receiver<Request>,
    /// 发送成功合并的分组数据给消费者的 MPSC 通道发送端
    completed_data_tx: mpsc::Sender<SuccessfulGroupData>,
    /// 发送失败分组数据给消费者的 MPSC 通道发送端
    failed_data_tx: mpsc::Sender<FailedGroupDataTransmission>, // 类型不变

    /// 存储当前活跃的或正在处理的分组状态。Key 是 `GroupId`。
    groups: HashMap<GroupId, GroupState>,
    /// 用于生成下一个唯一的预留 ID 的计数器
    next_reservation_id: ReservationId,
    /// 用于生成下一个唯一的分组 ID 的计数器
    next_group_id: GroupId,
    /// 指向下一个可分配的缓冲区逻辑绝对偏移量
    next_allocation_offset: AbsoluteOffset,

    /// 当前活跃（未密封）的分组 ID。
    active_group_id: Option<GroupId>,
    /// 触发分组密封和发送的最小字节数阈值。
    min_group_commit_size: usize,

    /// 标记 Manager 是否正在执行 Finalize 操作。
    is_finalizing: bool,
}

// Manager 的实现块
impl Manager {
    /// 启动 Manager Actor 任务
    ///
    /// 返回与 Manager 交互的 Handle、成功数据的接收通道以及失败分组数据的接收通道。
    pub fn spawn(
        channel_buffer_size: NonZeroUsize,
        min_group_commit_size: usize,
    ) -> (
        ZeroCopyHandle,
        mpsc::Receiver<SuccessfulGroupData>,
        mpsc::Receiver<FailedGroupDataTransmission>, // 返回类型不变
    ) {
        let chan_size = usize::from(channel_buffer_size);
        // 创建用于请求、成功数据和失败数据的通道
        let (request_tx, request_rx) = mpsc::channel(chan_size);
        let (completed_data_tx, completed_data_rx) = mpsc::channel(chan_size);
        let (failed_data_tx, failed_data_rx): (
            mpsc::Sender<FailedGroupDataTransmission>,
            mpsc::Receiver<FailedGroupDataTransmission>,
        ) = mpsc::channel(chan_size);

        // 初始化 Manager 状态
        let manager = Manager {
            request_rx,
            completed_data_tx,
            failed_data_tx,
            groups: HashMap::new(),
            next_reservation_id: 0,
            next_group_id: 0,
            next_allocation_offset: 0,
            active_group_id: None,
            min_group_commit_size,
            is_finalizing: false,
        };

        // 创建 Handle 并启动 Manager 的主循环任务
        let handle = ZeroCopyHandle::new(request_tx);
        tokio::spawn(manager.run()); // manager 的所有权转移到新任务
        info!(
            "(Manager) 任务已启动。通道缓冲区: {}, 最小分组提交大小: {}",
            chan_size, min_group_commit_size
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
}
