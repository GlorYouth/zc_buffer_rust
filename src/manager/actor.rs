//! `ManagerActor` 及其相关实现
//!
//! 这个模块定义了核心的 `ManagerActor` 结构体及其关联的 `spawn` 方法和主事件循环 `run`。
//! `ManagerActor` 是整个零拷贝缓冲系统的核心调度器，负责处理来自 `ZeroCopyHandle` 的请求，
//! 并协调内部组件 (如预留分配器、分组生命周期管理器、数据处理器和 Finalize 处理器) 来完成任务。

use crate::manager::components::finalization_handler::FinalizationHandler;
use crate::manager::components::{ComponentsBuilder, ManagerComponents, SpawnInfoProvider};
use crate::types::Request;
use crate::{FailedGroupDataTransmission, FinalizeResult, ManagerError, SuccessfulGroupData, ZeroCopyHandle};
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Manager Actor 负责实际的业务逻辑处理
///
/// 通过 MPSC 通道接收外部请求 (`Request`)，并驱动内部组件完成缓冲区的管理、
/// 数据块的合并、分组的提交以及最终的清理工作。
/// 使用泛型参数 `C: ManagerComponents` 来允许不同的组件实现组合，提高了系统的灵活性和可测试性。
pub struct ManagerActor<C: ManagerComponents> {
    /// 接收来自 `ZeroCopyHandle` 的请求的 MPSC 通道接收端。
    /// Manager 从此通道接收所有外部指令，如预留空间、提交数据、通知失败和触发 Finalize。
    request_rx: mpsc::Receiver<Request>,
    /// 发送成功合并的分组数据给消费者的 MPSC 通道发送端。
    /// 当一个数据分组成功合并并准备好被外部使用时，其数据 (`SuccessfulGroupData`) 会通过此通道发送出去。
    pub(crate) completed_data_tx: mpsc::Sender<SuccessfulGroupData>,
    /// 发送失败分组数据给消费者的 MPSC 通道发送端。
    /// 当一个分组因为包含未完成的预留或提交失败而无法成功合并时，
    /// 其相关信息 (`FailedGroupDataTransmission`) 会通过此通道发送出去，以便外部进行错误处理或记录。
    pub(crate) failed_data_tx: mpsc::Sender<FailedGroupDataTransmission>,

    // --- 核心业务逻辑组件 (通过泛型 C 注入) ---
    /// 预留空间分配器 (`Reservation Allocator`)。
    /// 负责处理 `Reserve` 请求，为新的写入操作分配唯一的 `ReservationId` 和在逻辑缓冲区中的 `AbsoluteOffset`。
    /// 它是管理缓冲区空间分配的核心。
    pub(crate) reservation_allocator: C::RA,
    /// 分组生命周期管理器 (`Group Lifecycle Manager`)。
    /// 负责创建、跟踪和密封数据分组 (`BufferGroup`)。
    /// 它决定何时创建新分组，何时将当前分组标记为已满（密封），并管理分组的状态转换。
    pub(crate) group_lifecycle_manager: C::GLM,
    /// 已提交数据处理器 (`Group Data Processor`)。
    /// 负责接收通过 `SubmitBytes` 或 `SubmitChunk` 提交的数据块，并将它们暂存到相应的分组中。
    /// 当分组被密封后，它负责将分组内的所有数据块合并成一个连续的数据块 (`Bytes`)。
    pub(crate) group_data_processor: C::GDP,
    /// Finalize 操作处理器 (`Finalization Handler`)。
    /// 负责处理 `Finalize` 请求。它会检查所有当前活动的分组，
    /// 处理任何未完成的预留（通常将它们标记为失败），并触发这些分组的最终处理（成功或失败）。
    /// 同时负责生成 `FinalizeResult` 报告。
    finalization_handler: C::FH,

    /// 标记 Manager 是否正在执行 Finalize 操作。
    /// 当 `Finalize` 请求被接收或 Handle 全部 Drop 导致通道关闭时，此标记设为 `true`。
    /// 在此状态下，Manager 会拒绝新的预留和提交请求，只处理重复的 Finalize 请求（忽略）和内部清理逻辑。
    /// Finalize 完成后，Manager 通常会停止事件循环。
    pub(crate) is_finalizing: bool,
}

// 为 ManagerActor 实现的方法
impl<C: ManagerComponents> ManagerActor<C> {
    /// 启动一个新的 Manager Actor 实例。
    ///
    /// 该方法执行以下操作:
    /// 1. 创建用于通信的 MPSC 通道 (请求通道、成功数据通道、失败数据通道)。
    /// 2. 使用指定的配置 (`config`) 和 `ComponentsBuilder` 构建所有必需的内部组件 (`ManagerComponents`)。
    /// 3. 初始化 `ManagerActor` 结构体实例。
    /// 4. 创建一个 `ZeroCopyHandle`，它是外部与 Manager 交互的句柄，并将请求通道的发送端交给它。
    /// 5. 在一个新的 Tokio 任务中启动 Manager 的主事件循环 (`run` 方法)。
    /// 6. 调用 `SpawnInfoProvider` 记录启动信息 (如配置参数)。
    /// 7. 返回 `ZeroCopyHandle` 以及成功/失败数据通道的接收端，供外部消费者使用。
    ///
    /// # Arguments
    /// * `channel_buffer_size` - 用于创建内部 MPSC 通道的缓冲区大小。
    /// * `config` - 特定于所选 `ComponentsBuilder` 的配置，用于初始化组件。
    ///
    /// # Returns
    /// 一个元组，包含:
    ///   - `ZeroCopyHandle`: 用于向 Manager 发送请求的句柄。
    ///   - `mpsc::Receiver<SuccessfulGroupData>`: 用于接收成功合并分组数据的通道接收端。
    ///   - `mpsc::Receiver<FailedGroupDataTransmission>`: 用于接收失败分组数据的通道接收端。
    pub fn spawn(
        channel_buffer_size: NonZeroUsize,
        config: &<<C as ManagerComponents>::CB as ComponentsBuilder<C>>::BuilderConfig,
    ) -> (
        ZeroCopyHandle,
        mpsc::Receiver<SuccessfulGroupData>,
        mpsc::Receiver<FailedGroupDataTransmission>,
    ) {
        // 根据 channel_buffer_size (NonZeroUsize) 获取 usize 值
        let chan_size = usize::from(channel_buffer_size);
        // 创建请求通道 (Handle -> Manager)
        let (request_tx, request_rx) = mpsc::channel(chan_size);
        // 创建成功数据通道 (Manager -> Success Consumer)
        let (completed_data_tx, completed_data_rx) = mpsc::channel(chan_size);
        // 创建失败数据通道 (Manager -> Failure Consumer)
        let (failed_data_tx, failed_data_rx) = mpsc::channel(chan_size);

        // 使用 ComponentsBuilder 构建 Manager 的内部组件
        // CB 是与泛型 C 关联的 ComponentsBuilder 类型
        let components = C::CB::build(config); // build 返回包含所有组件的元组
        // components.0: ReservationAllocator
        // components.1: GroupLifecycleManager
        // components.2: GroupDataProcessor
        // components.3: FinalizationHandler

        // 创建 ManagerActor 实例
        let manager: ManagerActor<C> = ManagerActor {
            request_rx, // 接收请求的通道末端
            completed_data_tx, // 发送成功数据的通道末端
            failed_data_tx,    // 发送失败数据的通道末端
            reservation_allocator: components.0, // 注入预留分配器
            group_lifecycle_manager: components.1, // 注入分组生命周期管理器
            group_data_processor: components.2, // 注入数据处理器
            finalization_handler: components.3, // 注入 Finalize 处理器
            is_finalizing: false, // 初始状态为非 Finalizing
        };

        // 创建 ZeroCopyHandle，将请求发送端交给它
        let handle = ZeroCopyHandle::new(request_tx);
        // 使用 tokio::spawn 启动 Manager 的主事件循环 (run 方法)
        // run 方法会消耗 manager 实例的所有权
        tokio::spawn(manager.run());
        // 调用 SpawnInfoProvider (通常是一个零大小类型 ZST) 记录启动日志
        // SIP 是与泛型 C 关联的 SpawnInfoProvider 类型
        C::SIP::info(channel_buffer_size, config);
        // 返回 Handle 和数据接收通道给调用者
        (handle, completed_data_rx, failed_data_rx)
    }

    /// Manager Actor 的主事件循环。
    ///
    /// 这个异步函数是 Manager Actor 的核心。它持续监听 `request_rx` 通道，
    /// 等待并处理传入的 `Request`。当收到请求时，它调用 `handle_request` 方法进行处理。
    ///
    /// 循环的终止条件:
    /// 1. `handle_request` 返回 `false`，通常发生在处理 `Request::Finalize` 之后。
    /// 2. `request_rx` 通道被关闭 (返回 `None`)，这表示所有的 `ZeroCopyHandle` 都已经被 Drop。
    ///    在这种情况下，Manager 会自动触发内部的 `finalize_internal` 逻辑，然后退出循环。
    ///
    /// 循环结束后，Manager 会记录日志并隐式地关闭其持有的数据发送通道 (`completed_data_tx`, `failed_data_tx`)，
    /// 通知下游消费者不会再有新的数据到来。
    async fn run(mut self) {
        info!("(Manager) 事件循环开始。");
        loop {
            // 使用 tokio::select! 宏同时等待来自请求通道的消息。
            // select! 允许多路复用多个异步操作，这里只监听了一个。
            tokio::select! {
                // biased; // 可以取消注释以优先处理请求，但通常不需要
                // 等待接收来自 Handle 的请求
                // `recv()` 返回 Option<Request>，`None` 表示通道已关闭
                maybe_request = self.request_rx.recv() => {
                    match maybe_request {
                        // 成功接收到请求
                        Some(request) => {
                            trace!("(Manager) 收到请求: {:?}", request);
                            // 调用 handle_request 处理请求
                            // 如果 handle_request 返回 false (通常在 Finalize 后)，则退出循环
                            if !self.handle_request(request).await {
                                info!("(Manager) handle_request 指示停止事件循环 (通常在 Finalize 后)。");
                                break; // 退出 loop
                            }
                        }
                        // 请求通道关闭 (所有 Handle 已 Drop)
                        None => {
                            // 这是触发 Finalize 的另一种方式 (除了显式调用)
                            info!("(Manager) 请求通道已关闭 (所有 Handle 已 Drop)，自动开始 Finalize...");
                            // 调用内部 Finalize 逻辑处理剩余的分组和预留
                            let _finalize_report = self.finalize_internal().await; // 忽略返回值，日志已在内部记录
                            // Finalize 报告内容已在 finalize_internal 中记录或发送
                            info!("(Manager) Finalize (因通道关闭) 完成。");
                            break; // 退出 loop
                        }
                    }
                }
                // --- 扩展点 ---
                 // 这里可以添加其他的 `select!` 分支来处理其他事件，例如：
                 // 1. 定时器: 定期执行清理、检查超时等任务。
                //    `_ = tokio::time::sleep(Duration::from_secs(60)) => { /* 定时任务逻辑 */ }`
                 // 2. 外部关闭信号: 监听一个关闭通道或信号，允许外部请求 Manager 优雅地关闭。
                //    `_ = shutdown_signal.recv() => { /* 处理关闭逻辑 */ break; }`
             }
        }
        info!("(Manager) 事件循环结束。正在关闭数据通道...");
        // 当 `self` (ManagerActor 实例) 离开 `run` 函数的作用域时，
        // 其拥有的 `completed_data_tx` 和 `failed_data_tx` 会被 Drop。
        // Sender 的 Drop 会自动关闭对应的 MPSC 通道，通知接收端 (消费者) 不会再有新消息。
        // 因此，不需要显式调用 `drop()`。
        // drop(self.completed_data_tx);
        // drop(self.failed_data_tx);
        info!("(Manager) 数据通道已关闭。Manager Actor 任务正常退出。");
    }

    /// 处理单个外部请求的核心逻辑分发函数。
    ///
    /// 根据传入的 `Request` 类型，调用相应的内部处理方法。
    /// 同时，会检查 `is_finalizing` 状态，如果在 Finalizing 状态，则拒绝除 `Finalize` 之外的新请求。
    ///
    /// # Arguments
    /// * `request` - 从 `request_rx` 通道接收到的请求。
    ///
    /// # Returns
    /// * `bool` - 返回 `true` 表示 Manager 应继续运行事件循环，
    ///            返回 `false` 表示 Manager 应停止事件循环 (通常在处理 `Finalize` 请求后)。
    async fn handle_request(&mut self, request: Request) -> bool {
        // --- Finalizing 状态检查 ---
         // 如果 Manager 正在执行 Finalize 操作
        if self.is_finalizing {
            // 记录日志，说明当前状态
            trace!("(Manager) 当前处于 Finalizing 状态，正在处理请求: {:?}", request);
            // 在 Finalizing 状态下，只处理特定情况或拒绝请求
            match request {
                // 收到重复的 Finalize 请求
                Request::Finalize { reply_tx } => {
                    // 这种情况可能发生在外部多次调用 finalize() 或 finalize() 与通道关闭同时发生
                    warn!("(Manager) Finalizing 状态下收到重复的 Finalize 请求，将忽略并回复 None");
                    // 通过 reply_tx 回复 None，告知调用者 Finalize 已在进行或已完成，没有新的报告生成
                    let _ = reply_tx.send(None).map_err(|_e| {
                        error!("(Manager) 回复重复 Finalize 请求失败，接收端可能已放弃等待")
                    });
                }
                // 收到新的预留请求
                Request::Reserve(req) => {
                    warn!("(Manager) Finalizing 状态，拒绝 Reserve 请求");
                    // 回复错误，告知调用者 Manager 正在关闭
                    let _ = req
                        .reply_tx
                        .send(Err(ManagerError::ManagerFinalizing))
                        .map_err(|_e| {
                            error!("(Manager) 回复 Reserve 拒绝失败，Agent 可能已放弃等待")
                        });
                }
                // 收到新的提交数据请求
                Request::SubmitBytes(req) => {
                    warn!(
                        "(Manager) Finalizing 状态，拒绝 SubmitBytes 请求 for Res {}",
                        req.reservation_id
                    );
                    // 回复错误
                    let _ = req
                        .reply_tx
                        .send(Err(ManagerError::ManagerFinalizing))
                        .map_err(|_e| {
                            error!("(Manager) 回复 SubmitBytes 拒绝失败，Agent 可能已放弃等待")
                        });
                }
                // 收到 Agent Drop 时发送的 FailedInfo
                Request::FailedInfo(req) => {
                    // 在 Finalizing 过程中，Agent 的 Drop 可能是预期的，
                    // 或者这些信息可能已经被 FinalizationHandler 处理了。
                    // 通常可以安全地忽略这些信息。
                    warn!(
                        "(Manager) Finalizing 状态，忽略 FailedInfo 请求 for Res {}",
                        req.info.id
                    );
                    // 不需要回复 FailedInfo 请求
                }
            }
            // 即使在 Finalizing 状态下收到请求，事件循环也应该继续运行，
            // 直到 Finalize 逻辑（通常由 finalize_internal 触发）完成。
            // finalize_internal 会在完成后通过其调用者 (Finalize 请求处理或通道关闭处理) 返回 false 来停止循环。
            // 所以这里返回 true，让循环继续。
            return true;
        }

         // --- 正常状态下的请求分发 ---
         // 如果不在 Finalizing 状态，则根据请求类型分发给相应的处理函数
         match request {
            // 处理预留空间请求
            Request::Reserve(req) => {
                // 调用 self 上的 `handle_reserve` 方法 (通常在 operations::request_handlers 中实现)
                // 注意：handle_reserve 设计为同步执行，因为它主要是计算和状态更新，不涉及长时间 IO
                self.handle_reserve(req);
            }
            // 处理提交数据块请求
            Request::SubmitBytes(req) => {
                // 调用 self 上的 `handle_submit_bytes` 方法 (异步)
                // 这个方法可能涉及将数据写入内部缓冲区或与其他异步任务交互
                self.handle_submit_bytes(req).await;
            }
            // 处理 Agent 提交失败或 Drop 时发送的失败信息
            Request::FailedInfo(req) => {
                // 调用 self 上的 `handle_failed_info` 方法 (异步)
                // 需要更新预留的状态，并可能影响分组的处理
                self.handle_failed_info(req).await;
            }
            // 处理 Finalize 请求
            Request::Finalize { reply_tx } => {
                info!("(Manager) 收到明确的 Finalize 请求，开始执行...");
                // 调用内部 Finalize 逻辑
                let finalize_result: Option<FinalizeResult> = self.finalize_internal().await; // finalize_internal 是异步的

                 // 记录 Finalize 报告的生成情况
                if let Some(ref report) = finalize_result {
                    info!(
                        "(Manager) Finalize 执行完成，生成报告（含 {} 个需报告的失败组信息）。",
                        report.failed_len()
                    );
                } else {
                    // 如果 finalize_internal 返回 None，说明 Finalize 已经在进行中
                    info!("(Manager) Finalize 执行完成，但未生成新报告 (Finalize 已在进行)。");
                }

                 // 通过 reply_tx 将 Finalize 结果 (Option<FinalizeResult>) 发送回调用者
                if reply_tx.send(finalize_result).is_err() {
                    // 如果发送失败，通常意味着调用 finalize() 的 Handle 在等待结果时被 Drop 了
                    error!("(Manager) 发送 Finalize 结果给调用者失败 (Handle 可能已 Drop)");
                }
                // Finalize 请求是终止信号，处理完成后，返回 false 以停止事件循环
                return false;
            }
        }
        // 对于 Reserve, SubmitBytes, FailedInfo 请求，处理完成后继续运行事件循环
        true
    }

    /// 执行内部的 Finalize 逻辑。
    ///
    /// 这是触发 Manager 清理和关闭的核心函数，可以由显式的 `Finalize` 请求或请求通道关闭触发。
    /// 它会委托给 `FinalizationHandler` 来处理所有当前活动的分组，
    /// 确保所有数据要么被发送给成功消费者，要么其失败信息被发送给失败消费者。
    ///
    /// # Returns
    /// * `Option<FinalizeResult>` - 如果 Finalize 成功执行，则返回包含失败分组信息的报告。
    ///                              如果 Finalize 已在进行中（重复调用），则返回 `None`。
    async fn finalize_internal(&mut self) -> Option<FinalizeResult> {
        // 防止重复执行 Finalize 逻辑
        if self.is_finalizing {
            warn!("(Manager) Finalize 已在进行中，忽略重复的 finalize_internal 调用");
            return None; // 返回 None 表示没有新的 Finalize 操作发生
        }
        info!("(Manager) 开始执行内部 Finalize 逻辑...");
        // 设置 Finalizing 标记，阻止后续的 Reserve/Submit 请求
        self.is_finalizing = true;

         // 调用注入的 FinalizationHandler 的 finalize_all_groups 方法
        // 需要将 Manager 内部的其他组件作为参数传递给它，以便 FinalizationHandler 可以操作它们。
        // 使用 &mut self.group_lifecycle_manager 等传递可变或不可变引用。
        let result = self
            .finalization_handler // 获取 FinalizationHandler 实例
            .finalize_all_groups( // 调用其核心方法
                &mut self.group_lifecycle_manager, // 传递分组生命周期管理器的可变引用
                &self.group_data_processor,        // 传递数据处理器的不可变引用 (通常 finalize 只读数据)
                &self.completed_data_tx,           // 传递成功数据发送通道
                &self.failed_data_tx,              // 传递失败数据发送通道
            )
            .await; // finalize_all_groups 是异步的

         // is_finalizing 标记保持为 true。事件循环将基于 handle_request 的返回值 (对于 Finalize 请求是 false)
        // 或 finalize_internal 被调用的上下文 (通道关闭后直接 break) 来停止。
        // 如果需要 Manager 在 Finalize 后能重新接受请求（不推荐），可以在这里重置 is_finalizing = false。
        info!(
            "(Manager) 内部 Finalize 调用 FinalizationHandler 完成，生成报告 (含 {} 个失败组)",
            result.failed_len()
        );
        Some(result) // 返回 FinalizationHandler 生成的报告
    }
}

 // --- 在 ManagerActor 中直接实现请求处理逻辑 ---
 // 为了模块化，这些 handle_* 方法通常会定义在 manager::operations 子模块中，
//并通过 ManagerActor 的方法调用它们。这里为了简化，假设它们直接在 impl 块中。
 // 实际代码中，这些方法位于 operations::request_handlers.rs 并通过 `impl<C: ManagerComponents> ManagerActor<C>` 扩展。
 // impl<C: ManagerComponents> ManagerActor<C> {
 //     fn handle_reserve(&mut self, req: ReserveRequest) { ... }
 //     async fn handle_submit_bytes(&mut self, req: SubmitBytesRequest) { ... }
 //     async fn handle_failed_info(&mut self, req: FailedInfoRequest) { ... }
 // }
