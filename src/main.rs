//! 主程序入口和示例用法演示
//!
//! 这个示例展示了如何使用零拷贝缓冲库 (重写后版本)：
//! 1. 启动 Manager Actor。
//! 2. 启动成功数据和失败数据的消费者任务。
//! 3. 启动多个并发写入者任务，使用 `SubmitAgent` -> `SingleAgent`/`ChunkAgent` 模拟不同场景：
//!    - 成功单次/分块提交并形成分组。
//!    - 预留后不提交 (测试 Agent Drop)。
//!    - 测试 Agent 层的大小/重复提交校验。
//!    - 模拟导致分组失败的场景 (例如包含未提交的预留)。
//! 4. 等待所有写入者完成。
//! 5. 调用 `handle.finalize()` 来处理未完成的预留并获取最终报告。
//! 6. 等待消费者任务结束并验证结果。

use bytes::Bytes;
// 引入 Bytes 用于创建数据
use futures::future::join_all;
// 引入 join_all 来并发等待多个异步任务
use std::{
    num::NonZeroUsize,  // 引入非零 usize
    sync::{Arc, Mutex}, // 用于在验证中安全地收集信息
    time::Duration,     // 引入 Duration 用于模拟延迟
};
use tokio::sync::mpsc;
// 引入 MPSC 通道
use tracing::{error, info, warn, Level};
// 引入 tracing 日志宏和 Level
use tracing_subscriber::FmtSubscriber;
use zc_buffer::{
    BufferError, FailedGroupDataTransmission, ManagerError, SuccessfulGroupData, ZeroCopyHandle,
};
#[tokio::main] // 使用 tokio 作为异步运行时
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- 初始化日志系统 ---
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG) // 设置日志级别
        .with_target(false) // 不显示模块路径
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("设置全局日志 subscriber 失败");

    info!("========================================================");
    info!("启动零拷贝缓冲管理器示例 (重写后版本)");
    info!("演示特性：并发写入 (Agent)、自动分组合并、失败分组处理、Finalize、Agent 层校验");
    info!("========================================================");

    // --- 配置参数 ---
    let channel_buffer_size = NonZeroUsize::new(128).unwrap(); // 通道缓冲区大小
    let min_group_commit_size = 600; // 分组密封阈值 (字节)
    info!(
        "配置: 通道缓冲区大小={}, 最小分组提交大小={}",
        channel_buffer_size.get(),
        min_group_commit_size
    );

    // --- 启动 Manager Actor ---
    info!("正在启动 Manager Actor...");
    let (handle, mut completed_rx, mut failed_rx): (
        ZeroCopyHandle,
        mpsc::Receiver<SuccessfulGroupData>,
        mpsc::Receiver<FailedGroupDataTransmission>,
    ) = zc_buffer::spawn_manager!(default, channel_buffer_size, &min_group_commit_size);

    info!("Manager Actor 已启动。");

    // --- 启动成功数据消费者任务 ---
    let success_consumer_task = tokio::spawn(async move {
        info!("(成功消费者) 任务启动，等待合并后的数据分组...");
        let mut total_bytes_received = 0;
        let mut group_count = 0;
        let mut received_groups = Vec::new(); // 收集成功分组信息 (偏移, 大小)

        while let Some((start_offset, data_box)) = completed_rx.recv().await {
            group_count += 1;
            let data_len = data_box.len();
            total_bytes_received += data_len;
            info!("-------------------- 成功消费者收到数据 --------------------");
            info!(
                "(成功消费者) 收到第 {} 个成功合并的分组: Offset={}, Size={}",
                group_count, start_offset, data_len
            );
            // 可以在这里处理 data_box 中的数据
            // trace!("(成功消费者) 数据内容预览 (前 10 字节): {:?}", &data_box[..std::cmp::min(10, data_len)]);
            info!("----------------------------------------------------------");
            received_groups.push((start_offset, data_len)); // 记录信息用于验证
        }
        info!(
            "(成功消费者) 数据通道关闭，任务结束。共收到 {} 个成功分组，总计 {} 字节。",
            group_count, total_bytes_received
        );
        (total_bytes_received, received_groups) // 返回收集到的信息
    });

    // --- 启动失败数据消费者任务 ---
    let failure_consumer_task = tokio::spawn(async move {
        info!("(失败消费者) 任务启动，等待失败分组的数据...");
        let mut failed_group_count = 0;
        let mut collected_failed_groups = Vec::new(); // 收集失败分组数据

        while let Some(failed_group_data) = failed_rx.recv().await {
            failed_group_count += 1;
            let group_id = failed_group_data.group_id;
            let num_chunks = failed_group_data.group_chunks.len();
            let total_chunk_bytes: usize = failed_group_data
                .group_chunks
                .values()
                .map(|b| b.len())
                .sum();
            let num_failed_reservations = failed_group_data.failed_reservations.len();

            warn!("!!!!!!!!!!!!!!!!!!!! 失败消费者收到分组数据 !!!!!!!!!!!!!!!!!!!!");
            warn!(
                "(失败消费者) 收到第 {} 个失败分组数据: Group ID={}",
                failed_group_count, group_id
            );
            warn!(
                "  - 包含 {} 个数据块, 总计 {} 字节",
                num_chunks, total_chunk_bytes
            );
            warn!(
                "  - 关联 {} 个失败/未完成预留信息:",
                num_failed_reservations
            );
            for res_info in &failed_group_data.failed_reservations {
                warn!(
                    "    - Res ID: {}, Offset: {}, Size: {}",
                    res_info.id, res_info.offset, res_info.size
                );
            }
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            // 可以在这里处理失败数据，例如记录到日志或持久化存储
            collected_failed_groups.push(failed_group_data); // 收集信息用于验证
        }
        info!(
            "(失败消费者) 失败数据通道关闭，任务结束。共收到 {} 个失败分组的数据。",
            failed_group_count
        );
        collected_failed_groups // 返回收集到的信息
    });

    // --- 用于收集写入者结果的共享状态 (用于验证) ---
    // 存储每个写入者任务的结果字符串 (例如 "W1 OK ResID=0", "W6 DROP ResID=5")
    let writer_results = Arc::new(Mutex::new(Vec::<String>::new()));

    // --- 启动多个写入者任务 ---
    let mut writer_tasks = Vec::new();

    // --- 写入者 1, 2, 3: 预期形成第一个成功的分组 (使用 SingleAgent) ---
    // Writer 1
    let h1 = handle.clone();
    let wr1 = writer_results.clone();
    async move {
        let writer_id = 1;
        let size_val = 200;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (SingleAgent)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h1.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id(); // 获取 ID
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                let data = Bytes::from(vec![1u8; size_val]);
                info!("(Writer {}) 准备提交 {} 字节", writer_id, data.len());
                // 使用 into_single_agent 消耗 submit_agent
                match submit_agent.into_single_agent().submit_bytes(data).await {
                    Ok(_) => {
                        info!("(Writer {}) SingleAgent 提交成功", writer_id);
                        format!("W{} OK {}", writer_id, res_id)
                    }
                    Err(e) => {
                        error!("(Writer {}) SingleAgent 提交失败: {:?}", writer_id, e);
                        format!("W{} ERR_SUBMIT {} {:?}", writer_id, res_id, e)
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr1.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }
    .await;

    // Writer 2
    let h2 = handle.clone();
    let wr2 = writer_results.clone();
    async move {
        let writer_id = 2;
        let size_val = 300;
        tokio::time::sleep(Duration::from_millis(10)).await; // 模拟延迟
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (SingleAgent)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h2.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                let data = Bytes::from(vec![2u8; size_val]);
                info!("(Writer {}) 准备提交 {} 字节", writer_id, data.len());
                match submit_agent.into_single_agent().submit_bytes(data).await {
                    Ok(_) => {
                        info!("(Writer {}) SingleAgent 提交成功", writer_id);
                        format!("W{} OK {}", writer_id, res_id)
                    }
                    Err(e) => {
                        error!("(Writer {}) SingleAgent 提交失败: {:?}", writer_id, e);
                        format!("W{} ERR_SUBMIT {} {:?}", writer_id, res_id, e)
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr2.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }
    .await;

    // Writer 3
    let h3 = handle.clone();
    let wr3 = writer_results.clone();
    async move {
        let writer_id = 3;
        let size_val = 500; // W1(200) + W2(300) + W3(500) = 1000 >= 600，触发第一个分组提交
        tokio::time::sleep(Duration::from_millis(20)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (SingleAgent)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h3.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                let data = Bytes::from(vec![3u8; size_val]);
                info!("(Writer {}) 准备提交 {} 字节", writer_id, data.len());
                match submit_agent.into_single_agent().submit_bytes(data).await {
                    Ok(_) => {
                        info!("(Writer {}) SingleAgent 提交成功", writer_id);
                        format!("W{} OK {}", writer_id, res_id)
                    }
                    Err(e) => {
                        error!("(Writer {}) SingleAgent 提交失败: {:?}", writer_id, e);
                        format!("W{} ERR_SUBMIT {} {:?}", writer_id, res_id, e)
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr3.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }
    .await;

    // --- 写入者 4 & 9: 预期形成第二个成功的分组 (Writer 4 Single, Writer 9 Chunk) ---
    // Writer 4
    let h4 = handle.clone();
    let wr4 = writer_results.clone();
    async move {
        let writer_id = 4;
        let size_val = 400;
        tokio::time::sleep(Duration::from_millis(40)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (SingleAgent)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h4.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                let data = Bytes::from(vec![4u8; size_val]);
                info!("(Writer {}) 准备提交 {} 字节", writer_id, data.len());
                match submit_agent.into_single_agent().submit_bytes(data).await {
                    Ok(_) => {
                        info!("(Writer {}) SingleAgent 提交成功", writer_id);
                        format!("W{} OK {}", writer_id, res_id)
                    }
                    Err(e) => {
                        error!("(Writer {}) SingleAgent 提交失败: {:?}", writer_id, e);
                        format!("W{} ERR_SUBMIT {} {:?}", writer_id, res_id, e)
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr4.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }
    .await;

    // Writer 9: 使用 ChunkAgent
    let h9 = handle.clone();
    let wr9 = writer_results.clone();
    async move {
        let writer_id = 9;
        let size_val = 250; // W4(400) + W9(250) = 650 >= 600，触发第二个分组提交
        let chunk_size1 = 100;
        let chunk_size2 = 150;
        tokio::time::sleep(Duration::from_millis(50)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (ChunkAgent)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h9.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                // 使用 into_chunk_agent 转换
                let mut chunk_agent = submit_agent.into_chunk_agent();

                // 添加块 1
                let data1 = Bytes::from(vec![9u8; chunk_size1]);
                info!(
                    "(Writer {}) 准备添加第 1 块 ({} 字节)",
                    writer_id,
                    data1.len()
                );
                if let Err(e) = chunk_agent.submit_chunk(data1) {
                    // 同步添加块
                    error!(
                        "(Writer {}) ChunkAgent 添加第 1 块失败 (同步): {:?}",
                        writer_id, e
                    );
                    // 添加失败，ChunkAgent 会在 Drop 时发送 FailedInfo
                    format!("W{} ERR_CHUNK1_SYNC {} {:?}", writer_id, res_id, e)
                } else {
                    info!("(Writer {}) ChunkAgent 添加第 1 块成功 (同步)", writer_id);

                    tokio::time::sleep(Duration::from_millis(15)).await; // 模拟处理

                    // 添加块 2
                    let data2 = Bytes::from(vec![10u8; chunk_size2]);
                    info!(
                        "(Writer {}) 准备添加第 2 块 ({} 字节)",
                        writer_id,
                        data2.len()
                    );
                    if let Err(e) = chunk_agent.submit_chunk(data2) {
                        error!(
                            "(Writer {}) ChunkAgent 添加第 2 块失败 (同步): {:?}",
                            writer_id, e
                        );
                        format!("W{} ERR_CHUNK2_SYNC {} {:?}", writer_id, res_id, e)
                    } else {
                        info!("(Writer {}) ChunkAgent 添加第 2 块成功 (同步)", writer_id);

                        // 所有块添加完毕，执行最终 Commit (异步)
                        info!(
                            "(Writer {}) 所有块加入 Agent，准备异步 Commit Res {}",
                            writer_id,
                            chunk_agent.id()
                        );
                        match chunk_agent.commit().await {
                            // commit 会消耗 chunk_agent
                            Ok(_) => {
                                info!("(Writer {}) ChunkAgent Commit 成功", writer_id);
                                format!("W{} OK {}", writer_id, res_id)
                            }
                            Err(e) => {
                                error!("(Writer {}) ChunkAgent Commit 失败: {:?}", writer_id, e);
                                // Commit 失败，此时 chunk_agent 已经被消耗，其 Drop 不会再执行
                                // 但 Manager 可能已经收到了部分数据（取决于错误类型）
                                format!("W{} ERR_COMMIT {} {:?}", writer_id, res_id, e)
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr9.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }
    .await;

    // --- 写入者 6: 故意不提交数据 (测试 Agent Drop) ---
    let h6 = handle.clone();
    let wr6 = writer_results.clone();
    writer_tasks.push(tokio::spawn(async move {
        let writer_id = 6;
        let size_val = 100; // 这个预留会进入第 3 个分组，但不会提交
        tokio::time::sleep(Duration::from_millis(60)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (将不提交，测试 Drop)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h6.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={} (将不提交)",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                warn!(
                    "(Writer {}) 故意不使用 SubmitAgent，让其在作用域结束时 Drop",
                    writer_id
                );
                // submit_agent 在这里超出作用域并 Drop，触发 FailedInfo
                format!("W{} DROP {}", writer_id, res_id)
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr6.lock().unwrap().push(result_str);
        info!("(Writer {}) 任务完成 (SubmitAgent 已 Drop)", writer_id);
    }));

    // --- 写入者 8: 测试 Agent 层提交大小校验失败 (SingleAgent) ---
    let h8 = handle.clone();
    let wr8 = writer_results.clone();
    writer_tasks.push(tokio::spawn(async move {
        let writer_id = 8;
        let size_val = 50; // 这个预留也进入第 3 个分组，尝试提交错误大小
        tokio::time::sleep(Duration::from_millis(80)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (将用 SingleAgent 提交错误大小)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        let result_str = match h8.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );
                let single_agent = submit_agent.into_single_agent();
                // 准备错误大小的数据
                let wrong_size = size_val + 10;
                let data = Bytes::from(vec![8u8; wrong_size]);
                info!(
                    "(Writer {}) 准备提交错误大小的数据 ({} 字节) (预期 {})",
                    writer_id,
                    data.len(),
                    single_agent.size()
                );
                match single_agent.submit_bytes(data).await {
                    Ok(_) => {
                        warn!(
                            "(Writer {}) SingleAgent 提交错误大小成功 - 这不符合预期!",
                            writer_id
                        );
                        format!("W{} UNEXPECTED_OK {}", writer_id, res_id)
                    }
                    Err(e) => {
                        info!(
                            "(Writer {}) SingleAgent 提交错误大小失败 (符合预期): {:?}",
                            writer_id, e
                        );
                        // 检查是否是期望的 SubmitSizeIncorrect 错误
                        if matches!(
                            e,
                            BufferError::ManagerError(ManagerError::SubmitSizeIncorrect { .. })
                        ) {
                            info!(
                                "(Writer {}) 确认收到预期的 SubmitSizeIncorrect 错误",
                                writer_id
                            );
                            format!("W{} ERR_SIZE_MISMATCH {}", writer_id, res_id)
                        } else {
                            warn!(
                                "(Writer {}) 收到的错误类型不是预期的 SubmitSizeIncorrect: {:?}",
                                writer_id, e
                            );
                            format!("W{} ERR_UNEXPECTED_TYPE {} {:?}", writer_id, res_id, e)
                        }
                        // 提交失败，single_agent 会在 Drop 时发送 FailedInfo
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                format!("W{} ERR_RESERVE {:?}", writer_id, e)
            }
        };
        wr8.lock().unwrap().push(result_str);
        info!(
            "(Writer {}) 任务完成 (提交因校验失败, SingleAgent 已 Drop)",
            writer_id
        );
    }));

    // --- 写入者 10: 失败后再次提交 (SingleAgent) ---
    let h10 = handle.clone();
    let wr10 = writer_results.clone();
    writer_tasks.push(tokio::spawn(async move {
        let writer_id = 10;
        let size_val = 30; // 进入第 3 分组
        tokio::time::sleep(Duration::from_millis(100)).await;
        info!(
            "(Writer {}) 启动，请求预留 {} 字节 (测试重复提交)",
            writer_id, size_val
        );
        let size = NonZeroUsize::new(size_val).unwrap();
        #[allow(unused_assignments)]
        let mut final_result_str = format!("W{} INIT", writer_id); // 初始结果

        match h10.reserve_writer(size).await {
            Ok(submit_agent) => {
                let res_id = submit_agent.id();
                info!(
                    "(Writer {}) 预留成功: ID={}, Offset={}, Size={}",
                    writer_id,
                    submit_agent.id(),
                    submit_agent.offset(),
                    submit_agent.size()
                );

                let single_agent = submit_agent.into_single_agent();
                let data = Bytes::from(vec![11u8; size_val]);

                info!("(Writer {}) 准备提交 {} 字节", writer_id, data.len());
                match single_agent.submit_bytes(data.clone()).await {
                    // 模拟第一次提交成功
                    Ok(_) => {
                        info!("(Writer {}) 提交成功", writer_id);
                        final_result_str = format!("W{} OK_FIRST {}", writer_id, res_id);
                    }
                    Err(e) => {
                        error!("(Writer {}) 提交失败: {:?}", writer_id, e);
                        final_result_str =
                            format!("W{} ERR_SUBMIT_FIRST {} {:?}", writer_id, res_id, e);
                        // 提交失败，single_agent 会在 Drop 时发送 FailedInfo
                    }
                }
            }
            Err(e) => {
                error!("(Writer {}) 预留失败: {:?}", writer_id, e);
                final_result_str = format!("W{} ERR_RESERVE {:?}", writer_id, e);
            }
        }
        wr10.lock().unwrap().push(final_result_str);
        info!("(Writer {}) 任务完成", writer_id);
    }));

    // --- 等待所有写入者任务完成 ---
    info!("========================================================");
    info!(
        "启动了 {} 个写入者任务，现在等待它们全部完成...",
        writer_tasks.len()
    );
    join_all(writer_tasks).await; // 等待 Vec 中的所有任务结束
    info!("所有写入者任务已结束。");

    // --- 打印写入者结果概览 ---
    info!("--- 写入者结果概览 ---");
    let final_results = writer_results.lock().unwrap();
    for result in final_results.iter() {
        info!("  - {}", result);
    }
    info!("-----------------------");
    info!("========================================================");

    // --- 执行 Finalize 操作 ---
    info!("准备调用 handle.finalize()... 这将触发未完成预留和分组的处理。");
    // finalize 现在消耗 handle
    let finalize_report_opt = match handle.finalize().await {
        Ok(report) => Some(report), // finalize 现在直接返回 FinalizeResult
        Err(e) => {
            error!("Finalize 调用本身失败: {:?}", e);
            None // 返回 None
        }
    };
    // 如果需要报告内容进行验证，从 Option 中取出
    let finalize_report = finalize_report_opt.unwrap_or_default(); // 使用 Default::default() 创建空报告

    // --- 等待消费者任务处理完所有数据并结束 ---
    info!("========================================================");
    info!("等待消费者任务处理完所有数据并结束...");

    // 等待成功消费者
    let success_consumer_result = success_consumer_task.await;
    // 等待失败消费者
    let failure_consumer_result = failure_consumer_task.await;

    info!("==================== 最终验证 ====================");

    // --- 验证成功消费者 ---
    match success_consumer_result {
        Ok((total_bytes_success, received_groups_success)) => {
            info!(
                "(验证) 成功消费者任务成功结束，共处理 {} 字节，收到 {} 个分组。",
                total_bytes_success,
                received_groups_success.len()
            );
            // 预期成功分组:
            // 分组1: W1(200) + W2(300) + W3(500) = 1000 (成功) Offset: 0
            // 分组2: W4(400) + W9(250) = 650 (成功) Offset: 1000
            // 分组3: W6(100, Drop), W8(50, Fail Size), W10(30, OK First) -> 失败
            let expected_total_bytes = 1000 + 650;
            let expected_groups = vec![(0, 1000), (1000, 650)]; // (Offset, Size)

            assert_eq!(
                total_bytes_success, expected_total_bytes,
                "成功消费者接收的总字节数与预期不符"
            );
            assert_eq!(
                received_groups_success.len(),
                expected_groups.len(),
                "成功消费者接收的分组数量与预期不符"
            );

            // 比较每个成功分组的偏移和大小
            for (i, (received_offset, received_size)) in received_groups_success.iter().enumerate()
            {
                if i < expected_groups.len() {
                    let (expected_offset, expected_size) = expected_groups[i];
                    assert_eq!(
                        *received_offset,
                        expected_offset,
                        "第 {} 个成功分组的偏移不符 (预期 {}, 实际 {})",
                        i + 1,
                        expected_offset,
                        *received_offset
                    );
                    assert_eq!(
                        *received_size,
                        expected_size,
                        "第 {} 个成功分组的大小不符 (预期 {}, 实际 {})",
                        i + 1,
                        expected_size,
                        *received_size
                    );
                }
            }

            info!(
                "(验证) 成功消费者接收的总字节数 {} 和分组信息 {:?} 与预期一致。",
                total_bytes_success, received_groups_success
            );
        }
        Err(e) => {
            error!("(验证) 成功消费者任务执行时发生错误: {:?}", e);
            panic!("成功消费者任务失败"); // 在测试中可以 panic
        }
    }

    // --- 验证失败消费者和 Finalize 报告 ---
    match failure_consumer_result {
        Ok(failed_groups_received) => {
            info!(
                "(验证) 失败消费者任务成功结束，共收到 {} 个失败分组的数据。",
                failed_groups_received.len()
            );

            // 预期失败分组:
            // 分组3: 包含 W6(Drop), W8(Fail), W10(OK)
            // 当 finalize 处理时，W6 和 W8 会被标记为失败。
            // W10 的数据已提交。
            // 因此预期失败通道收到 1 个分组的数据。
            assert_eq!(
                failed_groups_received.len(),
                1,
                "失败消费者应收到 1 个失败分组的数据"
            );

            if let Some(failed_group) = failed_groups_received.first() {
                info!(
                    "(验证) 失败消费者收到 Group ID: {} 的数据",
                    failed_group.group_id
                );

                // 检查分组中已提交的数据块 (应该只包含 W10 成功提交的 30 字节)
                let total_chunk_bytes: usize =
                    failed_group.group_chunks.values().map(|b| b.len()).sum();
                let expected_failed_bytes = 30; // 只有 W10 的数据
                assert_eq!(
                    total_chunk_bytes, expected_failed_bytes,
                    "失败分组的总块字节数应为 {} (来自 W10)",
                    expected_failed_bytes
                );
                info!(
                    "(验证) 失败分组的总块字节数 {} 与预期 {} 一致。",
                    total_chunk_bytes, expected_failed_bytes
                );

                // 检查失败预留信息 (Agent Drop 或提交失败会触发 Manager 添加 FailedInfo)
                // W6 (Drop) 和 W8 (提交大小错误导致 Drop) 应该会触发 FailedInfo
                // W10 第一次提交成功，第二次尝试失败，但 Agent 不会再 Drop 发送 FailedInfo
                // 所以预期 failed_reservations 包含 W6 和 W8 的信息
                let expected_failed_res_count = 2;
                assert_eq!(
                    failed_group.failed_reservations.len(),
                    expected_failed_res_count,
                    "失败分组应包含 {} 个失败预留的信息 (W6, W8)",
                    expected_failed_res_count
                );
                info!(
                    "(验证) 失败分组包含 {} 个预留信息，符合预期。",
                    failed_group.failed_reservations.len()
                );

                // 检查具体失败信息的大小
                let w6_info_found = failed_group
                    .failed_reservations
                    .iter()
                    .any(|info| info.size == 100); // W6 size
                let w8_info_found = failed_group
                    .failed_reservations
                    .iter()
                    .any(|info| info.size == 50); // W8 size
                assert!(w6_info_found, "失败分组信息应包含 Writer 6 (size 100)");
                assert!(w8_info_found, "失败分组信息应包含 Writer 8 (size 50)");
                info!("(验证) 失败分组关联的预留信息与预期 (Writer 6, 8) 一致。");
            } else {
                // 如果预期是 1 但列表为空
                panic!("验证失败：期望收到一个失败分组，但列表为空");
            }

            // 验证 Finalize 报告
            // 因为我们假设失败分组数据已成功发送到失败通道 (通道未关闭/阻塞)，
            // 所以 Finalize 报告应该为空。
            assert!(
                finalize_report.is_empty(),
                "Finalize 报告应为空 (预期 {} 个失败组)",
                finalize_report.failed_len()
            );
            info!("(验证) Finalize 报告为空，符合预期。");
        }
        Err(e) => {
            error!("(验证) 失败消费者任务执行时发生错误: {:?}", e);
            panic!("失败消费者任务失败");
        }
    }

    info!("========================================================");
    info!("示例程序正常结束。");
    Ok(())
}
