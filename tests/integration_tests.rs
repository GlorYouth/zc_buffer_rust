// /tests/integration_tests.rs

// 引入必要的依赖
// Add necessary dependencies
use bytes::Bytes;
// 用于处理字节数据 (For handling byte data)
use std::num::NonZeroUsize;
// 用于非零大小 (For non-zero sizes)
use std::time::Duration;
// 用于模拟延迟 (For simulating delays)
use tracing::info;
// 日志记录 (Logging)

use zc_buffer::{
    BufferError, FailedGroupDataTransmission, Manager, ManagerError, SuccessfulGroupData,
    ZeroCopyHandle,
};

// 辅助函数：初始化 Manager 和消费者通道 (Helper function: Initialize Manager and consumer channels)
async fn setup_manager_and_consumers(
    min_group_commit_size: usize,
) -> (
    ZeroCopyHandle,
    tokio::task::JoinHandle<Vec<SuccessfulGroupData>>, // 成功数据收集器句柄 (Success data collector handle)
    tokio::task::JoinHandle<Vec<FailedGroupDataTransmission>>, // 失败数据收集器句柄 (Failure data collector handle)
) {
    let channel_buffer_size = NonZeroUsize::new(128).unwrap();
    let (handle, mut completed_rx, mut failed_rx) =
        Manager::spawn(channel_buffer_size, min_group_commit_size);

    // 启动成功数据消费者 (Start success data consumer)
    let success_consumer_handle = tokio::spawn(async move {
        let mut received_success_groups = Vec::new();
        info!("(集成测试-成功消费者) 启动 (Integration Test-Success Consumer) Started");
        while let Some(group) = completed_rx.recv().await {
            info!(
                "(集成测试-成功消费者) 收到成功分组: Offset={}, Size={}",
                group.0,       // group.0 is AbsoluteOffset
                group.1.len()  // group.1 is Box<[u8]>
            );
            received_success_groups.push(group);
        }
        info!("(集成测试-成功消费者) 通道关闭，结束 (Integration Test-Success Consumer) Channel closed, ending");
        received_success_groups
    });

    // 启动失败数据消费者 (Start failure data consumer)
    let failure_consumer_handle = tokio::spawn(async move {
        let mut received_failed_groups = Vec::new();
        info!("(集成测试-失败消费者) 启动 (Integration Test-Failure Consumer) Started");
        while let Some(group) = failed_rx.recv().await {
            info!(
                "(集成测试-失败消费者) 收到失败分组: GroupID={}",
                group.group_id
            );
            received_failed_groups.push(group);
        }
        info!("(集成测试-失败消费者) 通道关闭，结束 (Integration Test-Failure Consumer) Channel closed, ending");
        received_failed_groups
    });

    (handle, success_consumer_handle, failure_consumer_handle)
}

// 测试1：基本的预留和单次成功提交 (Test 1: Basic reservation and single successful submission)
#[tokio::test]
async fn test_basic_reserve_and_single_submit_success() {
    // 初始化日志记录器 (Initialize logger)
    // 如果测试中出现 panic: "there is no global default subscriber set"，则取消下一行的注释
    // (If test panics with "there is no global default subscriber set", uncomment the next line)
    // let _ = tracing_subscriber::fmt::try_init();

    let min_group_size = 100; // 设置一个较小的分组大小以便测试 (Set a small group size for testing)
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let size_to_reserve = NonZeroUsize::new(min_group_size).unwrap(); // 预留大小等于分组大小，应立即触发分组 (Reserve size equals group size, should trigger group immediately)

    // 预留 (Reserve)
    let submit_agent = handle
        .reserve_writer(size_to_reserve)
        .await
        .expect("预留应成功 (Reservation should succeed)");
    info!(
        "(测试1) 预留成功: ID={}, Offset={}, Size={}",
        submit_agent.id(),
        submit_agent.offset(),
        submit_agent.size()
    );

    // 转换为 SingleAgent 并提交 (Convert to SingleAgent and submit)
    let single_agent = submit_agent.into_single_agent();
    let data = Bytes::from(vec![1u8; size_to_reserve.get()]);
    single_agent
        .submit_bytes(data)
        .await
        .expect("单次提交应成功 (Single submission should succeed)");
    info!("(测试1) 数据提交成功 (Data submission successful)");

    // Finalize 并等待消费者 (Finalize and wait for consumers)
    let finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");
    assert!(
        finalize_result.is_empty(),
        "Finalize 报告应为空 (Finalize report should be empty)"
    );

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert_eq!(
        success_groups.len(),
        1,
        "应收到一个成功的分组 (Should receive one successful group)"
    );
    assert_eq!(
        success_groups[0].0, // Offset
        0,
        "成功分组的偏移应为0 (Offset of the successful group should be 0)"
    );
    assert_eq!(
        success_groups[0].1.len(), // Size
        size_to_reserve.get(),
        "成功分组的大小应与预留大小一致 (Size of the successful group should match reservation size)"
    );
    assert!(
        failed_groups.is_empty(),
        "不应收到失败的分组 (Should not receive any failed groups)"
    );
    info!("(测试1) 断言通过 (Assertions passed)");
}

// 测试2：预留和分块成功提交 (Test 2: Reservation and chunked successful submission)
#[tokio::test]
async fn test_reserve_and_chunked_submit_success() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 150;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let reserve_size_val = min_group_size;
    let size_to_reserve = NonZeroUsize::new(reserve_size_val).unwrap();

    let submit_agent = handle
        .reserve_writer(size_to_reserve)
        .await
        .expect("预留应成功 (Reservation should succeed)");
    info!(
        "(测试2) 预留成功: ID={}, Offset={}, Size={}",
        submit_agent.id(),
        submit_agent.offset(),
        submit_agent.size()
    );

    let mut chunk_agent = submit_agent.into_chunk_agent();
    let chunk1_size = 50;
    let chunk2_size = reserve_size_val - chunk1_size; // 剩余部分 (Remaining part)

    let data1 = Bytes::from(vec![2u8; chunk1_size]);
    chunk_agent
        .submit_chunk(data1)
        .expect("提交块1应成功 (Submitting chunk 1 should succeed)");
    info!("(测试2) 块1提交到 Agent 成功 (Chunk 1 submitted to Agent successfully)");

    let data2 = Bytes::from(vec![3u8; chunk2_size]);
    chunk_agent
        .submit_chunk(data2)
        .expect("提交块2应成功 (Submitting chunk 2 should succeed)");
    info!("(测试2) 块2提交到 Agent 成功 (Chunk 2 submitted to Agent successfully)");

    chunk_agent
        .commit()
        .await
        .expect("分块提交的 Commit 应成功 (Commit for chunked submission should succeed)");
    info!("(测试2) 分块数据 Commit 成功 (Chunked data committed successfully)");

    let finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");
    assert!(
        finalize_result.is_empty(),
        "Finalize 报告应为空 (Finalize report should be empty)"
    );

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert_eq!(
        success_groups.len(),
        1,
        "应收到一个成功的分组 (Should receive one successful group)"
    );
    assert_eq!(
        success_groups[0].0, 0,
        "成功分组的偏移应为0 (Offset of the successful group should be 0)"
    );
    assert_eq!(
        success_groups[0].1.len(),
        reserve_size_val,
        "成功分组的大小应与预留总大小一致 (Size of the successful group should match total reservation size)"
    );
    assert!(
        failed_groups.is_empty(),
        "不应收到失败的分组 (Should not receive any failed groups)"
    );
    info!("(测试2) 断言通过 (Assertions passed)");
}

// 测试3：SubmitAgent 未消费直接 Drop (Test 3: SubmitAgent dropped without consumption)
#[tokio::test]
async fn test_submit_agent_drop_unconsumed() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let size_to_reserve = NonZeroUsize::new(50).unwrap();
    let reservation_id_for_check: u64;
    {
        let submit_agent = handle
            .reserve_writer(size_to_reserve)
            .await
            .expect("预留应成功 (Reservation should succeed)");
        reservation_id_for_check = submit_agent.id();
        info!(
            "(测试3) 预留成功 (ID={0})，将让 SubmitAgent Drop (Reservation successful (ID={0}), will let SubmitAgent drop)",
            reservation_id_for_check
        );
        // submit_agent 在这里 drop (submit_agent drops here)
    }

    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");
    // Finalize 报告可能包含此失败预留，也可能失败通道已处理
    // (Finalize report might contain this failed reservation, or failure channel might have processed it)

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert!(
        success_groups.is_empty(),
        "不应收到成功的分组 (Should not receive any successful groups)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "应收到一个失败的分组 (Should receive one failed group)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations[0].id, reservation_id_for_check,
        "失败预留的 ID 应匹配 (ID of the failed reservation should match)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations[0].size,
        size_to_reserve.get()
    );
    info!("(测试3) 断言通过 (Assertions passed)");
}

// 测试4：SingleAgent 未提交直接 Drop (Test 4: SingleAgent dropped without commit)
#[tokio::test]
async fn test_single_agent_drop_uncommitted() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let size_to_reserve = NonZeroUsize::new(60).unwrap();
    let reservation_id_for_check: u64;
    {
        let submit_agent = handle
            .reserve_writer(size_to_reserve)
            .await
            .expect("预留应成功 (Reservation should succeed)");
        reservation_id_for_check = submit_agent.id();
        let _single_agent = submit_agent.into_single_agent(); // 转换为 SingleAgent (Convert to SingleAgent)
        info!(
            "(测试4) 预留并转换为 SingleAgent (ID={0})，将让 SingleAgent Drop (Reserved and converted to SingleAgent (ID={0}), will let SingleAgent drop)",
            reservation_id_for_check
        );
        // _single_agent 在这里 drop ( _single_agent drops here)
    }

    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert!(
        success_groups.is_empty(),
        "不应收到成功的分组 (Should not receive any successful groups)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "应收到一个失败的分组 (Should receive one failed group)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations[0].id,
        reservation_id_for_check
    );
    info!("(测试4) 断言通过 (Assertions passed)");
}

// 测试5：ChunkAgent 未 Commit 直接 Drop (Test 5: ChunkAgent dropped without commit)
#[tokio::test]
async fn test_chunk_agent_drop_uncommitted() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let size_to_reserve = NonZeroUsize::new(70).unwrap();
    let reservation_id_for_check: u64;
    {
        let submit_agent = handle
            .reserve_writer(size_to_reserve)
            .await
            .expect("预留应成功 (Reservation should succeed)");
        reservation_id_for_check = submit_agent.id();
        let mut chunk_agent = submit_agent.into_chunk_agent(); // 转换为 ChunkAgent (Convert to ChunkAgent)
        chunk_agent
            .submit_chunk(Bytes::from(vec![5u8; 30]))
            .expect("提交块应成功 (Submitting chunk should succeed)");
        info!(
            "(测试5) 预留并转换为 ChunkAgent (ID={0})，添加了块但未 Commit，将让 ChunkAgent Drop (Reserved and converted to ChunkAgent (ID={0}), added chunk but not committed, will let ChunkAgent drop)",
            reservation_id_for_check
        );
        // chunk_agent 在这里 drop (chunk_agent drops here)
    }

    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert!(
        success_groups.is_empty(),
        "不应收到成功的分组 (Should not receive any successful groups)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "应收到一个失败的分组 (Should receive one failed group)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations[0].id,
        reservation_id_for_check
    );
    info!("(测试5) 断言通过 (Assertions passed)");
}

// 测试6：SingleAgent 提交错误大小 (Test 6: SingleAgent submits incorrect size)
#[tokio::test]
async fn test_single_agent_submit_wrong_size() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let reserve_size = NonZeroUsize::new(50).unwrap();
    let submit_agent = handle
        .reserve_writer(reserve_size)
        .await
        .expect("预留应成功 (Reservation should succeed)");
    let agent_id = submit_agent.id();
    info!(
        "(测试6) 预留成功 (ID={0}) (Reservation successful (ID={0}))",
        agent_id
    );

    let single_agent = submit_agent.into_single_agent();
    let wrong_data = Bytes::from(vec![6u8; 40]); // 预留50，提交40 (Reserved 50, submitting 40)

    let result = single_agent.submit_bytes(wrong_data).await;
    info!(
        "(测试6) 提交错误大小的结果: {0:?} (Result of submitting wrong size)",
        result
    );

    assert!(
        matches!(
            result,
            Err(BufferError::ManagerError(
                ManagerError::SubmitSizeIncorrect { .. }
            ))
        ),
        "提交错误大小应返回 SubmitSizeIncorrect 错误 (Submitting wrong size should return SubmitSizeIncorrect error)"
    );

    // 即使提交失败，Agent 也会 Drop 并发送 FailedInfo，最终由 Finalize 处理
    // (Even if submission fails, Agent drops and sends FailedInfo, eventually handled by Finalize)
    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert!(
        success_groups.is_empty(),
        "不应收到成功的分组 (Should not receive any successful groups)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "应收到一个失败的分组 (Should receive one failed group)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(failed_groups[0].failed_reservations[0].id, agent_id);
    info!("(测试6) 断言通过 (Assertions passed)");
}

// 测试7：ChunkAgent 提交总大小错误 (Test 7: ChunkAgent commits incorrect total size)
#[tokio::test]
async fn test_chunk_agent_commit_wrong_total_size() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100;
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let reserve_size = NonZeroUsize::new(50).unwrap();
    let submit_agent = handle
        .reserve_writer(reserve_size)
        .await
        .expect("预留应成功 (Reservation should succeed)");
    let agent_id = submit_agent.id();
    info!(
        "(测试7) 预留成功 (ID={0}) (Reservation successful (ID={0}))",
        agent_id
    );

    let mut chunk_agent = submit_agent.into_chunk_agent();
    chunk_agent
        .submit_chunk(Bytes::from(vec![7u8; 20]))
        .unwrap(); // 总共提交20，但预留了50 (Total submitted 20, but reserved 50)

    let result = chunk_agent.commit().await;
    info!(
        "(测试7) Commit 错误总大小的结果: {0:?} (Result of committing wrong total size)",
        result
    );

    assert!(
        matches!(
            result,
            Err(BufferError::ManagerError(
                ManagerError::CommitSizeMismatch { .. }
            ))
        ),
        "Commit 错误总大小应返回 CommitSizeMismatch 错误 (Committing wrong total size should return CommitSizeMismatch error)"
    );

    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert!(
        success_groups.is_empty(),
        "不应收到成功的分组 (Should not receive any successful groups)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "应收到一个失败的分组 (Should receive one failed group)"
    );
    assert_eq!(
        failed_groups[0].failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(failed_groups[0].failed_reservations[0].id, agent_id);
    info!("(测试7) 断言通过 (Assertions passed)");
}

// 测试8：多个并发写入者，形成多个分组 (Test 8: Multiple concurrent writers forming multiple groups)
#[tokio::test]
async fn test_multiple_writers_multiple_groups() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 100; // 每个分组大小100 (Each group size 100)
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    let mut writer_tasks = Vec::new();

    // 写入者 1 & 2 形成第一个分组 (Writers 1 & 2 form the first group)
    for i in 0..2 {
        let h_clone = handle.clone();
        let task = tokio::spawn(async move {
            let size = NonZeroUsize::new(50).unwrap(); // 50 * 2 = 100 (Group 1)
            let submit_agent = h_clone
                .reserve_writer(size)
                .await
                .expect("预留应成功 (Reservation should succeed)");
            let agent_id = submit_agent.id();
            info!(
                "(测试8-W{0}) 预留成功 (ID={1}) (Reservation successful (ID={1}))",
                i, agent_id
            );
            let single_agent = submit_agent.into_single_agent();
            let data = Bytes::from(vec![8u8; 50]);
            single_agent
                .submit_bytes(data)
                .await
                .expect("提交应成功 (Submission should succeed)");
            info!(
                "(测试8-W{0}) 提交成功 (ID={1}) (Submission successful (ID={1}))",
                i, agent_id
            );
        });
        writer_tasks.push(task);
    }

    // 写入者 3 & 4 形成第二个分组 (Writers 3 & 4 form the second group)
    for i in 2..4 {
        let h_clone = handle.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await; // 稍微错开 (Slightly offset)
            let size = NonZeroUsize::new(50).unwrap(); // 50 * 2 = 100 (Group 2)
            let submit_agent = h_clone
                .reserve_writer(size)
                .await
                .expect("预留应成功 (Reservation should succeed)");
            let agent_id = submit_agent.id();
            info!(
                "(测试8-W{0}) 预留成功 (ID={1}) (Reservation successful (ID={1}))",
                i, agent_id
            );
            let single_agent = submit_agent.into_single_agent();
            let data = Bytes::from(vec![9u8; 50]);
            single_agent
                .submit_bytes(data)
                .await
                .expect("提交应成功 (Submission should succeed)");
            info!(
                "(测试8-W{0}) 提交成功 (ID={1}) (Submission successful (ID={1}))",
                i, agent_id
            );
        });
        writer_tasks.push(task);
    }

    // 等待所有写入者完成 (Wait for all writers to complete)
    for task in writer_tasks {
        task.await
            .expect("写入者任务不应 panic (Writer task should not panic)");
    }
    info!("(测试8) 所有写入者任务完成 (All writer tasks completed)");

    let finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");
    assert!(
        finalize_result.is_empty(),
        "Finalize 报告应为空 (Finalize report should be empty)"
    );

    let mut success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    assert_eq!(
        success_groups.len(),
        2,
        "应收到两个成功的分组 (Should receive two successful groups)"
    );
    assert!(
        failed_groups.is_empty(),
        "不应收到失败的分组 (Should not receive any failed groups)"
    );

    // 对成功分组排序以确保断言顺序一致 (Sort successful groups to ensure consistent assertion order)
    success_groups.sort_by_key(|g| g.0); // 按偏移排序 (Sort by offset)

    assert_eq!(success_groups[0].0, 0); // 第一个分组的偏移 (Offset of the first group)
    assert_eq!(success_groups[0].1.len(), min_group_size);
    assert_eq!(success_groups[1].0, min_group_size); // 第二个分组的偏移 (Offset of the second group)
    assert_eq!(success_groups[1].1.len(), min_group_size);
    info!("(测试8) 断言通过 (Assertions passed)");
}

// 测试9：Finalize 处理一个有成功提交和有失败（Drop）预留的混合分组
// (Test 9: Finalize processes a mixed group with successful submissions and failed (dropped) reservations)
#[tokio::test]
async fn test_finalize_mixed_group() {
    // let _ = tracing_subscriber::fmt::try_init();
    let min_group_size = 200; // 分组大小 (Group size)
    let (handle, success_consumer, failure_consumer) =
        setup_manager_and_consumers(min_group_size).await;

    // 成功提交部分 (Successfully submitted part)
    let size1 = NonZeroUsize::new(50).unwrap();
    let submit_agent1 = handle
        .reserve_writer(size1)
        .await
        .expect("预留1应成功 (Reservation 1 should succeed)");
    let agent1_id = submit_agent1.id();
    let single_agent1 = submit_agent1.into_single_agent();
    single_agent1
        .submit_bytes(Bytes::from(vec![1u8; 50]))
        .await
        .expect("提交1应成功 (Submission 1 should succeed)");
    info!(
        "(测试9) 预留 {} (大小50) 已成功提交 (Reservation {} (size 50) submitted successfully)",
        agent1_id, agent1_id
    );

    // Drop 的部分 (Dropped part)
    let size2 = NonZeroUsize::new(30).unwrap();
    let reservation2_id_for_check: u64;
    {
        let submit_agent2 = handle
            .reserve_writer(size2)
            .await
            .expect("预留2应成功 (Reservation 2 should succeed)");
        reservation2_id_for_check = submit_agent2.id();
        info!(
            "(测试9) 预留 {} (大小30) 将被 Drop (Reservation {} (size 30) will be dropped)",
            reservation2_id_for_check, reservation2_id_for_check
        );
        // submit_agent2 drops here
    }

    // 另一个成功提交部分，使得总大小超过 min_group_size
    // (Another successfully submitted part, making total size exceed min_group_size)
    // 但由于中间有 Drop，这个分组可能不会在提交时立即密封和处理，而是由 Finalize 处理
    // (But due to the drop in between, this group might not be sealed and processed immediately upon submission, but by Finalize)
    let size3 = NonZeroUsize::new(min_group_size - 50 + 10).unwrap(); // e.g., 160
    let submit_agent3 = handle
        .reserve_writer(size3)
        .await
        .expect("预留3应成功 (Reservation 3 should succeed)");
    let agent3_id = submit_agent3.id();
    let single_agent3 = submit_agent3.into_single_agent();
    single_agent3
        .submit_bytes(Bytes::from(vec![3u8; size3.get()]))
        .await
        .expect("提交3应成功 (Submission 3 should succeed)");
    info!(
        "(测试9) 预留 {} (大小{}) 已成功提交 (Reservation {} (size {}) submitted successfully)",
        agent3_id,
        size3.get(),
        agent3_id,
        size3.get()
    );

    // 调用 Finalize (Call Finalize)
    let _finalize_result = handle
        .finalize()
        .await
        .expect("Finalize 应成功 (Finalize should succeed)");
    // Finalize 报告可能为空，因为失败信息可能已经通过失败通道发出
    // (Finalize report might be empty as failure info might have been sent via failure channel)

    let success_groups = success_consumer
        .await
        .expect("成功消费者任务应结束 (Success consumer task should end)");
    let failed_groups = failure_consumer
        .await
        .expect("失败消费者任务应结束 (Failure consumer task should end)");

    // 预期：
    // - 成功通道不应收到任何数据，因为包含失败预留的分组整体应判定为失败。
    // - 失败通道应收到一个分组，其中包含成功提交的数据块 (来自预留1和3) 和失败预留的信息 (来自预留2)。
    // (Expectation) :
    // (- Success channel should not receive any data, as the group containing failed reservation should be judged as failed overall.)
    // (- Failure channel should receive one group, containing successfully committed data chunks (from res1 & res3) and failed reservation info (from res2).)

    assert!(
        success_groups.is_empty(),
        "成功通道不应收到任何数据 (Success channel should not receive any data)"
    );
    assert_eq!(
        failed_groups.len(),
        1,
        "失败通道应收到一个分组 (Failure channel should receive one group)"
    );

    let failed_group = &failed_groups[0];
    assert_eq!(
        failed_group.failed_reservations.len(),
        1,
        "失败分组应包含一个失败预留信息 (Failed group should contain one failed reservation info)"
    );
    assert_eq!(
        failed_group.failed_reservations[0].id, reservation2_id_for_check,
        "失败预留的 ID 应为预留2的ID (ID of the failed reservation should be that of reservation 2)"
    );
    assert_eq!(failed_group.failed_reservations[0].size, size2.get());

    // 检查失败分组中是否包含成功提交的数据块 (Check if the failed group contains successfully submitted data chunks)
    // 两个成功提交的块：大小50和大小 (min_group_size - 50 + 10)
    // (Two successfully submitted chunks: size 50 and size (min_group_size - 50 + 10))
    let total_committed_bytes_in_failed_group: usize =
        failed_group.group_chunks.values().map(|b| b.len()).sum();
    assert_eq!(
        total_committed_bytes_in_failed_group,
        size1.get() + size3.get(),
        "失败分组中提交的数据块总大小不正确 (Total size of committed data chunks in the failed group is incorrect)"
    );
    info!("(测试9) 断言通过 (Assertions passed)");
}

// 更多测试用例可以包括：(More test cases can include:)
// - Manager 在 Finalizing 状态下拒绝新请求 (Manager rejecting new requests in Finalizing state)
// - 提交到不存在的预留 (Submitting to a non-existent reservation)
// - 提交偏移错误 (Submit offset error)
// - 通道满或关闭时的行为 (Behavior when channels are full or closed - difficult to reliably test in unit/integration)
// - 极小或极大预留值的处理 (Handling of very small or very large reservation values)
