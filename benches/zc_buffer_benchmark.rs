// 引入 Criterion 相关的宏和结构体 (Import Criterion related macros and structs)
use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId, BatchSize};
// 引入标准库中的非零 usize 和 Duration (Import NonZeroUsize and Duration from the standard library)
use std::num::NonZeroUsize;
use std::time::Duration;
// 引入 Tokio 运行时，用于在同步的 Criterion 测试中运行异步代码 (Import Tokio runtime for running async code in sync Criterion tests)
use tokio::runtime::Runtime;
// 引入 Bytes 类型，用于高效处理二进制数据 (Import Bytes type for efficient binary data handling)
use bytes::Bytes;

// 引入您的代码库中的必要组件 (Import the necessary components from your codebase)
// 假设您的库名为 zc_buffer (根据您的实际库名修改) (Assuming your library name is zc_buffer, modify according to your actual library name)
use zc_buffer::{Manager, ZeroCopyHandle}; // 引入 Manager 和 ZeroCopyHandle (Import Manager and ZeroCopyHandle)

// 定义常量 (Define constants)
// 定义分组密封的最小字节数阈值，例如 130MB (Define the minimum byte threshold for group sealing, e.g., 130MB)
const MIN_GROUP_COMMIT_SIZE_BYTES: usize = 130 * 1024 * 1024; // 128 MB 分组大小 (130 MB group size)
// 定义每次提交操作的数据大小，例如 10MB (Define the data size for each submission operation, e.g., 10MB)
const SUBMISSION_SIZE_BYTES: usize = 10 * 1024 * 1024;       // 每次提交 10 MB (Submit 10 MB each time)
// 计算形成一个 128MB 的分组大约需要多少次 10MB 的提交 (向上取整)
// (Calculate how many 10MB submissions are approximately needed to form a 130MB group (round up))
const SUBMISSIONS_PER_GROUP: usize = (MIN_GROUP_COMMIT_SIZE_BYTES + SUBMISSION_SIZE_BYTES - 1) / SUBMISSION_SIZE_BYTES;
// 为了让基准测试运行一段时间并能观察到几个分组的形成，我们定义每次迭代中执行的提交次数
// 例如，提交足够形成大约2个完整分组的数据量
// (To let the benchmark run for a period and observe the formation of several groups, define the number of submissions per iteration)
// (For example, submit enough data to form approximately 2 full groups)
const NUM_SUBMISSIONS_PER_ITERATION: usize = SUBMISSIONS_PER_GROUP * 2;


// 基准测试函数 (Benchmark function)
fn high_throughput_single_submit_benchmark(c: &mut Criterion) {
    // 创建 Tokio 运行时，因为 Manager 和 Handle 是异步的 (Create Tokio runtime because Manager and Handle are asynchronous)
    let rt = Runtime::new().expect("创建 Tokio 运行时失败 (Failed to create Tokio runtime)");

    // 创建一个名为 "zc_buffer_throughput" 的基准测试组 (Create a benchmark group named "zc_buffer_throughput")
    let mut group = c.benchmark_group("zc_buffer_throughput");

    // 设置吞吐量，以便 Criterion 可以报告每秒处理的字节数 (Set throughput so Criterion can report bytes processed per second)
    // 我们计算的是一次迭代 (即一次 routine 调用) 中提交的总数据量
    // (We calculate the total data submitted in one iteration (i.e., one routine call))
    let total_bytes_per_iteration = (NUM_SUBMISSIONS_PER_ITERATION * SUBMISSION_SIZE_BYTES) as u64;
    group.throughput(Throughput::Bytes(total_bytes_per_iteration));

    // 设置采样参数 (Set sampling parameters)
    group.sample_size(10); // 减少采样次数以便快速看到结果，实际测试中可以增加 (Reduce sample count for quick results, can be increased in actual tests)
    group.measurement_time(Duration::from_secs(10)); // 增加单次测量时间以获得更稳定的结果 (Increase single measurement time for more stable results)

    // 定义一个基准测试 (Define a benchmark test)
    // 使用 BenchmarkId 来区分不同的输入参数（如果将来有的话）(Use BenchmarkId to distinguish different input parameters if any in the future)
    group.bench_function(
        BenchmarkId::new("single_submit_10mb_to_130mb_group", total_bytes_per_iteration),
        |b| {
            // b.to_async(&rt).iter_batched() 用于对异步代码进行基准测试，它包含 setup, routine 和 BatchSize
            // (b.to_async(&rt).iter_batched() is used for benchmarking asynchronous code, including setup, routine, and BatchSize)
            // setup: 每次测量前运行，用于准备环境 (setup: Runs before each measurement to prepare the environment)
            // routine: 实际要进行基准测试的代码 (routine: The actual code to be benchmarked)
            // BatchSize: 控制 setup 和 routine 的调用方式 (BatchSize: Controls how setup and routine are called)
            b.to_async(&rt).iter_batched(
                /* setup */
                || {
                    // 在 Tokio 运行时上下文中初始化 Manager (Initialize Manager within Tokio runtime context)
                    let (handle, mut completed_rx, mut failed_rx) = Manager::spawn(
                        NonZeroUsize::new(1024).unwrap(), // 通道缓冲区大小 (Channel buffer size)
                        MIN_GROUP_COMMIT_SIZE_BYTES,      // 分组密封阈值 (Group sealing threshold)
                    );

                    // 启动简单的消费者任务以防止通道阻塞 (Start simple consumer tasks to prevent channel blocking)
                    // 这些任务会在后台运行，基准测试主要关注提交性能
                    // (These tasks run in the background; the benchmark mainly focuses on submission performance)
                    tokio::spawn(async move {
                        while let Some(_) = completed_rx.recv().await {
                            // 仅消费，不作处理 (Just consume, no processing)
                        }
                    });
                    tokio::spawn(async move {
                        while let Some(_) = failed_rx.recv().await {
                            // 仅消费，不作处理 (Just consume, no processing)
                        }
                    });

                    handle // 返回 Handle 给 routine (Return Handle to the routine)
                },
                /* routine */
                |handle: ZeroCopyHandle| async move {
                    // 准备要提交的数据 (一次性创建，重复使用) (Prepare data for submission (create once, reuse))
                    let data_to_submit = Bytes::from(vec![0u8; SUBMISSION_SIZE_BYTES]);
                    let submission_size_nz = NonZeroUsize::new(SUBMISSION_SIZE_BYTES).unwrap();

                    for _i in 0..NUM_SUBMISSIONS_PER_ITERATION {
                        match handle.reserve_writer(submission_size_nz).await {
                            Ok(submit_agent) => {
                                // 转换为 SingleAgent 并提交数据 (Convert to SingleAgent and submit data)
                                if let Err(_e) = submit_agent
                                    .into_single_agent()
                                    .submit_bytes(data_to_submit.clone()) // 克隆 Bytes 以便在循环中重复使用 (Clone Bytes for reuse in the loop)
                                    .await
                                {
                                    // 在基准测试中，我们通常期望操作成功 (In benchmarks, we usually expect operations to succeed)
                                    // 如果发生错误，可以考虑打印或 panic，但这可能会影响基准测试的准确性
                                    // (If an error occurs, consider printing or panicking, but this might affect benchmark accuracy)
                                    // eprintln!("提交失败 (Submit failed): {:?}", _e);
                                }
                            }
                            Err(_e) => {
                                // eprintln!("预留失败 (Reserve failed): {:?}", _e);
                            }
                        }
                    }
                },
                /* BatchSize */
                // NumIterations(1) 表示 setup 返回的值会被传递给 routine 一次。
                // routine 内部已经包含了 NUM_SUBMISSIONS_PER_ITERATION 次提交，形成一个完整的 "操作单元"。
                // (NumIterations(1) means the value returned by setup is passed to routine once.)
                // (The routine itself already contains NUM_SUBMISSIONS_PER_ITERATION submissions, forming a complete "operation unit".)
                BatchSize::NumIterations(1)
            );
        },
    );

    group.finish(); // 结束基准测试组 (Finish the benchmark group)
}

// 注册基准测试组和主函数 (Register benchmark group and main function)
criterion_group!(benches, high_throughput_single_submit_benchmark);
criterion_main!(benches);