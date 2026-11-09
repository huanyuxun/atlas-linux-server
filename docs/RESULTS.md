# 实际验证报告

验证日期：2026-10-30。以下数字来自 `results/` 原始文件，未使用估算或虚构性能数据。

## 环境与方法

- 用户已有的 VMware Ubuntu 24.04 虚拟机，Linux 6.8.0-146-generic，x86_64。
- 2 vCPU，宿主处理器标识 Intel Core i7-14700HX，虚拟机内存约 3.8 GiB。
- GCC 13.3.0、Boost 1.83、CMake 3.28.3、Python 3.12.3；Release 使用 CMake 的 `-O3 -DNDEBUG`。
- 4 个 I/O 线程、2 个计算线程。压力测试帧上限 8192 字节、计算任务上限 4096、连接上限 8192、受管内存预算 256 MiB；完整配置在 [benchmark.conf](../results/performance/benchmark.conf)。
- 压测器和服务运行在同一 VM，通过 loopback 通信。每行测量 10 秒，连接建立和初始数据写入不计入，最后一批请求排空计入。
- 成功 QPS 仅统计状态成功且 ID/类型/GET 内容校验通过的响应；每组所有连接均完整结束，无错误响应或未匹配响应。
- CPU 100% 代表一个 CPU 核，RSS 每 200ms 采样；峰值是采样峰值。P95/P99 为固定对数直方图上界，相对桶宽不超过 2%。
- 这是 Python 客户端与服务共同消耗两核 CPU 的端到端结果，不能解释为服务器极限容量。连接数增加、流水线深度改变都会影响延迟及吞吐，不同工作负载之间不能只按 QPS 排序。

## 自动化测试

| 验证 | 实际结果 | 原始记录 |
| --- | --- | --- |
| Release 编译 | 成功，无项目编译警告 | [build-release.log](../results/build-release.log) |
| C++ 单元测试 | 通过：字节序/二进制、预算、版本 CAS、8000 次并发 ADD、整数溢出、取消、存储上限及任务准入 | [ctest-final-release.log](../results/ctest-final-release.log) |
| 黑盒集成测试（含持续运行） | **26/26 通过**，总耗时 122.871 秒 | [integration.log](../results/integration.log) |
| ASan + UBSan | 单元测试及常规集成套件通过，启用 leak detection 和 halt_on_error；持续运行项未在 sanitizer 下重复 | [ctest-final-asan.log](../results/ctest-final-asan.log) |
| TSan | 已编译并运行，但当前工具链无法给出可靠通过结论，见下文 | [ctest-tsan.log](../results/ctest-tsan.log) |

集成测试覆盖连接建立/断开、反复请求、碎片帧/合并帧、非法长度/版本/参数、未知类型、突然断连、共享键原子累加、CAS 单赢家、整数溢出、连接上限、计算池满、单连接/全局在途上限、全局预算不足、首次消息/消息体/空闲/业务/写入超时、慢读者背压、存储容量、重复在途 ID、优雅排空、关闭截止时间及非法配置。

下游锁截止时间在实现中检查，并覆盖取消后不发生写入的单元测试；未通过注入锁长时间占用进行单独故障测试。没有外部数据库、磁盘持久化或网络下游，因此没有宣称验证这些系统的故障恢复。

## GET：128 字节值，流水线深度 1

| 连接数 | 每连接在途 | 成功 QPS | 平均 ms | P95 ms | P99 ms | 服务 CPU % | RSS 峰值 MiB | 稳定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 6,961.27 | 0.1407 | 0.2610 | 0.5431 | 56.80 | 4.25 | 通过 |
| 32 | 1 | 28,696.04 | 1.1133 | 1.6139 | 2.3511 | 62.19 | 4.50 | 通过 |
| 128 | 1 | 30,139.10 | 4.2441 | 5.5090 | 7.8682 | 54.38 | 5.25 | 通过 |
| 512 | 1 | 31,248.58 | 16.3627 | 18.0753 | 20.3557 | 53.61 | 6.62 | 通过 |
| 1024 | 1 | 27,590.12 | 37.0077 | 43.2009 | 45.8451 | 48.10 | 8.50 | 通过 |

原始机器可读报告：[get-matrix.json](../results/performance/get-matrix.json)。

## 混合读写：4 次 GET / 1 次 PUT，流水线深度 4

| 连接数 | 每连接在途 | 成功 QPS | 平均 ms | P95 ms | P99 ms | 服务 CPU % | RSS 峰值 MiB | 稳定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 32 | 4 | 50,585.97 | 2.5282 | 3.7074 | 5.2951 | 85.48 | 8.50 | 通过 |
| 128 | 4 | 48,223.82 | 10.6037 | 15.1246 | 18.8055 | 77.46 | 8.50 | 通过 |

原始报告：[mixed-matrix.json](../results/performance/mixed-matrix.json)。混合测试有更深流水线，因此其更高吞吐不能用来判断 PUT 比 GET 快。

## 连接保持及持续运行

4096 个连接先各完成一次 STATS，全部建立后保持 5 秒，再逐个完成一次 STATS。初次及最终健康检查均为 4096/4096，零连接错误。连接建立耗时 0.5985 秒；保持期间服务 CPU 0.20%，RSS 采样峰值 17.12 MiB。见 [idle-connections.json](../results/performance/idle-connections.json)。这是短期多连接验证，不是 4096 连接的多日稳定性证明。

32 个客户端持续 120 秒更新各自计数器，共 **1,784,024** 次成功请求，最少的单客户端也完成 55,274 次；无请求错误、拒绝或超时。RSS 从 4.29 MiB 到 4.79 MiB。最后查询时仅剩查询本身的一个连接和在途请求，发送队列为零。见 [soak.json](../results/integration/soak.json)。两端 RSS 快照不能单独证明无内存泄漏；ASan 检测与资源回收断言是补充证据。

压测结束后的空闲关闭耗时 0.0078 秒，退出码为 0。重任务关闭另由集成测试使用 100ms 排空期限验证在 3 秒内退出；这个 3 秒测试容忍调度延迟，不能解释为严格 100ms 实时保证。

## TSan 限制与复现

首次 TSan 运行在进入测试前出现 `unexpected memory mapping`。使用仅作用于测试进程的 `setarch x86_64 -R` 后可以运行，但遇到 timed mutex 的 `unlock of an unlocked mutex` 告警。没有修改 VM 全局安全或内核设置，也没有添加 sanitizer 抑制规则。

独立于 Atlas 的单线程程序 [tsan-clocklock-probe.cpp](../results/tsan-clocklock-probe.cpp) 使用标准 `std::timed_mutex` 和 `unique_lock::try_lock_until(steady_clock)`，也出现相同告警，记录在 [tsan-clocklock-probe.log](../results/tsan-clocklock-probe.log)。当前 libtsan 导出 timedlock 拦截器而缺少 clocklock 拦截器。这与 [GCC 官方记录的 timed mutex / TSan 问题](https://gcc.gnu.org/pipermail/gcc-cvs/2023-May/382895.html) 一致。

```bash
g++ -std=c++20 -g -fsanitize=thread -pthread \
  results/tsan-clocklock-probe.cpp -o /tmp/atlas-tsan-clocklock-probe
setarch x86_64 -R /tmp/atlas-tsan-clocklock-probe
setarch x86_64 -R env TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan
```

因此 **TSan 验证未通过，不能据此宣称不存在数据竞争**。应在修复该运行库问题的工具链上重新执行；没有为获得“绿色结果”改变生产环境的单调时钟锁等待语义。替代启动完整日志见 [ctest-tsan-no-aslr.log](../results/ctest-tsan-no-aslr.log)。

## 交付边界

当前提供内存存储、自定义 TCP 协议和扩展接口，没有 TLS/认证、真实数据库池、持久化、HTTP/WebSocket 实现、多机部署、外部故障注入或多日稳定性验证。256 MiB 是逻辑受管内存预算，不能当作包含线程栈、分配器和内核缓冲的 RSS 硬限制。关闭期限依赖任务协作取消及操作系统正常调度；不可中断的未来驱动和阻塞文件系统仍需要单独约束。

原始校验状态中的 `tsan_exit=8` 是已保留的失败，不应忽略。早期 `results/soak.log` 是 SSH 会话中断留下的未完成尝试，不计入本报告；本报告采用后续完整的 `results/integration/soak.json`。最终源码和 Linux 二进制的校验和在 [verified-source-sha256.txt](../results/verified-source-sha256.txt)。
