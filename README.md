# Atlas：有界并发 Linux 网络服务

这是可运行的 C++20 / Boost.Asio 协程项目。业务是支持版本条件写入、原子计数的内存键值服务，另提供可取消计算及运行状态查询。数据不持久化。

## 构建与运行

实际验证环境：Ubuntu 24.04、Linux 6.8、GCC 13.3、Boost 1.83、CMake 3.28、Python 3.12。要求 C++20 编译器、Boost >= 1.74、CMake >= 3.20；使用 presets 时 CMake >= 3.21。Boost.System 使用 header-only 模式，无第三方数据库依赖。

```bash
# 仅在缺少依赖时安装；本次验证复用了虚拟机已有依赖
sudo apt-get install build-essential cmake libboost-dev python3
cmake --preset release
cmake --build --preset release -j2
ctest --preset release
./build/atlas_server --config config/server.conf
```

启动输出 `READY 7777`；`port=0` 自动选择空闲端口。默认监听 `127.0.0.1:7777`。在第二个终端运行：

```bash
python3 tools/client.py put greeting hello
python3 tools/client.py get greeting
python3 tools/client.py add counter 1
python3 tools/client.py hash 100000 seed
python3 tools/client.py stats
```

Ctrl-C 或 `kill -TERM <pid>` 触发限时排空。进程退出时输出 `FINAL` 指标。可通过已有仓储接口替换内存存储；HTTP/WebSocket 适配器尚未实现。

交付包也包含此次验证的 Linux x86_64 可执行文件 `bin/atlas_server`（Ubuntu 24.04 动态链接环境）。在其他发行版上建议按上面步骤从源码构建；解压工具未保留执行权限时先运行 `chmod +x bin/atlas_server`。它不能作为 Windows 程序直接启动。

## 目录

```text
include/atlas/  核心接口、协议、Session、Server
src/
  main.cpp     配置、线程启动、信号、监控、退出编排
  server.cpp   连接准入与全局关闭
  session.cpp  strand、协程读写、在途请求和超时
  protocol.cpp 帧编码与解码
  core.cpp     配置、预算、指标、日志、计算池
  service.cpp  请求路由、参数校验、业务与 Repository 实现
config/        完整默认配置
tests/         C++ 单元测试、独立 Python 黑盒集成测试
tools/         客户端、异步压测器、复现验证脚本
docs/          架构说明与实测报告
results/       实际构建、测试、压测原始输出
```

## 测试与压测

```bash
# 全部集成测试 + 可调整时长的持续并发测试
python3 tests/integration.py --server build/atlas_server \
  --soak-seconds 120 --artifacts results/manual-integration
# GET / 混合读写矩阵，并验证 4096 个长连接
python3 tools/perf_run.py --server build/atlas_server \
  --idle-connections 4096 --output results/manual-performance
# ASan + UBSan
cmake --preset asan
cmake --build --preset asan -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ctest --preset asan
# TSan（运行支持取决于主机地址空间布局）
cmake --preset tsan
cmake --build --preset tsan -j2
TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan
# 顺序运行以上验证；详细结果保存在 results
bash tools/validate.sh
```

压测工具仅使用 Python 标准库。QPS 只计通过内容与 ID 检查的成功响应；客户端 P95/P99 使用相对桶宽不超过 2% 的固定直方图。`perf_run.py` 在进程允许的 hard limit 内提高自身和子进程文件描述符 soft limit，不修改系统全局配置。详细方法见 [tools/README.md](tools/README.md)，实测见 [docs/RESULTS.md](docs/RESULTS.md)。

## 运行约束

- 配置文件采用 `key=value`，支持 `#` 注释。未知、重复、越界配置启动时报错；不支持热更新。
- 连接、帧、发送队列、在途请求、计算任务、存储及日志均有独立上限。全局预算用于应用受管数据及保守预留，不等同于 RSS 硬上限。
- 每个连接预留 `4096 + max_frame_bytes`；每个任务预留 `3 * max_frame_bytes + 2048`。所有上限共同生效，不能将单项连接上限理解为任何负载下都能接满。
- 单连接在途超限、重复在途 ID、非法帧、发送队列超限会关闭连接；全局请求/计算配额不足返回 overloaded，内存不足到无法排队响应时关闭连接。
- 响应允许乱序，客户端按 RequestID 匹配。流水线中涉及同一键的操作若要求顺序，必须等待前一响应。PUT 的非零 expected_version 提供 CAS；RequestID 不提供重试幂等性。
- `metrics_file` 是监控线程原子替换的最新 JSON 快照，大小有界。CPU/RSS 采样不占用 I/O 线程；STATS 返回内存中的指标。服务端延迟是业务准入到响应排队的桶上界估计，不含慢客户端传输时间。
- 日志以 JSON 写 stderr，包含 UTC 时间、等级、线程、连接和请求 ID。日志满或接收端慢时丢弃并计数；普通请求错误默认 debug。外部重定向的日志文件应由 journald/logrotate 等管理。
- 取消是协作式；计算循环和下游锁等待检查截止时间。写入可能已提交但响应超时，不承诺超时回滚或 exactly-once。
- 没有真实数据库，因此没有虚设数据库连接池。未来适配器须实现有界连接池和驱动超时。当前下游超时覆盖仓储锁等待。
- 默认不提供 TLS、认证、持久化、HTTP 或 WebSocket；适合本机验证及作为服务开发基础。多日稳定性与独立机器压测需要部署环境进一步验证。

完整协议、所有权、关闭语义与扩展边界见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。协程与 strand 使用方式参考 [Boost.Asio 官方说明](https://www.boost.org/doc/libs/1_83_0/doc/html/boost_asio/overview/composition/cpp20_coroutines.html)。
