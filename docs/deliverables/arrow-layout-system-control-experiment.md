# Arrow 布局与系统收益的对照实验

本实验冻结当前生产实现，只修改实验读取器以读取真实原始文件；不在计时期间优化查询服务。使用 Electricity 140,256 × 370、未压缩 Arrow、固定 L96/H24、51 请求/102 窗口。原始文件为两行/batch，128 点 step-major 派生文件为一行/batch。保留原始 source/version 和所有文件字节。

## 主对照与辅助对照

主 2×2：真实原始文件 / 128 点 step-major，分别放在普通 FS 和 IoTDB 后端。两个后端使用同一个 Python 实验读取器、PyArrow decoder、1 GiB 按字节缓存、窗口组装和标准化函数，均返回连续 Float32 input/target 和 bool mask。普通 FS 主路径为 mmap，普通 read 作为敏感性对照。IoTDB 路径通过现有只读范围 helper 获取原始 IPC message，不包含生产 typed 前端。

额外保留 channel-major 8192 和 channel-major 128：前者隔离原始两行/batch 与派生一行/batch 的差别；后两者之间隔离时间分块；channel-major 128 与 step-major 128 隔离转置。原始 batch 内保留各 row 的视图，不通过预先转置整个数据集人为改变其暖缓存布局。本地 mmap/read 不受 IoTDB 8 MiB 传输限制，不强制分段重组；IoTDB 仍遵守后端范围上限。旧实验读取器曾对所有后端执行该分段，本轮已纠正，不能把旧 FS 耗时与本轮混用。

生产层另外运行真实原始 / step-major 128 的 typed 查询，保留权限、调度、公共缓存、IPC/FUSE 和客户端结果校验。这一层与实验读取器的差值是两条完整实现路径的差值，不能全部归因于数据库或 FUSE 的固有成本。

## 固定条件与统计

- 5 个独立重复，每个 reader 一遍应用缓存冷轨迹、三遍同一上下文暖轨迹。按重复轮次循环移动并反转运行顺序，各臂不并发计时。
- 应用缓存冷不等于磁盘冷：保留 OS/IoTDB 缓存，文件校验和 oracle 读取在计时前完成，不宣称首次磁盘加载性能。
- typed 每个冷臂重启专用实验 daemon；保留 1 GiB/16384 entries/4 workers/4 read lanes，暖遍复用 prepared ID。结束恢复原配置并关闭 profiling。
- 逐请求在计时结束后验证标准化 Float32 位模式、mask 和连续输出。取数层分别记录原始窗口读取、标准化/输出整理、总耗时。typed 记录首次准备与各服务阶段。
- 全部文件 hash 在前后校验。原始文件只生成 offset 索引，单列索引构建成本；派生文件复用已验证 artifact，构建与额外存储成本引用其构建清单。
- 冷统计以独立重复为单位；暖统计先在每个重复内取三遍均值，再汇总五个重复，避免将暖遍伪装成独立样本。报告均值、范围和配对差值，不依据五次测量声称统计显著性。

## 要回答的问题

令 A=FS 原始，B=FS 派生，C=IoTDB 原始，D=IoTDB 派生：

- A−B 与 C−D：同一后端内部的布局收益。
- C−A 与 D−B：同一布局下后端路径的额外成本。
- (C−D)−(A−B)：布局改变与后端路径的交互，而非可直接宣称的 IoTDB 独占收益。
- 相同布局下 typed 总时间、首次 prepare、payload、窗口执行、结果交付：生产路径为何获益或未获益。

本轮是单客户端、固定 50-step 轨迹的数值/取数回放，不重新运行模型，不代表多实验共享、完整 epoch、跨文件筛选或 DDP 性能。

执行结果见 [五轮对照报告](../reports/arrow-layout-system-control-results-20260930.md)。
