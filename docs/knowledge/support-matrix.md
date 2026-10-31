# IoTDB-FS-C 平台支持矩阵

| 平台 | 构建/运行目标 | 当前证据 | 状态 |
| --- | --- | --- | --- |
| Ubuntu 22.04 Linux | GCC、FUSE、ecCodes、IoTDB 客户端；普通文件与 GRIB | 本机 Debug 构建、CTest 295/295、发布包构建与校验 | 已验证 |
| 其他 Linux 发行版（含麒麟候选） | 使用系统 GCC/Clang、FUSE、ecCodes 和 IoTDB 依赖 | 尚无目标机实测；已有 Filesystem/CMake 条件分支 | 待实机验证 |
| macOS | AppleClang、macFUSE、ecCodes、Boost、Finder schema | `packaging/current/platform_check.sh --platform macos --static` 验证 CMake 依赖、schema 资源和安装契约；无 macOS 主机实测 | 待实机验证 |
| WSL2 | Linux 用户态构建、FUSE、临时文件和 Web 服务 | `packaging/current/platform_check.sh --platform wsl2 --static` 验证 WSL2 边界、运行时脚本和集成入口；当前主机无 WSL 工具 | 待实机验证 |
| Windows 原生 | WinFsp/Dokany 挂载 | CMake 在配置早期明确拒绝并指向 WSL2 | 暂不支持 |

## 验收要求

- “已验证”必须同时记录编译器、依赖版本、IoTDB 版本、FUSE 能力和测试结果。
- macOS 验收必须覆盖 Finder schema、挂载/卸载、属性、容量、权限、时间、重命名、重挂和历史版本。
- WSL2 验收必须分别记录 Linux 文件系统路径与 `/mnt/*` 路径下的行为及 FUSE 权限要求。
- 真实 IoTDB/FUSE 测试必须使用隔离数据库，并在卸载重挂后校验内容、长度、哈希、历史版本和索引。
- Windows 原生支持需要先批准 WinFsp 或 Dokany 方案，再建立独立后端和 CI；在此之前不添加伪兼容实现。
