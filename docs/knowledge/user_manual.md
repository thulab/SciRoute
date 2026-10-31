# IoTDB-FS-C 用户手册

## 系统简介

IoTDB-FS-C 是一个基于 FUSE 的用户态文件系统。用户可以把它挂载为本地目录，通过常规文件命令读写数据；系统在后端维护文件元数据、索引和版本信息，并提供查询检索、Web 浏览和高级结构化数据访问能力。

这份手册以文件系统使用为主线。GRIB 气象数据只作为高级结构化数据访问的具体案例，不作为独立的顶层功能组织。

## 核心能力

- 挂载为本地目录，支持常见 POSIX 风格文件操作。
- 使用 IoTDB 管理文件元数据、索引和版本信息。
- 支持最新视图、历史版本查看和跨版本查询入口。
- 支持自然语言 grep、跨文件查询和结构化子数据访问。
- 提供轻量 Web 文件管理器，用于浏览、预览和下载挂载目录中的文件。
- 支持打包发布、自检、集成验证和性能测试流程。

## 阅读路径

- 第一次试用：阅读 [快速开始与安装运行](user_manual/02-quick-start.md)。
- 理解系统概念：阅读 [概览与基本概念](user_manual/01-overview.md)。
- 日常文件操作：阅读 [基础文件操作与版本管理](user_manual/03-filesystem-usage.md)。
- 查询和高级访问：阅读 [查询检索与高级扩展访问](user_manual/04-query-and-advanced-access.md)。
- 使用浏览器查看和管理文件：阅读 [Web 文件管理器](user_manual/05-web-file-manager.md)，其中包含页面操作说明。
- 准备演示流程：阅读 [典型使用场景](user_manual/06-scenarios.md)。
- 部署、测试和排错：阅读 [打包发布、测试验证与故障排查](user_manual/07-deployment-testing-troubleshooting.md)。
- 查命令和参数：阅读 [参考资料](user_manual/08-reference.md)。

## 手册目录

1. [概览与基本概念](user_manual/01-overview.md)
2. [快速开始与安装运行](user_manual/02-quick-start.md)
3. [基础文件操作与版本管理](user_manual/03-filesystem-usage.md)
4. [查询检索与高级扩展访问](user_manual/04-query-and-advanced-access.md)
5. [Web 文件管理器](user_manual/05-web-file-manager.md)
6. [典型使用场景](user_manual/06-scenarios.md)
7. [打包发布、测试验证与故障排查](user_manual/07-deployment-testing-troubleshooting.md)
8. [参考资料](user_manual/08-reference.md)
