# imgpatchtools-rs

[![CI](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/release.yml/badge.svg)](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/release.yml)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

一个现代、跨平台、高性能的 Android **传统（块式）OTA** 补丁工具链的 Rust 实现。
它负责解包并应用 OTA 包——把 `.new.dat(.br/.lzma) + transfer.list + patch.dat` 还原为分区镜像，
并应用 `bsdiff` / `imgdiff` 补丁——输出与 AOSP **逐字节一致 (bit-exact)**。

## 为什么需要这个项目？

AOSP 原版的补丁工具有其强大的功能，但也存在显著的局限性：

- 它们深度集成在 Android 构建系统中，难以独立编译和使用。
- 官方不支持，也很难在 Windows 上运行。
- C/C++ 代码库在维护、扩展和确保内存安全方面充满挑战。

`imgpatchtools-rs` 旨在解决这些问题。它提供了一个单一的、静态链接的二进制文件，可以在
**Windows、Linux 和 macOS** 上一致地运行，无需外部依赖或复杂的环境配置。

## 核心原则

1. **二进制级别一致 (Bit-Exact)**：输出文件必须与 AOSP 参考实现产生的二进制文件完全相同。这是最高优先级。
2. **跨平台确定性**：相同的输入在任何支持的操作系统上都必须产生完全相同的输出。
3. **高性能**：Release 构建版本的性能必须达到或超过原版 C++ 工具。
4. **现代化与可维护性**：代码库使用安全、整洁且符合 Rust 惯例的方式编写，为长期稳定而设计。

## 项目状态

| 功能模块 | 状态 | 说明 |
| :--- | :---: | :--- |
| `blockimg` OTA 升级 | ✅ | 完整/增量 OTA，`new`/`diff`/`move` 操作 |
| `applypatch` | ✅ | `bsdiff` 和 `imgdiff` 补丁应用，支持从 `update-script` 或 OTA zip 读取参数 |
| `batch` 批量 OTA 处理 | ✅ | 按顺序处理一个全量包和多个增量包 |
| `imgdiff` 补丁生成 | 🚧 | 非当前开发重点 |

✅ = 已实现并验证 · 🚧 = 开发中

> **范围说明**：逻辑分区（`super.img`）工具和完整的 Edify 脚本解释器**不属于**本工具。
> super.img 相关操作请使用追踪 AOSP `partition_tools`（`lpmake` / `lpdump` / `lpunpack`）
> 的专用 `lptools` 构建。

## 命令概览

| 命令 | 用途 |
| :--- | :--- |
| `blockimg` | 块镜像 OTA 操作：`update`（自动检测配套文件）、`verify`（校验）、`range-sha1`（范围哈希）。 |
| `applypatch` | 对单个文件应用 `bsdiff` 或 `imgdiff` 补丁。支持从 `update-script`（磁盘或 OTA zip）读取参数。 |
| `batch` | 一次性按顺序处理一个全量包和多个增量包。 |

> **注意**：最新的语法请始终以内置帮助为准：
>
> ```
> imgpatchtools-rs --help
> imgpatchtools-rs <command> --help
> ```

## 构建

**环境要求：**

- Rust 工具链（推荐最新 stable）

**构建命令：**

```
cargo build --release
```

最终可执行文件位于 `target/release/imgpatchtools-rs`。

构建和运行时**都不需要任何外部二进制**（如 `7z`）——OTA zip 使用纯 Rust 的 `zip` crate 读取。

## 使用示例

### 1. 块镜像 OTA 升级

应用块式 transfer list 生成目标镜像。

```
# 从当前目录自动检测配套文件：
# system.transfer.list、system.new.dat(.br|.lzma)、system.patch.dat
imgpatchtools-rs blockimg update system

# 全量 OTA（无源镜像）- 显式路径
imgpatchtools-rs blockimg update \
  system.img \
  system.transfer.list \
  system.new.dat.br \
  system.patch.dat

# 增量 OTA（带源镜像）
imgpatchtools-rs blockimg update \
  system.img \
  system.transfer.list \
  system.new.dat.br \
  system.patch.dat \
  --source old-system.img
```

### 2. 从 update-script 应用补丁

自动从 `update-script` 读取参数来应用补丁。

```
# 从 update-script 读取 boot 分区的 apply_patch 参数
# 搜索路径：./update-script 或 META-INF/com/google/android/update-script
imgpatchtools-rs applypatch boot - --from-script

# 直接从 OTA zip 读取参数（以及补丁文件）—— 无需任何解压工具
imgpatchtools-rs applypatch boot - --ota full_ota.zip

# 显式补丁应用
imgpatchtools-rs applypatch \
  boot.img \
  boot_patched.img \
  <target_sha1> \
  <target_size> \
  patch/boot.img.p
```

### 3. 从 update-script 计算范围 SHA-1

计算特定块范围的 SHA-1 哈希，范围从 `update-script` 读取。
当范围从脚本读取时，也会提取期望的 SHA1 并进行比对。

```
# 从 update-script 自动读取 system 分区的范围
# 同时与脚本中的期望 SHA1 比对
imgpatchtools-rs blockimg range-sha1 system
# 输出：
# Computed: e4c514166c64863dcfb97bfaa277efa8240c6115
# Expected: e4c514166c64863dcfb97bfaa277efa8240c6115
# Result: MATCH ✓

# 显式范围（不比对）
imgpatchtools-rs blockimg range-sha1 system.img "4,0,10,20,30"
```

### 4. 批量处理多个 OTA

依次应用一个全量包和若干增量包，重建最终分区镜像。

```
imgpatchtools-rs batch \
  full_ota.zip \
  incremental_ota_1.zip \
  incremental_ota_2.zip \
  --workdir ./temp_work \
  --output ./final_images
```

## FAQ

**Q：为什么不直接用 AOSP 原版工具？**

A：原版工具需要复杂的 AOSP 构建环境，难以在 Windows 上编译，也难以用于跨平台 CI/CD 流水线。
`imgpatchtools-rs` 提供一个单一、零依赖的二进制，处处可用。

**Q：这个项目保证 100% bug 级兼容吗？**

A：目标是**逐字节输出一致**。即对于给定的合法输入，输出文件应完全相同。本项目旨在复现 AOSP
的正确行为而非其 bug。不过在 AOSP 行为模糊或客户端依赖其副作用之处，我们优先保证兼容性。

**Q：可以用它创建自己的 OTA 包吗？**

A：本项目重点在于**应用**已有补丁、在宿主机上执行设备侧逻辑。生成完整的、可签名的 OTA 包不在当前范围内。

**Q：super.img（lpmake / lpdump / lpunpack）工具去哪了？**

A：已被拆分出去。直接追踪 AOSP `partition_tools`（而不是重新实现）能让 LP 元数据处理与 AOSP 保持同步。
请参见 `lptools` 仓库，其中包含从指定 AOSP 版本构建这些工具的工作流。

## 参考

本项目的实现离不开以下优秀工作的参考：

- The Android Open Source Project (AOSP)
- [LineageOS/android_bootable_deprecated-ota](https://github.com/LineageOS/android_bootable_deprecated-ota)
- [GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools](https://github.com/GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools)

## 许可证

本项目采用 **GNU General Public License v3.0** 许可，详见 `LICENSE` 文件。
