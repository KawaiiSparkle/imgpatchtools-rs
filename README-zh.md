# imgpatchtools-rs

[![CI](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/ci.yml)

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

一个现代、跨平台、高性能的 Android OTA 补丁工具链的 Rust 实现。本项目旨在成为 AOSP `updater`、`blockimg`、`applypatch` 和 Edify 脚本运行时的一个二进制级别一致 (bit-exact) 的替代品。
(纯AI生成,但人工测试了一周)

## 为什么需要这个项目？

AOSP 原版的补丁工具有其强大的功能，但也存在显著的局限性：

- 它们深度集成在 Android 构建系统中，难以独立编译和使用。
- 官方不支持，也很难在 Windows 上运行。
- C/C++ 代码库在维护、扩展和确保内存安全方面充满挑战。

`imgpatchtools-rs` 旨在解决这些问题。它提供了一个单一的、静态链接的二进制文件，可以在 **Windows、Linux 和 macOS** 上一致地运行，无需外部依赖或复杂的环境配置。

## 核心原则

1. **二进制级别一致 (Bit-Exact)**：输出文件必须与 AOSP 参考实现产生的二进制文件完全相同。这是最高优先级。
2. **跨平台确定性**：相同的输入在任何支持的操作系统上都必须产生完全相同的输出。
3. **高性能**: Release 构建版本的性能必须达到或超过原版 C++ 工具。
4. **现代化与可维护性**: 代码库使用安全、整洁且符合 Rust 惯例的方式编写，为长期稳定而设计。

## 项目状态

| 功能模块 | 状态 | 说明 |

| :--- | :---: | :--- |

| `blockimg` OTA 升级 | ✅ | 完整/增量 OTA, `new`/`diff`/`move` 操作 |

| `applypatch` | ✅ | `bsdiff` 和 `imgdiff` 补丁应用 |

| 动态分区 (`super.img`) | ✅ | `lpmake`, `lpdump`, `lpunpack`, sparse 格式读写 |

| `batch` 批量 OTA 处理 | ✅ | 按顺序处理一个全量包和多个增量包 |

| `edify` 脚本执行 | 🚧 | 核心功能已实现，但函数覆盖尚不完整 |

| `imgdiff` 补丁生成 | ⚠️ | 已实现，但非当前开发重点 |

| 性能与用户体验 | 🚧 | 功能已完备，但仍在持续优化 |

✅ = 已实现并验证 | 🚧 = 开发中 | ⚠️ = 低优先级/谨慎使用

## 命令概览

| 命令 | 用途 |

| :--- | :--- |

| `blockimg` | 块镜像 OTA 操作：`update` (应用更新), `verify` (校验), `range-sha1` (范围哈希)。 |

| `applypatch` | 对单个文件应用 `bsdiff` 或 `imgdiff` 补丁。 |

| `imgdiff` | 在两个文件之间创建一个 `imgdiff` 格式的补丁。 |

| `edify` | 执行一个 Edify `updater-script` 脚本。 |

| `super` | 从包含分区镜像的目录智能构建 `super.img`。 |

| `lpmake` | 专家模式的 `super.img` 构建器，提供精细化控制。 |

| `lpdump` | 从 `super.img` 中转储 LP (逻辑分区) 元数据。 |

| `lpunpack` | 从 `super.img` 中提取所有分区镜像。 |

| `batch` | 一次性处理一个全量 OTA 包和随后的多个增量 OTA 包。 |

> **注意:** 如需获取最新最准确的命令语法，请始终使用内置的帮助文档：

> imgpatchtools-rs --help

> imgpatchtools-rs <命令> --help

## 构建

**环境要求:**

- Rust 工具链 (推荐使用最新的稳定版)

**可选依赖:**

- `7z` (7-Zip): `edify` 的某些从压缩包提取文件的功能需要它。

**构建命令:**

```bash

cargo build --release

```

最终的可执行文件位于 `target/release/imgpatchtools-rs`。

## 使用示例

### 1. 块镜像 OTA 升级

应用一个基于块的 transfer list 来生成目标镜像。

```bash

# 全量 OTA (无源镜像)

imgpatchtools-rs blockimg update\

  system.img \

  system.transfer.list\

  system.new.dat.br \

  system.patch.dat


# 增量 OTA (提供源镜像)

imgpatchtools-rs blockimg update\

  system.img \

  system.transfer.list\

  system.new.dat \

  system.patch.dat\

  --source old-system.img

```

### 2. 解包 `super.img`

从 `super.img` 中提取所有逻辑分区镜像。

```bash

imgpatchtools-rs lpunpack super.img -o ./unpacked_partitions

```

### 3. 执行 `updater-script`

在指定的工作目录中运行 Edify 脚本。

```bash

# 工作目录应包含脚本所需的文件 (如固件、补丁等)

imgpatchtools-rs edify \

  META-INF/com/google/android/updater-script \

  --workdir ./ota_extracted

```

### 4. 批量处理多个 OTA 包

通过依次应用一个全量 OTA 和多个增量 OTA，来重建最终的分区镜像。

```bash

imgpatchtools-rs batch\

  full_ota.zip \

  incremental_ota_1.zip\

  incremental_ota_2.zip \

  --workdir. /temp_work \

  --output ./final_images

```

## 常见问题 (FAQ)

**问：为什么不直接用 AOSP 原版工具？**

答：原版工具需要复杂的 AOSP 构建环境，不易为 Windows 编译，并且难以用于跨平台的 CI/CD 流水线。`imgpatchtools-rs` 提供了一个无依赖的单一二进制文件，可以在任何地方“开箱即用”。

**问：这个项目能保证 100% 与原版“bug 对 bug”兼容吗？**

答：我们的目标是**二进制输出一致性**。这意味着对于给定的有效输入，生成的输出文件应该是相同的。项目旨在复现 AOSP 的正确行为，而不是它的 bug。但是，当 AOSP 的行为存在歧义或其副作用被客户端所依赖时，我们会优先考虑兼容性。

**问：我可以用它来制作我自己的 OTA 包吗？**

答：本项目的主要焦点是在宿主机上**应用**现有的补丁和执行设备端逻辑。虽然包含了一些如 `imgdiff` 这样的补丁创建工具，但生成一个完整的、可签名的 OTA 包超出了当前的范围。

## 参考

本项目的实现离不开以下优秀项目的启发和参考：

- Android 开源项目 (AOSP)
- [LineageOS/android_bootable_deprecated-ota](https://github.com/LineageOS/android_bootable_deprecated-ota)
- [GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools](https://github.com/GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools)

## 许可证

本项目基于 **GNU General Public License v3.0** 许可证。详情请参阅 `LICENSE` 文件。

