# imgpatchtools-rs

[![CI](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/ci.yml)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

A modern, cross-platform, and high-performance Rust implementation of the Android OTA patching toolchain. This project aims to be a bit-exact replacement for AOSP tools like `updater`, `blockimg`, `applypatch`, and the Edify scripting runtime.

## Why does this project exist?

The original AOSP patching tools are powerful but come with significant limitations:
- They are deeply embedded in the Android build system, making them difficult to compile and use standalone.
- They are not officially supported or easily runnable on Windows.
- The C/C++ codebase can be challenging to maintain, extend, and ensure memory safety in.

`imgpatchtools-rs` solves these problems by providing a single, statically-linked binary that runs identically on **Windows, Linux, and macOS**, with no external dependencies or complex setup required.

## Core Principles

1.  **Bit-Exact Parity**: Output files must be binary-identical to those produced by the reference AOSP implementation. This is the highest priority.
2.  **Cross-Platform Determinism**: The same input must produce the exact same output on any supported operating system.
3.  **Performance**: Release builds must meet or exceed the performance of the original C++ tools.
4.  **Modern & Maintainable Code**: A safe, clean, and idiomatic Rust codebase built for long-term stability.

## Project Status

| Feature Area | Status | Notes |
| :--- | :---: | :--- |
| `blockimg` OTA Update | ✅ | Full/Incremental OTA, `new`/`diff`/`move` ops |
| `applypatch` | ✅ | `bsdiff` and `imgdiff` patch application |
| Dynamic Partitions (`super.img`) | ✅ | `lpmake`, `lpdump`, `lpunpack`, sparse I/O |
| `batch` OTA Processing | ✅ | Sequential processing of full + incremental OTAs |
| `edify` Script Execution | 🚧 | Core functions implemented; coverage is partial |
| `imgdiff` Patch Generation | ⚠️ | Implemented, but not a primary focus |
| Performance & UX | 🚧 | Functionally complete, but optimizations are ongoing |

✅ = Implemented & Validated | 🚧 = In Progress | ⚠️ = Low Priority / Use with caution

## Command Overview

| Command | Purpose |
| :--- | :--- |
| `blockimg` | Block-image OTA operations: `update`, `verify`, `range-sha1`. |
| `applypatch` | Apply a `bsdiff` or `imgdiff` patch to a single file. |
| `imgdiff` | Create an `imgdiff`-format patch between two files. |
| `edify` | Execute an Edify `updater-script`. |
| `super` | Smart `super.img` builder from a directory of partition images. |
| `lpmake` | Expert-mode `super.img` builder with fine-grained control. |
| `lpdump` | Dump LP metadata from a `super.img`. |
| `lpunpack` | Extract all partition images from a `super.img`. |
| `batch` | Process a full OTA and subsequent incremental OTAs in one go. |

> **Note:** For the most up-to-date syntax, always use the built-in help:
> ```bash
> imgpatchtools-rs --help
> imgpatchtools-rs <command> --help
> ```

## Build

**Requirements:**
- Rust toolchain (latest stable recommended)

**Optional Dependencies:**
- `7z` (7-Zip): Required for some `edify` functions that extract from archives.

**Build Command:**
```bash
cargo build --release
```
The final executable will be located at `target/release/imgpatchtools-rs`.

## Usage Examples

### 1. Block-Image OTA Update

Apply a block-based transfer list to generate a target image.

```bash
# Full OTA (no source image)
imgpatchtools-rs blockimg update \
  system.img \
  system.transfer.list \
  system.new.dat.br \
  system.patch.dat

# Incremental OTA (with a source image)
imgpatchtools-rs blockimg update \
  system.img \
  system.transfer.list \
  system.new.dat.br \
  system.patch.dat \
  --source old-system.img
```

### 2. Unpack a `super.img`

Extract all logical partition images from a `super.img`.

```bash
imgpatchtools-rs lpunpack super.img -o ./unpacked_partitions
```

### 3. Execute an `updater-script`

Run an Edify script within a specified working directory.

```bash
# The workdir should contain files the script expects (e.g., firmware, patches)
imgpatchtools-rs edify \
  META-INF/com/google/android/updater-script \
  --workdir ./ota_extracted
```

### 4. Batch-Process Multiple OTAs

Reconstruct final partition images by applying a full OTA, followed by several incrementals.

```bash
imgpatchtools-rs batch \
  full_ota.zip \
  incremental_ota_1.zip \
  incremental_ota_2.zip \
  --workdir ./temp_work \
  --output ./final_images
```

## FAQ

**Q: Why not just use the original AOSP tools?**  
A: The original tools require a complex AOSP build environment, are not easily compiled for Windows, and can be difficult to use in cross-platform CI/CD pipelines. `imgpatchtools-rs` provides a single, dependency-free binary that "just works" everywhere.

**Q: Is this project guaranteed to be 100% bug-for-bug compatible?**  
A: The goal is **bit-exact output parity**. This means for a given valid input, the resulting file should be identical. The project aims to replicate correct AOSP behavior, not its bugs. However, where AOSP behavior is ambiguous or has side effects that clients rely on, we prioritize compatibility.

**Q: Can I use this to create my own OTA packages?**  
A: The primary focus is on **applying** existing patches and performing device-side logic on a host machine. While some patch creation tools like `imgdiff` are included, the generation of a complete, signable OTA package is outside the current scope.

## References

This project would not be possible without referencing the excellent work in:
- The Android Open Source Project (AOSP)
- [LineageOS/android_bootable_deprecated-ota](https://github.com/LineageOS/android_bootable_deprecated-ota)
- [GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools](https://github.com/GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools)

## License

This project is licensed under the **GNU General Public License v3.0**. Please see the `LICENSE` file for details.
