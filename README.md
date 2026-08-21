# imgpatchtools-rs

A modern, cross-platform, and high-performance Rust implementation of the Android
**traditional (block-based) OTA** patching toolchain. It unpacks and applies OTA
packages — turning `.new.dat(.br/.lzma) + transfer.list + patch.dat` into partition
images and applying `bsdiff` / `imgdiff` patches — with **bit-exact output parity**
against AOSP.

[![CI](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/release.yml/badge.svg)](https://github.com/KawaiiSparkle/imgpatchtools-rs/actions/workflows/release.yml)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

## Why does this project exist?

The original AOSP patching tools are powerful but come with significant limitations:

- They are deeply embedded in the Android build system, making them difficult to compile and use standalone.
- They are not officially supported or easily runnable on Windows.
- The C/C++ codebase can be challenging to maintain, extend, and ensure memory safety in.

`imgpatchtools-rs` solves these problems by providing a single, statically-linked binary
that runs identically on **Windows, Linux, and macOS**, with no external dependencies and
no complex setup required.

## Core Principles

1. **Bit-Exact Parity**: Output files must be binary-identical to those produced by the reference AOSP implementation. This is the highest priority.
2. **Cross-Platform Determinism**: The same input must produce the exact same output on any supported operating system.
3. **Performance**: Release builds must meet or exceed the performance of the original C++ tools.
4. **Modern & Maintainable Code**: A safe, clean, and idiomatic Rust codebase built for long-term stability.

## Project Status

| Feature Area | Status | Notes |
| :-- | :-: | :-- |
| `blockimg` OTA Update | ✅ | Full/Incremental OTA, `new`/`diff`/`move` ops |
| `applypatch` | ✅ | `bsdiff` and `imgdiff` patch application, incl. reading parameters from `update-script` or an OTA zip |
| `batch` OTA Processing | ✅ | Sequential processing of full + incremental OTAs |
| `imgdiff` Patch Generation | 🚧 | Not a primary focus |

✅ = Implemented & Validated · 🚧 = In Progress

> **Scope note:** Logical-partition (`super.img`) tooling and the full Edify script
> interpreter are intentionally **not** part of this tool. Use a dedicated `lptools`
> build that tracks AOSP `partition_tools` (`lpmake` / `lpdump` / `lpunpack`) for
> super.img operations.

## Command Overview

| Command | Purpose |
| :-- | :-- |
| `blockimg` | Block-image OTA operations: `update` (auto-detects companion files), `verify`, `range-sha1`. |
| `applypatch` | Apply a `bsdiff` or `imgdiff` patch to a single file. Supports reading parameters from `update-script` (on disk or from an OTA zip). |
| `batch` | Process a full OTA and subsequent incremental OTAs in one go. |

> **Note:** For the most up-to-date syntax, always use the built-in help:
>
> ```
> imgpatchtools-rs --help
> imgpatchtools-rs <command> --help
> ```

## Build

**Requirements:**

- Rust toolchain (latest stable recommended)

**Build Command:**

```
cargo build --release
```

The final executable will be located at `target/release/imgpatchtools-rs`.

No external binaries (such as `7z`) are required at build time **or** run time —
OTA zips are read with the pure-Rust `zip` crate.

## Usage Examples

### 1. Block-Image OTA Update

Apply a block-based transfer list to generate a target image.

```
# Auto-detect companion files from current directory:
# system.transfer.list, system.new.dat(.br|.lzma), system.patch.dat
imgpatchtools-rs blockimg update system

# Full OTA (no source image) - explicit paths
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

### 2. Apply Patch from Update-Script

Apply a patch using parameters automatically read from `update-script`.

```
# Read apply_patch parameters from update-script for the boot partition
# Searches: ./update-script or META-INF/com/google/android/update-script
imgpatchtools-rs applypatch boot - --from-script

# Read parameters (and the patch file) directly from an OTA zip — no unzip tool needed
imgpatchtools-rs applypatch boot - --ota full_ota.zip

# Explicit patch application
imgpatchtools-rs applypatch \
  boot.img \
  boot_patched.img \
  <target_sha1> \
  <target_size> \
  patch/boot.img.p
```

### 3. Compute Range SHA-1 from Update-Script

Calculate SHA-1 hash for specific block ranges, reading ranges from `update-script`.
When ranges are read from the script, the expected SHA1 is also extracted and compared.

```
# Auto-read ranges from update-script for the system partition
# Also compares with expected SHA1 from the script
imgpatchtools-rs blockimg range-sha1 system
# Output:
# Computed: e4c514166c64863dcfb97bfaa277efa8240c6115
# Expected: e4c514166c64863dcfb97bfaa277efa8240c6115
# Result: MATCH ✓

# Explicit ranges (no comparison)
imgpatchtools-rs blockimg range-sha1 system.img "4,0,10,20,30"
```

### 4. Batch-Process Multiple OTAs

Reconstruct final partition images by applying a full OTA, followed by several incrementals.

```
imgpatchtools-rs batch \
  full_ota.zip \
  incremental_ota_1.zip \
  incremental_ota_2.zip \
  --workdir ./temp_work \
  --output ./final_images
```

## FAQ

**Q: Why not just use the original AOSP tools?**

A: The original tools require a complex AOSP build environment, are not easily compiled
for Windows, and can be difficult to use in cross-platform CI/CD pipelines.
`imgpatchtools-rs` provides a single, dependency-free binary that "just works" everywhere.

**Q: Is this project guaranteed to be 100% bug-for-bug compatible?**

A: The goal is **bit-exact output parity**. This means for a given valid input, the
resulting file should be identical. The project aims to replicate correct AOSP behavior,
not its bugs. However, where AOSP behavior is ambiguous or has side effects that clients
rely on, we prioritize compatibility.

**Q: Can I use this to create my own OTA packages?**

A: The primary focus is on **applying** existing patches and performing device-side logic
on a host machine. The generation of a complete, signable OTA package is outside the
current scope.

**Q: Where are the super.img (lpmake / lpdump / lpunpack) tools?**

A: They were split out. Tracking AOSP `partition_tools` directly (rather than reimplementing
them) keeps the LP metadata handling in lock-step with AOSP. See the `lptools` repository
for a workflow that builds them from a chosen AOSP revision.

## References

This project would not be possible without referencing the excellent work in:

- The Android Open Source Project (AOSP)
- [LineageOS/android_bootable_deprecated-ota](https://github.com/LineageOS/android_bootable_deprecated-ota)
- [GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools](https://github.com/GrapheneOS/platform_system_extras/tree/16-qpr2/partition_tools)

## License

This project is licensed under the **GNU General Public License v3.0**. Please see the
`LICENSE` file for details.
