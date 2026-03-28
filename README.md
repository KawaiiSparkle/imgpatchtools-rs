# imgpatchtools-rs

A **bit-exact**, high-performance, cross-platform Rust reimplementation of
[AOSP imgpatchtools](https://github.com/erfanoabdi/imgpatchtools) and the
AOSP `updater` block-image / apply-patch pipeline.

## Goals

| Property                 | Guarantee                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------- |
| **Bit-Exact**      | Output is binary-identical to AOSP `updater` — same padding, alignment, hashes. |
| **Performance**    | Release builds match or exceed the original C++ implementation.                    |
| **Cross-platform** | Linux, macOS, and Windows produce identical results.                               |

## Subcommands

| Command        | Description                                                          |
| -------------- | -------------------------------------------------------------------- |
| `blockimg`   | Apply a block-based OTA (transfer list + new data + patch → image). |
| `applypatch` | Apply a bsdiff / imgdiff patch to a single file.                     |
| `imgdiff`    | Create an imgdiff-format patch between two images.                   |
| `edify`      | Execute an Edify script (subset used by OTA).                        |

## Build

```bash
cargo build --release
```

## Usage

```bash
# Block image update
imgpatchtools-rs blockimg \
    --transfer-list system.transfer.list \
    --new-data system.new.dat.br \
    --patch-data system.patch.dat \
    --target system.img

# Apply patch
imgpatchtools-rs applypatch \
    --source boot.img \
    --patch boot.patch \
    --target boot-patched.img \
    --target-hash <sha1hex>

# Create imgdiff patch(Not implemented yet)
imgpatchtools-rs imgdiff \
    --source system-old.img \
    --target system-new.img \
    --output system.imgdiff

# Run Edify script
imgpatchtools-rs edify \
    META-INF/com/google/android/updater-script \
    -w <WORKDIR>





```
