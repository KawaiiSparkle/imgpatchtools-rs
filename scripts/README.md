# 版本管理脚本

## auto-version.yml (GitHub Actions)

每次推送到 main/master 分支时自动运行：

1. **edition** 更新为 `YYYYMMDD-推送次数` 格式
   - 例如: `20260409-3` 表示 2026年4月9日的第3次推送

2. **version** 更新为 `MAJOR.0.0` 格式
   - 只保留 major 版本号，每次推送自动 +1
   - 例如: `1.0.0` → `2.0.0`

## update-version.ps1 (本地脚本)

手动更新版本（推送前使用）：

```powershell
# 干运行 - 查看会更新什么
.\scripts\update-version.ps1 -DryRun

# 更新 Cargo.toml 但不推送
.\scripts\update-version.ps1

# 更新并自动 git add/commit/push
.\scripts\update-version.ps1 -Push
```

## 为什么这样设计？

- **edition**: 用日期格式可以直观看到开发进度，推送次数反映活跃程度
- **version**: 只保留 major 简化版本管理，适合快速迭代的工具项目
