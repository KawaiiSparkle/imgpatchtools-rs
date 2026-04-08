#!/usr/bin/env pwsh
# 更新 Cargo.toml 版本
# 格式: version = "YYYYMMDD.X.Y" (edition 保持 2024 不变)
# X.Y 表示第 1-99 次提交: 0.1 -> 9.9

param(
    [switch]$DryRun,
    [switch]$Push
)

$cargoPath = "Cargo.toml"
if (-not (Test-Path $cargoPath)) {
    Write-Error "Cargo.toml not found!"
    exit 1
}

# 读取当前内容
$content = Get-Content $cargoPath -Raw

# 获取日期 (本地时间，假设是 UTC+8)
$date = Get-Date -Format "yyyyMMdd"

# 计算今天第几次提交
$today = Get-Date -Format "yyyy-MM-dd"
$commitsToday = (git log --oneline --since="$today 00:00:00" --until="$today 23:59:59" 2>$null | Measure-Object).Count
$pushNum = $commitsToday + 1

# 限制到 99 次
if ($pushNum -gt 99) { $pushNum = 99 }

# 计算 minor 和 patch: 0.1 -> 9.9 对应第 1-99 次
$minor = [math]::Floor(($pushNum - 1) / 10)
$patch = ($pushNum - 1) % 10

# 新的 version 格式: MAJOR.MINOR.PATCH = 日期.X.Y
$newVersion = "$date.$minor.$patch"

$currentVersion = if($content -match 'version\s*=\s*"([^"]+)"'){$matches[1]} else {"unknown"}
Write-Host "Push number: $pushNum" -ForegroundColor Gray
Write-Host "Current: version=$currentVersion" -ForegroundColor Gray
Write-Host "New:     version=$newVersion" -ForegroundColor Green

if ($DryRun) {
    Write-Host "(Dry run - no changes made)" -ForegroundColor Yellow
    exit 0
}

# 更新 Cargo.toml (只更新 version, edition 保持 2024 不变)
$newContent = $content -replace '(?m)^(version\s*=\s*")[^"]*(".*)$', "`$1$newVersion`$2"

Set-Content -Path $cargoPath -Value $newContent -NoNewline
Write-Host "Updated $cargoPath" -ForegroundColor Green

# Git 操作
if ($Push) {
    git add $cargoPath
    git commit -m "chore: auto update version=$newVersion"
    git push
    Write-Host "Pushed to remote" -ForegroundColor Green
}
