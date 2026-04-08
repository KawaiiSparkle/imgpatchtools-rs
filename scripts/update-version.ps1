#!/usr/bin/env pwsh
# 更新 Cargo.toml 版本和 edition
# 格式: edition = "YYYYMMDD-推送次数", version = "MAJOR.0.0"

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

# 获取日期和推送次数
$date = Get-Date -Format "yyyyMMdd"

# 计算今天第几次提交
$commitsToday = (git log --oneline --since="$($date.Substring(0,4))-$($date.Substring(4,2))-$($date.Substring(6,2)) 00:00:00" --until="$($date.Substring(0,4))-$($date.Substring(4,2))-$($date.Substring(6,2)) 23:59:59" 2>$null | Measure-Object).Count
$pushNum = $commitsToday + 1

$newEdition = "$date-$pushNum"

# 获取当前 major version 并递增
if ($content -match 'version\s*=\s*"(\d+)\.') {
    $currentMajor = [int]$matches[1]
    $newMajor = $currentMajor + 1
} else {
    $newMajor = 1
}
$newVersion = "$newMajor.0.0"

Write-Host "Current: edition=$(if($content -match 'edition\s*=\s*"([^"]+)"'){$matches[1]}), version=$(if($content -match 'version\s*=\s*"([^"]+)"'){$matches[1]})" -ForegroundColor Gray
Write-Host "New:     edition=$newEdition, version=$newVersion" -ForegroundColor Green

if ($DryRun) {
    Write-Host "(Dry run - no changes made)" -ForegroundColor Yellow
    exit 0
}

# 更新 Cargo.toml
$newContent = $content -replace '(?m)^(edition\s*=\s*")[^"]*(".*)$', "`$1$newEdition`$2"
$newContent = $newContent -replace '(?m)^(version\s*=\s*")[^"]*(".*)$', "`$1$newVersion`$2"

Set-Content -Path $cargoPath -Value $newContent -NoNewline
Write-Host "Updated $cargoPath" -ForegroundColor Green

# Git 操作
if ($Push) {
    git add $cargoPath
    git commit -m "chore: auto update edition=$newEdition, version=$newVersion"
    git push
    Write-Host "Pushed to remote" -ForegroundColor Green
}
