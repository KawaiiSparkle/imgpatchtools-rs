#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Cross-platform 7-Zip with Zstandard support installer
.DESCRIPTION
    Downloads and installs 7-Zip with Zstandard codec support.
    Works on Windows, Linux, and macOS.
#>

param(
    [string]$InstallDir = "",
    [switch]$Force,
    [switch]$AddToPath
)

$ErrorActionPreference = "Stop"

# Detect OS
$IsWindows = $PSVersionTable.Platform -eq "Win32NT" -or $env:OS -eq "Windows_NT"
$IsLinux = $PSVersionTable.Platform -eq "Unix" -and (Test-Path "/proc/version")
$IsMacOS = $PSVersionTable.Platform -eq "Unix" -and (Test-Path "/System/Library")

function Write-Info { param([string]$Message) Write-Host "[INFO] $Message" -ForegroundColor Cyan }
function Write-Success { param([string]$Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Warn { param([string]$Message) Write-Host "[WARN] $Message" -ForegroundColor Yellow }

function Get-Latest7zUrl {
    # Try to get latest release from GitHub
    try {
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/ip7z/7zip/releases/latest" -TimeoutSec 10
        $asset = $release.assets | Where-Object { $_.name -match "7z\d+-x64\.exe$" } | Select-Object -First 1
        if ($asset) { return $asset.browser_download_url }
    } catch { }
    
    # Fallback to known version
    return "https://7-zip.org/a/7z2409-x64.exe"
}

function Install-7zWindows {
    param([string]$DestDir)
    
    $7zExe = Join-Path $DestDir "7z.exe"
    
    if ((Test-Path $7zExe) -and -not $Force) {
        Write-Warn "7-Zip already exists at $DestDir. Use -Force to reinstall."
        return $7zExe
    }
    
    $tempDir = [System.IO.Path]::GetTempPath()
    $installer = Join-Path $tempDir "7z-installer.exe"
    
    $url = Get-Latest7zUrl
    Write-Info "Downloading 7-Zip from $url..."
    
    try {
        Invoke-WebRequest -Uri $url -OutFile $installer -UseBasicParsing
    } catch {
        # Fallback to direct download
        Write-Warn "GitHub API failed, trying direct download..."
        Invoke-WebRequest -Uri "https://7-zip.org/a/7z2409-x64.exe" -OutFile $installer -UseBasicParsing
    }
    
    Write-Info "Installing 7-Zip to $DestDir..."
    
    # Silent install
    $process = Start-Process -FilePath $installer -ArgumentList "/S","/D=$DestDir" -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "7-Zip installation failed with exit code $($process.ExitCode)"
    }
    
    Remove-Item $installer -Force -ErrorAction SilentlyContinue
    
    if (-not (Test-Path $7zExe)) {
        throw "7-Zip installation failed - 7z.exe not found"
    }
    
    Write-Success "7-Zip installed to $DestDir"
    return $7zExe
}

function Install-7zUnix {
    param([string]$DestDir)
    
    $7zExe = Join-Path $DestDir "7zz"
    
    # Check if 7z is already available
    $existing7z = Get-Command "7z" -ErrorAction SilentlyContinue
    if ($existing7z -and -not $Force) {
        Write-Warn "7-Zip already available at $($existing7z.Source). Use -Force to reinstall."
        return $existing7z.Source
    }
    
    if ((Test-Path $7zExe) -and -not $Force) {
        Write-Warn "7-Zip already exists at $DestDir. Use -Force to reinstall."
        return $7zExe
    }
    
    # Create directory
    New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
    
    # Determine platform
    $arch = if ([Environment]::Is64BitOperatingSystem) { "x64" } else { "x86" }
    
    if ($IsLinux) {
        $platform = "linux"
        $archiveName = "7z2409-linux-$arch.tar.xz"
    } elseif ($IsMacOS) {
        $platform = "mac"
        $archiveName = "7z2409-mac.tar.xz"
    } else {
        $platform = "linux"
        $archiveName = "7z2409-linux-x64.tar.xz"
    }
    
    $url = "https://7-zip.org/a/$archiveName"
    $tempDir = [System.IO.Path]::GetTempPath()
    $archive = Join-Path $tempDir $archiveName
    
    Write-Info "Downloading 7-Zip for $platform..."
    Invoke-WebRequest -Uri $url -OutFile $archive -UseBasicParsing
    
    Write-Info "Extracting 7-Zip..."
    
    # Try using system tar first
    try {
        & tar -xf $archive -C $tempDir 2>$null
    } catch {
        # Fallback: try to use busybox or other tools
        if (Get-Command "busybox" -ErrorAction SilentlyContinue) {
            & busybox tar -xf $archive -C $tempDir
        } else {
            throw "Cannot extract archive. Please install tar."
        }
    }
    
    # Find extracted directory
    $extractedDir = Get-ChildItem $tempDir -Directory -Filter "7z*" | Select-Object -First 1
    if (-not $extractedDir) {
        throw "Failed to find extracted 7-Zip directory"
    }
    
    # Copy binaries
    $source7zz = Join-Path $extractedDir.FullName "7zz"
    $source7zzs = Join-Path $extractedDir.FullName "7zzs"
    
    if (Test-Path $source7zz) {
        Copy-Item $source7zz $7zExe -Force
        chmod +x $7zExe
    } elseif (Test-Path $source7zzs) {
        Copy-Item $source7zzs $7zExe -Force
        chmod +x $7zExe
    } else {
        throw "7zz binary not found in extracted archive"
    }
    
    # Cleanup
    Remove-Item $archive -Force -ErrorAction SilentlyContinue
    Remove-Item $extractedDir.FullName -Recurse -Force -ErrorAction SilentlyContinue
    
    Write-Success "7-Zip installed to $DestDir"
    return $7zExe
}

function Add-ToPath {
    param([string]$Dir)
    
    Write-Info "Adding $Dir to PATH..."
    
    if ($IsWindows) {
        # Add to user PATH
        $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($userPath -notlike "*$Dir*") {
            [Environment]::SetEnvironmentVariable("Path", "$userPath;$Dir", "User")
            Write-Success "Added to user PATH. Restart your terminal to use 7z."
        } else {
            Write-Warn "Already in PATH"
        }
    } else {
        # Add to shell profile
        $profileFile = if (Test-Path ~/.zshrc) { "~/.zshrc" } else { "~/.bashrc" }
        $profilePath = (Resolve-Path $profileFile).Path
        
        $exportLine = "export PATH=`"`$PATH:$Dir`""
        $content = Get-Content $profilePath -Raw -ErrorAction SilentlyContinue
        
        if ($content -notlike "*$Dir*") {
            Add-Content $profilePath "`n# 7-Zip`n$exportLine"
            Write-Success "Added to $profileFile. Run 'source $profileFile' to use 7z."
        } else {
            Write-Warn "Already in PATH"
        }
        
        # Create symlink for convenience
        $localBin = "$env:HOME/.local/bin"
        if (-not (Test-Path $localBin)) {
            New-Item -ItemType Directory -Force -Path $localBin | Out-Null
        }
        
        $symlink = Join-Path $localBin "7z"
        if (-not (Test-Path $symlink)) {
            ln -s (Join-Path $Dir "7zz") $symlink 2>$null
        }
    }
}

function Test-7zZstd {
    param([string]$7zPath)
    
    Write-Info "Testing 7-Zip with Zstandard support..."
    
    $output = & $7zPath i 2>&1 | Select-String -Pattern "zstd|Zstandard" -CaseSensitive:$false
    
    if ($output) {
        Write-Success "Zstandard support detected!"
        return $true
    } else {
        Write-Warn "Zstandard support may not be available in this build"
        return $false
    }
}

# Main installation logic
Write-Info "Detecting platform..."

if ($IsWindows) {
    $platform = "Windows"
    $defaultDir = "${env:ProgramFiles}\7-Zip"
} elseif ($IsLinux) {
    $platform = "Linux"
    $defaultDir = "$env:HOME/.local/7z"
} elseif ($IsMacOS) {
    $platform = "macOS"
    $defaultDir = "$env:HOME/.local/7z"
} else {
    Write-Warn "Unknown platform, assuming Linux"
    $platform = "Unknown"
    $defaultDir = "$env:HOME/.local/7z"
}

if (-not $InstallDir) {
    $InstallDir = $defaultDir
}

Write-Info "Platform: $platform"
Write-Info "Install directory: $InstallDir"

# Install
if ($IsWindows) {
    $7zPath = Install-7zWindows -DestDir $InstallDir
} else {
    $7zPath = Install-7zUnix -DestDir $InstallDir
}

# Test
Test-7zZstd -7zPath $7zPath

# Add to PATH if requested
if ($AddToPath) {
    Add-ToPath -Dir $InstallDir
}

Write-Success "Installation complete!"
Write-Host "`nUsage:"
if ($IsWindows) {
    Write-Host "  & '$7zPath' x archive.zst"
} else {
    Write-Host "  $7zPath x archive.zst"
}
Write-Host "`nOr add to your PATH and use: 7z x archive.zst"
