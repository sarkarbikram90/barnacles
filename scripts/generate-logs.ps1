# PowerShell log generator for Barnacles Agent

$LogDir = "./data/demo"
$AppLog = "$LogDir/app.log"
$AccessLog = "$LogDir/access.log"

if (!(Test-Path -Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

if (!(Test-Path -Path $AppLog)) {
    New-Item -ItemType File -Path $AppLog -Force | Out-Null
}
if (!(Test-Path -Path $AccessLog)) {
    New-Item -ItemType File -Path $AccessLog -Force | Out-Null
}

Write-Host "Log generator started writing to $LogDir (Press Ctrl+C to stop)" -ForegroundColor Cyan

$Endpoints = @("/api/v1/users", "/api/v1/checkout", "/healthz", "/api/v1/products", "/auth/login")
$Statuses = @(200, 200, 200, 201, 204, 400, 401, 404, 500, 502)
$Levels = @("INFO", "INFO", "INFO", "WARN", "ERROR")

while ($true) {
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $lvl = $Levels[(Get-Random -Maximum $Levels.Length)]
    $ep = $Endpoints[(Get-Random -Maximum $Endpoints.Length)]
    $status = $Statuses[(Get-Random -Maximum $Statuses.Length)]
    $latency = (Get-Random -Minimum 10 -Maximum 500)

    # 1. Plain text log
    if ($lvl -eq "ERROR") {
        $line = "$ts [$lvl] failed to process request on $ep : downstream service timed out (status=$status latency=${latency}ms)"
    } elseif ($lvl -eq "WARN") {
        $line = "$ts [$lvl] high memory watermark detected in worker pool: 84% utilized"
    } else {
        $line = "$ts [$lvl] processed request on $ep successfully (status=$status latency=${latency}ms)"
    }
    Add-Content -Path $AppLog -Value $line

    # 2. JSON log
    $jsonObj = @{
        timestamp = $ts
        level = $lvl
        endpoint = $ep
        status = $status
        latency_ms = $latency
        ip = "192.168.1.$((Get-Random -Minimum 1 -Maximum 254))"
        message = "HTTP $status $ep in ${latency}ms"
    }
    $jsonLine = $jsonObj | ConvertTo-Json -Compress
    Add-Content -Path $AccessLog -Value $jsonLine

    Start-Sleep -Milliseconds 500
}
