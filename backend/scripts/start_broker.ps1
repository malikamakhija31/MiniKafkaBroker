# Restart ONE broker (use it after you killed a broker, to watch it rejoin the cluster).
# Usage:  .\scripts\start_broker.ps1 -Id broker-1 -Port 8000
param(
    [Parameter(Mandatory = $true)][string]$Id,
    [Parameter(Mandatory = $true)][int]$Port
)
$backendPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$command = "Set-Location -LiteralPath '$backendPath'; `$env:BROKER_NODE_ID='$Id'; `$env:BROKER_DATA_DIR='data/$Id'; `$env:BROKER_CLUSTER_STATE='data/cluster_state.json'; `$env:BROKER_MIN_ISR='2'; `$env:BROKER_ENABLE_FAULTS='1'; python -m uvicorn app.main:app --host 127.0.0.1 --port $Port --ws wsproto"
Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoExit", "-Command", $command)
Write-Host "Started $Id on port $Port" -ForegroundColor Green
