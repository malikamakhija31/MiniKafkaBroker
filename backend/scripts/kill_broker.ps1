# Hard-kill ONE broker by port (simulates a crash / power loss, no graceful shutdown).
# Usage:  .\scripts\kill_broker.ps1 -Port 8000
param([Parameter(Mandatory = $true)][int]$Port)
$connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if (-not $connections) { Write-Host "Nothing is listening on port $Port" -ForegroundColor Yellow; exit }
$connections | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
Write-Host "Killed the broker on port $Port" -ForegroundColor Red
