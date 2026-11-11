$ErrorActionPreference = "Stop"
$backendPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$nodes = @(
    @{ Id = "broker-1"; Port = 8000 },
    @{ Id = "broker-2"; Port = 8001 },
    @{ Id = "broker-3"; Port = 8002 }
)

foreach ($node in $nodes) {
    $id = $node.Id
    $port = $node.Port
    $dataDir = "data/$id"
    $command = "Set-Location -LiteralPath '$backendPath'; `$env:BROKER_NODE_ID='$id'; `$env:BROKER_DATA_DIR='$dataDir'; `$env:BROKER_CLUSTER_STATE='data/cluster_state.json'; python -m uvicorn app.main:app --host 127.0.0.1 --port $port"
    Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoExit", "-Command", $command)
    Start-Sleep -Milliseconds 700
}

Write-Host "Started MiniKafkaBroker cluster launch windows." -ForegroundColor Green
Write-Host "Broker 1: http://127.0.0.1:8000/docs"
Write-Host "Broker 2: http://127.0.0.1:8001/docs"
Write-Host "Broker 3: http://127.0.0.1:8002/docs"
Write-Host "Cluster status: http://127.0.0.1:8000/api/v1/cluster"
