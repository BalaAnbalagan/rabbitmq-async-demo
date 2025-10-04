# RabbitMQ Queue and Exchange Cleanup Script
# Deletes queues and exchanges to reset the environment

param(
    [switch]$All,
    [switch]$Exchanges,
    [switch]$Help,
    [string[]]$Queue
)

# Find rabbitmqctl.bat
$rabbitmqctl = Get-ChildItem "C:\Program Files\RabbitMQ Server\" -Filter "rabbitmqctl.bat" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName

if (-not $rabbitmqctl) {
    Write-Host "[ERROR] rabbitmqctl.bat not found in C:\Program Files\RabbitMQ Server\" -ForegroundColor Red
    Write-Host "Please ensure RabbitMQ is installed or update the path in the script." -ForegroundColor Yellow
    exit 1
}

Write-Host "[INFO] Using RabbitMQ at: $rabbitmqctl" -ForegroundColor Gray
Write-Host ""

function Show-Help {
    Write-Host "RabbitMQ Queue and Exchange Cleanup Script" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\cleanup_queues.ps1 -All                      # Delete ALL queues and exchanges"
    Write-Host "  .\cleanup_queues.ps1 -Exchanges                # Delete only unused exchanges"
    Write-Host "  .\cleanup_queues.ps1 -Queue work.queue         # Delete specific queue"
    Write-Host "  .\cleanup_queues.ps1 -Queue q1,q2,q3           # Delete multiple queues"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\cleanup_queues.ps1 -All                      # Complete reset (queues + exchanges)"
    Write-Host "  .\cleanup_queues.ps1 -Exchanges                # Clean up old exchanges only"
    Write-Host "  .\cleanup_queues.ps1 -Queue work.queue         # Delete work.queue only"
    Write-Host "  .\cleanup_queues.ps1 -Queue work.queue,audit.queue  # Delete both"
    Write-Host ""
}

if ($Help -or (-not $All -and -not $Queue -and -not $Exchanges)) {
    Show-Help
    exit
}

Write-Host ""
Write-Host "RabbitMQ Queue Cleanup" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Get list of existing queues
Write-Host "[*] Listing current queues..." -ForegroundColor Yellow
$queueList = & $rabbitmqctl list_queues name messages 2>&1 | Select-Object -Skip 2
Write-Host ($queueList -join "`n")
Write-Host ""

# Parse queue names from output
$existingQueues = @()
foreach ($line in $queueList) {
    if ($line -match "^(\S+)\s+\d+") {
        $existingQueues += $matches[1]
    }
}

if ($All) {
    if ($existingQueues.Count -eq 0) {
        Write-Host "[INFO] No queues found to delete." -ForegroundColor Yellow
    } else {
        Write-Host "[*] Deleting ALL queues ($($existingQueues.Count) found)..." -ForegroundColor Yellow
        foreach ($queue in $existingQueues) {
            $result = & $rabbitmqctl delete_queue $queue 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  [OK] Deleted: $queue" -ForegroundColor Green
            } else {
                Write-Host "  [FAIL] Could not delete: $queue" -ForegroundColor Red
            }
        }
    }
} elseif ($Queue) {
    Write-Host "[*] Deleting specified queues..." -ForegroundColor Yellow
    foreach ($q in $Queue) {
        $result = & $rabbitmqctl delete_queue $q 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK] Deleted: $q" -ForegroundColor Green
        } else {
            Write-Host "  [FAIL] Not found or error: $q" -ForegroundColor Red
        }
    }
}

# Clean up exchanges if requested
if ($All -or $Exchanges) {
    Write-Host ""
    Write-Host "[*] Cleaning up custom exchanges..." -ForegroundColor Yellow
    Write-Host "  [INFO] Waiting for bindings to clear..." -ForegroundColor Cyan
    Start-Sleep -Seconds 1

    # List all exchanges
    $exchangeList = & $rabbitmqctl list_exchanges name type 2>&1 | Select-Object -Skip 2

    # Built-in exchanges to preserve (never delete these)
    $builtInExchanges = @("", "amq.direct", "amq.fanout", "amq.headers", "amq.match", "amq.rabbitmq.trace", "amq.topic")

    $deletedCount = 0
    $failedCount = 0
    foreach ($line in $exchangeList) {
        if ($line -match "^(\S+)\s+") {
            $exchangeName = $matches[1]

            # Skip built-in exchanges
            if ($builtInExchanges -notcontains $exchangeName) {
                # Try with --if-unused flag first (safer - only deletes if no bindings)
                $result = & $rabbitmqctl delete_exchange $exchangeName --if-unused 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  [OK] Deleted exchange: $exchangeName" -ForegroundColor Green
                    $deletedCount++
                } else {
                    # If --if-unused fails, try force delete
                    $result = & $rabbitmqctl delete_exchange $exchangeName 2>&1
                    if ($LASTEXITCODE -eq 0) {
                        Write-Host "  [OK] Deleted exchange: $exchangeName (forced)" -ForegroundColor Green
                        $deletedCount++
                    } else {
                        Write-Host "  [FAIL] Could not delete exchange: $exchangeName" -ForegroundColor Red
                        Write-Host "        Reason: $($result | Select-Object -First 1)" -ForegroundColor DarkGray
                        $failedCount++
                    }
                }
            }
        }
    }

    if ($deletedCount -eq 0 -and $failedCount -eq 0) {
        Write-Host "  [INFO] No custom exchanges found to delete" -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Host "[*] Remaining exchanges:" -ForegroundColor Yellow
    & $rabbitmqctl list_exchanges name type
}

Write-Host ""
Write-Host "[*] Final queue list:" -ForegroundColor Yellow
& $rabbitmqctl list_queues name messages

Write-Host ""
Write-Host "[SUCCESS] Cleanup complete!" -ForegroundColor Green
Write-Host ""
