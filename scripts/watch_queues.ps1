param([int]$Seconds = 30)
# Continuous display of queue stats using the management API (preferred)
while ($true) {
  Clear-Host
  Write-Host "RabbitMQ queues (Management API) `nLast updated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
  python src/monitor.py
  Start-Sleep -Seconds $Seconds
}
