# run_100k_chaos_benchmark.ps1
# Automates the 100K Distributed Job Chaos Benchmark with live failure injection.

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  100K Distributed Job Chaos Benchmark (10-Minute Live Run)" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Ensure cluster nodes are running
Write-Host "`n[1/5] Verifying cluster status..." -ForegroundColor Yellow
docker compose ps

# 2. Run ingestion via SchedulerBenchmark
Write-Host "`n[2/5] Compiling and starting 100k job batch ingestion..." -ForegroundColor Yellow
$env:JAVA_HOME = "C:\Users\Asus\.jdks\temurin-21"
./mvnw test-compile -DskipTests

Write-Host "`n[3/5] Ingesting 100,000 jobs..." -ForegroundColor Yellow
$cp = "target/classes;target/test-classes;" + (Get-ChildItem -Recurse -Filter *.jar C:\Users\Asus\.m2\repository | ForEach-Object { $_.FullName } | Join-String -Separator ";")
# Use java to run SchedulerBenchmark main method
java -cp "target/classes;target/test-classes;$((./mvnw dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) 2>$null)" com.movesync.scheduler.perf.SchedulerBenchmark

$startTime = [System.Diagnostics.Stopwatch]::StartNew()
Write-Host "`n[4/5] Chaos Orchestration Started at $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Green

$killedNode2 = $false
$pausedNode3 = $false
$unpausedNode3 = $false
$restartedNode2 = $false

while ($true) {
    $elapsedSec = [math]::Floor($startTime.Elapsed.TotalSeconds)
    $min = [math]::Floor($elapsedSec / 60)
    $sec = $elapsedSec % 60
    $timeStr = "{0:D2}:{1:D2}" -f $min, $sec

    # Query current progress
    $progressQuery = "SELECT (SELECT count(*) FROM jobs WHERE status = 'COMPLETED' AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%')) AS completed, (SELECT count(*) FROM jobs WHERE status = 'ACTIVE' AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%')) AS active;"
    $res = docker compose exec postgres psql -U scheduler -d scheduler_db -t -A -c $progressQuery 2>$null
    if ($res) {
        $parts = $res.Trim().Split('|')
        $completed = [int]$parts[0]
        $active = [int]$parts[1]
        Write-Host "[$timeStr] Completed: $completed / 100,000 | Active: $active" -ForegroundColor DarkCyan
        
        if ($completed -ge 100000 -and $elapsedSec -ge 450) {
            Write-Host "`n[SUCCESS] All 100,000 jobs completed!" -ForegroundColor Green
            break
        }
    }

    # Chaos Injection Events
    # Minute 3:00 (180s) — kill -9 scheduler-node2
    if ($elapsedSec -ge 180 -and -not $killedNode2) {
        Write-Host "`n>>> [CHAOS] Minute 3:00 — Executing 'docker kill -s 9 scheduler-scheduler-node2-1'..." -ForegroundColor Red
        docker kill -s 9 scheduler-scheduler-node2-1
        $killedNode2 = $true
        Write-Host ">>> scheduler-node2 killed! Surviving nodes (node1, node3) will rebalance.`n" -ForegroundColor Red
    }

    # Minute 5:00 (300s) — docker pause scheduler-node3 (35s freeze)
    if ($elapsedSec -ge 300 -and -not $pausedNode3) {
        Write-Host "`n>>> [CHAOS] Minute 5:00 — Executing 'docker pause scheduler-scheduler-node3-1' (simulating 40s GC pause)..." -ForegroundColor Yellow
        docker pause scheduler-scheduler-node3-1
        $pausedNode3 = $true
        Write-Host ">>> scheduler-node3 paused! Its leases will expire in Postgres, node1 will take over.`n" -ForegroundColor Yellow
    }

    # Minute 5:40 (340s) — docker unpause scheduler-node3
    if ($elapsedSec -ge 340 -and -not $unpausedNode3) {
        Write-Host "`n>>> [CHAOS] Minute 5:40 — Executing 'docker unpause scheduler-scheduler-node3-1'..." -ForegroundColor Magenta
        docker unpause scheduler-scheduler-node3-1
        $unpausedNode3 = $true
        Write-Host ">>> scheduler-node3 resumed! Its stale writes must be rejected via fencing token.`n" -ForegroundColor Magenta
    }

    # Minute 7:00 (420s) — restart scheduler-node2
    if ($elapsedSec -ge 420 -and -not $restartedNode2) {
        Write-Host "`n>>> [CHAOS] Minute 7:00 — Restarting scheduler-node2 ('docker compose start scheduler-node2')..." -ForegroundColor Cyan
        docker compose start scheduler-node2
        $restartedNode2 = $true
        Write-Host ">>> scheduler-node2 restarted! 3 nodes rebalancing to finish remaining workload.`n" -ForegroundColor Cyan
    }

    if ($elapsedSec -ge 660) {
        Write-Host "`n[TIMEOUT] 11 minutes elapsed. Proceeding to final verification." -ForegroundColor Yellow
        break
    }

    Start-Sleep -Seconds 15
}

# 5. Final Verification & Metrics
Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "  FINAL BENCHMARK VERIFICATION" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green

Write-Host "`n--- 1. Partition Leases Distribution & Fencing Tokens ---" -ForegroundColor Yellow
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT node_id, count(*) AS partitions, min(fencing_token) AS min_token, max(fencing_token) AS max_token FROM partition_leases GROUP BY node_id ORDER BY node_id;"

Write-Host "`n--- 2. Duplicate Execution Check ---" -ForegroundColor Yellow
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT count(*) AS duplicate_executions FROM (SELECT job_id FROM job_executions WHERE status = 'COMPLETED' AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%') GROUP BY job_id HAVING count(*) > 1) d;"

Write-Host "`n--- 3. Total Completed Jobs ---" -ForegroundColor Yellow
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT status, count(*) FROM jobs WHERE tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%') GROUP BY status;"

Write-Host "`n--- 4. Fire Lag Statistics (p50, p90, p95, p99, min, max) ---" -ForegroundColor Yellow
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT count(*) AS total_measured, round(cast(percentile_cont(0.50) within group (order by fire_lag_ms) as numeric), 2) AS p50_ms, round(cast(percentile_cont(0.90) within group (order by fire_lag_ms) as numeric), 2) AS p90_ms, round(cast(percentile_cont(0.95) within group (order by fire_lag_ms) as numeric), 2) AS p95_ms, round(cast(percentile_cont(0.99) within group (order by fire_lag_ms) as numeric), 2) AS p99_ms, min(fire_lag_ms) AS min_lag_ms, max(fire_lag_ms) AS max_lag_ms FROM job_executions WHERE status = 'COMPLETED' AND fire_lag_ms IS NOT NULL AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');"

Write-Host "`n--- 5. Fencing Rejections in Node-3 Logs ---" -ForegroundColor Yellow
docker compose logs scheduler-node3 | Select-String "FENCING_REJECTED" | Select-Object -First 5
