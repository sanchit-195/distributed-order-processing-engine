# Comprehensive End-to-End Choreographed Saga Test for Phase 2
$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "STARTING DISTRIBUTED EVENT-DRIVEN ORDER ENGINE (PHASE 2)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Clean existing process on ports 8081, 8082, 8083 if any
8081, 8082, 8083 | ForEach-Object {
    $port = $_
    $conns = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
    if ($conns) {
        $pids = $conns | Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($p in $pids) {
            Write-Host "Stopping existing process $p on port $port" -ForegroundColor Yellow
            Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
        }
    }
}

# 2. Start services in background
Write-Host "`n[1/4] Starting microservices..." -ForegroundColor Yellow

$orderLogOut = "$PSScriptRoot\order-service.out.log"
$orderLogErr = "$PSScriptRoot\order-service.err.log"
$inventoryLogOut = "$PSScriptRoot\inventory-service.out.log"
$inventoryLogErr = "$PSScriptRoot\inventory-service.err.log"
$paymentLogOut = "$PSScriptRoot\payment-service.out.log"
$paymentLogErr = "$PSScriptRoot\payment-service.err.log"

Remove-Item $orderLogOut, $orderLogErr, $inventoryLogOut, $inventoryLogErr, $paymentLogOut, $paymentLogErr -ErrorAction SilentlyContinue

$orderProc = Start-Process java -ArgumentList "-jar", "order-service/target/order-service-1.0.0.jar" -PassThru -RedirectStandardOutput $orderLogOut -RedirectStandardError $orderLogErr
$inventoryProc = Start-Process java -ArgumentList "-jar", "inventory-service/target/inventory-service-1.0.0.jar" -PassThru -RedirectStandardOutput $inventoryLogOut -RedirectStandardError $inventoryLogErr
$paymentProc = Start-Process java -ArgumentList "-jar", "payment-service/target/payment-service-1.0.0.jar", "--payment.failing-product-ids=2" -PassThru -RedirectStandardOutput $paymentLogOut -RedirectStandardError $paymentLogErr

Write-Host "order-service PID: $($orderProc.Id) (port 8081)"
Write-Host "inventory-service PID: $($inventoryProc.Id) (port 8082)"
Write-Host "payment-service PID: $($paymentProc.Id) (port 8083, failing-product-ids=2)"

try {
    # 3. Wait for services to be reachable
    Write-Host "`n[2/4] Waiting for services to come online..." -ForegroundColor Yellow
    $timeoutSeconds = 120
    $start = Get-Date

    function Wait-Port($port, $name) {
        while ($true) {
            if ((Get-Date) - $start -gt [TimeSpan]::FromSeconds($timeoutSeconds)) {
                throw "Timeout waiting for $name on port $port"
            }
            $conn = Test-NetConnection -ComputerName localhost -Port $port -WarningAction SilentlyContinue
            if ($conn.TcpTestSucceeded) {
                Write-Host "  -> $name is listening on port $port" -ForegroundColor Green
                break
            }
            Start-Sleep -Seconds 1
        }
    }

    function Get-ProductStock($productId) {
        $val = (psql -U postgres -d inventorydb -t -A -c "SELECT stock FROM products WHERE id=$productId;").Trim()
        return [int]($val -split '\r?\n')[0]
    }

    Wait-Port 8081 "order-service"
    Wait-Port 8082 "inventory-service"
    Wait-Port 8083 "payment-service"

    # Give Kafka consumers a moment to complete initial partition assignment
    Start-Sleep -Seconds 5

    Write-Host "`n[3/4] Running Saga Test Scenarios..." -ForegroundColor Yellow

    # ---------------------------------------------------------
    # SCENARIO 1: Happy Path
    # ---------------------------------------------------------
    Write-Host "`n>>> SCENARIO 1: Happy Path (Product 1: Wireless Mouse)" -ForegroundColor Magenta
    $p1StockBefore = Get-ProductStock 1
    Write-Host "Product 1 stock before order: $p1StockBefore"

    $orderBody = @{
        productId = 1
        quantity = 2
        unitPrice = 25.99
    } | ConvertTo-Json

    $res = Invoke-RestMethod -Uri "http://localhost:8081/orders" -Method POST -Body $orderBody -ContentType "application/json"
    $orderId = $res.id
    Write-Host "Placed Order #$orderId, initial status: $($res.status)"

    # Poll for confirmation
    $confirmed = $false
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Seconds 1
        $order = Invoke-RestMethod -Uri "http://localhost:8081/orders/$orderId" -Method GET
        Write-Host "  Poll Order #$orderId status: $($order.status)"
        if ($order.status -eq "CONFIRMED") {
            $confirmed = $true
            break
        }
    }

    if (-not $confirmed) {
        throw "Scenario 1 failed: Order #$orderId did not reach CONFIRMED within timeout"
    }
    Write-Host "  [PASS] Order #$orderId reached CONFIRMED!" -ForegroundColor Green

    $p1StockAfter = Get-ProductStock 1
    Write-Host "Product 1 stock after order: $p1StockAfter (expected $($p1StockBefore - 2))"
    if ($p1StockAfter -ne ($p1StockBefore - 2)) {
        throw "Stock was not deducted correctly for Product 1"
    }

    # ---------------------------------------------------------
    # SCENARIO 2: Out of Stock Failure
    # ---------------------------------------------------------
    Write-Host "`n>>> SCENARIO 2: Insufficient Stock Rejection" -ForegroundColor Magenta
    $orderBody = @{
        productId = 1
        quantity = 999
        unitPrice = 25.99
    } | ConvertTo-Json

    $res = Invoke-RestMethod -Uri "http://localhost:8081/orders" -Method POST -Body $orderBody -ContentType "application/json"
    $orderId = $res.id
    Write-Host "Placed Order #$orderId for 999 units, initial status: $($res.status)"

    $failed = $false
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Seconds 1
        $order = Invoke-RestMethod -Uri "http://localhost:8081/orders/$orderId" -Method GET
        Write-Host "  Poll Order #$orderId status: $($order.status)"
        if ($order.status -eq "FAILED") {
            $failed = $true
            Write-Host "  Failure Reason: $($order.failureReason)" -ForegroundColor Yellow
            break
        }
    }

    if (-not $failed) {
        throw "Scenario 2 failed: Order #$orderId did not fail for insufficient stock"
    }
    Write-Host "  [PASS] Order #$orderId correctly marked FAILED due to insufficient stock!" -ForegroundColor Green

    # ---------------------------------------------------------
    # SCENARIO 3: Forced Failure & Compensable Rollback (Stock Restored)
    # ---------------------------------------------------------
    Write-Host "`n>>> SCENARIO 3: Payment Failure & Compensating Rollback (Product 2: Mechanical Keyboard)" -ForegroundColor Magenta
    $p2StockBefore = Get-ProductStock 2
    Write-Host "Product 2 stock before order: $p2StockBefore"

    $orderBody = @{
        productId = 2
        quantity = 1
        unitPrice = 89.50
    } | ConvertTo-Json

    $res = Invoke-RestMethod -Uri "http://localhost:8081/orders" -Method POST -Body $orderBody -ContentType "application/json"
    $orderId = $res.id
    Write-Host "Placed Order #$orderId for failing Product #2, initial status: $($res.status)"

    $rollbackConfirmed = $false
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Seconds 1
        $order = Invoke-RestMethod -Uri "http://localhost:8081/orders/$orderId" -Method GET
        Write-Host "  Poll Order #$orderId status: $($order.status)"
        if ($order.status -eq "FAILED") {
            $rollbackConfirmed = $true
            Write-Host "  Failure Reason: $($order.failureReason)" -ForegroundColor Yellow
            break
        }
    }

    if (-not $rollbackConfirmed) {
        throw "Scenario 3 failed: Order #$orderId did not fail as expected"
    }

    # Verify inventory was compensated (stock restored)
    Start-Sleep -Seconds 2
    $p2StockAfter = Get-ProductStock 2
    Write-Host "Product 2 stock after rollback: $p2StockAfter (initial was $p2StockBefore)"

    $reservationRaw = (psql -U postgres -d inventorydb -t -A -c "SELECT status FROM stock_reservations WHERE order_id=$orderId;").Trim()
    $reservationStatus = ($reservationRaw -split '\r?\n')[0]
    Write-Host "Stock Reservation status in inventorydb: $reservationStatus"

    if ($p2StockAfter -ne $p2StockBefore) {
        throw "Compensation failed: Stock was not restored! Expected $p2StockBefore but found $p2StockAfter"
    }

    if ($reservationStatus -ne "RELEASED") {
        throw "Reservation status expected RELEASED but was $reservationStatus"
    }

    Write-Host "  [PASS] Compensating rollback successful! Stock was fully restored and reservation marked RELEASED!" -ForegroundColor Green

    Write-Host "`n==========================================================" -ForegroundColor Green
    Write-Host "ALL SAGA TEST SCENARIOS PASSED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Green

} finally {
    Write-Host "`n[4/4] Shutting down services..." -ForegroundColor Yellow
    Stop-Process -Id $orderProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $inventoryProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $paymentProc.Id -Force -ErrorAction SilentlyContinue
    Write-Host "Services stopped cleanly." -ForegroundColor Green
}
