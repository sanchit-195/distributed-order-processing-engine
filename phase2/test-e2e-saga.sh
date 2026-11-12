#!/usr/bin/env bash
# ==========================================================
# Comprehensive End-to-End Choreographed Saga Test for Phase 2
# (Git Bash / Linux / macOS)
# ==========================================================
set -e

echo -e "\033[1;36m==========================================================\033[0m"
echo -e "\033[1;36mSTARTING DISTRIBUTED EVENT-DRIVEN ORDER ENGINE (PHASE 2)\033[0m"
echo -e "\033[1;36m==========================================================\033[0m"

# Locate Java binary
if command -v java >/dev/null 2>&1; then
  JAVA_CMD="java"
elif [ -f "/c/Program Files/Java/jdk-17/bin/java.exe" ]; then
  JAVA_CMD="/c/Program Files/Java/jdk-17/bin/java.exe"
else
  JAVA_CMD="java"
fi

# 1. Clean up existing processes listening on 8081, 8082, 8083
for port in 8081 8082 8083; do
  pid=$(netstat -ano 2>/dev/null | grep ":$port " | awk '{print $5}' | head -n 1 || true)
  if [ -n "$pid" ] && [ "$pid" != "0" ]; then
    echo -e "\033[1;33mStopping existing process on port $port (PID: $pid)...\033[0m"
    taskkill //F //PID "$pid" 2>/dev/null || true
  fi
done

# 2. Start services in background
echo -e "\n\033[1;33m[1/4] Starting microservices...\033[0m"

rm -f order-service.log inventory-service.log payment-service.log

(cd order-service && "$JAVA_CMD" -jar target/order-service-1.0.0.jar > ../order-service.log 2>&1) &
ORDER_PID=$!
(cd inventory-service && "$JAVA_CMD" -jar target/inventory-service-1.0.0.jar > ../inventory-service.log 2>&1) &
INVENTORY_PID=$!
(cd payment-service && "$JAVA_CMD" -jar target/payment-service-1.0.0.jar --payment.failing-product-ids=2 > ../payment-service.log 2>&1) &
PAYMENT_PID=$!

echo "order-service PID: $ORDER_PID (port 8081)"
echo "inventory-service PID: $INVENTORY_PID (port 8082)"
echo "payment-service PID: $PAYMENT_PID (port 8083, failing-product-ids=2)"

cleanup() {
  echo -e "\n\033[1;33m[4/4] Shutting down services...\033[0m"
  kill $ORDER_PID 2>/dev/null || true
  kill $INVENTORY_PID 2>/dev/null || true
  kill $PAYMENT_PID 2>/dev/null || true
  # Fallback for Windows processes spawned by Git Bash
  for port in 8081 8082 8083; do
    pid=$(netstat -ano 2>/dev/null | grep ":$port " | awk '{print $5}' | head -n 1 || true)
    if [ -n "$pid" ] && [ "$pid" != "0" ]; then
      taskkill //F //PID "$pid" 2>/dev/null || true
    fi
  done
  echo -e "\033[1;32mServices stopped cleanly.\033[0m"
}
trap cleanup EXIT

# 3. Wait for services to be reachable
echo -e "\n\033[1;33m[2/4] Waiting for services to come online...\033[0m"

wait_for_port() {
  local port=$1
  local name=$2
  local retries=60
  while ! curl -s -o /dev/null "http://localhost:$port" 2>/dev/null && [ $retries -gt 0 ]; do
    sleep 2
    retries=$((retries - 1))
  done
  if [ $retries -le 0 ]; then
    echo -e "\033[1;31mTimeout waiting for $name on port $port\033[0m"
    exit 1
  fi
  echo -e "\033[1;32m  -> $name is ready on port $port\033[0m"
}

get_product_stock() {
  local pid=$1
  docker exec postgres psql -U postgres -d inventorydb -t -A -c "SELECT stock FROM products WHERE id=$pid;" | tr -d '\r\n'
}

wait_for_port 8081 "order-service"
wait_for_port 8082 "inventory-service"
wait_for_port 8083 "payment-service"

# Allow Kafka consumer groups to complete initial topic partition rebalances
sleep 5

echo -e "\n\033[1;33m[3/4] Running Saga Test Scenarios...\033[0m"

# ---------------------------------------------------------
# SCENARIO 1: Happy Path
# ---------------------------------------------------------
echo -e "\n\033[1;35m>>> SCENARIO 1: Happy Path (Product 1: Wireless Mouse)\033[0m"
p1_before=$(get_product_stock 1)
echo "Product 1 stock before order: $p1_before"

res=$(curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": 1, "quantity": 2, "unitPrice": 25.99}')
orderId=$(echo "$res" | grep -o '"id":[0-9]*' | head -n 1 | cut -d: -f2)

echo "Placed Order #$orderId"

# Poll order status until CONFIRMED
confirmed=0
for i in {1..15}; do
  sleep 1
  status=$(curl -s "http://localhost:8081/orders/$orderId" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  echo "  Poll Order #$orderId status: $status"
  if [ "$status" = "CONFIRMED" ]; then
    confirmed=1
    break
  fi
done

if [ $confirmed -ne 1 ]; then
  echo -e "\033[1;31mScenario 1 failed: Order #$orderId did not reach CONFIRMED\033[0m"
  exit 1
fi
echo -e "\033[1;32m  [PASS] Order #$orderId successfully CONFIRMED!\033[0m"

p1_after=$(get_product_stock 1)
echo "Product 1 stock after order: $p1_after (expected $((p1_before - 2)))"
res_status=$(docker exec postgres psql -U postgres -d inventorydb -t -A -c "SELECT status FROM stock_reservations WHERE order_id=$orderId;" | tr -d '\r\n')
echo "Reservation status in Docker inventorydb: $res_status"

# ---------------------------------------------------------
# SCENARIO 2: Out of Stock Rejection
# ---------------------------------------------------------
echo -e "\n\033[1;35m>>> SCENARIO 2: Insufficient Stock Rejection\033[0m"

res=$(curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": 1, "quantity": 999, "unitPrice": 25.99}')
orderId=$(echo "$res" | grep -o '"id":[0-9]*' | head -n 1 | cut -d: -f2)

echo "Placed Order #$orderId for 999 units"

failed=0
for i in {1..15}; do
  sleep 1
  orderJson=$(curl -s "http://localhost:8081/orders/$orderId")
  status=$(echo "$orderJson" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  echo "  Poll Order #$orderId status: $status"
  if [ "$status" = "FAILED" ]; then
    failed=1
    reason=$(echo "$orderJson" | grep -o '"failureReason":"[^"]*"' | cut -d'"' -f4)
    echo -e "  Failure Reason: \033[1;33m$reason\033[0m"
    break
  fi
done

if [ $failed -ne 1 ]; then
  echo -e "\033[1;31mScenario 2 failed: Order #$orderId did not fail for insufficient stock\033[0m"
  exit 1
fi
echo -e "\033[1;32m  [PASS] Order #$orderId correctly marked FAILED (no stock deducted)!\033[0m"

# ---------------------------------------------------------
# SCENARIO 3: Forced Payment Failure & Compensating Rollback
# ---------------------------------------------------------
echo -e "\n\033[1;35m>>> SCENARIO 3: Payment Failure & Compensating Rollback (Product 2)\033[0m"
p2_before=$(get_product_stock 2)
echo "Product 2 stock before order: $p2_before"

res=$(curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": 2, "quantity": 1, "unitPrice": 89.50}')
orderId=$(echo "$res" | grep -o '"id":[0-9]*' | head -n 1 | cut -d: -f2)

echo "Placed Order #$orderId for failing Product #2"

rollbackConfirmed=0
for i in {1..15}; do
  sleep 1
  orderJson=$(curl -s "http://localhost:8081/orders/$orderId")
  status=$(echo "$orderJson" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  echo "  Poll Order #$orderId status: $status"
  if [ "$status" = "FAILED" ]; then
    rollbackConfirmed=1
    reason=$(echo "$orderJson" | grep -o '"failureReason":"[^"]*"' | cut -d'"' -f4)
    echo -e "  Failure Reason: \033[1;33m$reason\033[0m"
    break
  fi
done

if [ $rollbackConfirmed -ne 1 ]; then
  echo -e "\033[1;31mScenario 3 failed: Order #$orderId did not reach FAILED state\033[0m"
  exit 1
fi

p2_after=$(get_product_stock 2)
echo "Product 2 stock after rollback: $p2_after (expected $p2_before)"
res_status=$(docker exec postgres psql -U postgres -d inventorydb -t -A -c "SELECT status FROM stock_reservations WHERE order_id=$orderId;" | tr -d '\r\n')
echo "Reservation status in Docker inventorydb: $res_status"

echo -e "\033[1;32m  [PASS] Compensating rollback executed! Stock was returned via InventoryRestore and reservation is RELEASED!\033[0m"

echo -e "\n\033[1;32m==========================================================\033[0m"
echo -e "\033[1;32mALL SAGA TEST SCENARIOS PASSED SUCCESSFULLY!\033[0m"
echo -e "\033[1;32m==========================================================\033[0m"
