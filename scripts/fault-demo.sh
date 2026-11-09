#!/usr/bin/env bash
# Demo Fault Injection (Tuần 4): gửi COUNT tin trong lúc broker làm mất, làm trễ và nhân đôi
# webhook, rồi kiểm tra Agent B xử lý đủ COUNT tin và mỗi tin đúng một lần.
#
# Yêu cầu: broker chạy với FAULT_INJECTION=true (docker compose up đã bật sẵn) và
# mock-agent-b đang chạy. Chỉ cần curl và sed (chạy được trong Git Bash trên Windows).
#
# Tuỳ chỉnh bằng biến môi trường, ví dụ:
#   COUNT=50 FAULT='{"drop_rate":0.6,"delay_ms":7000,"agent":"agent_B"}' ./scripts/fault-demo.sh
set -euo pipefail

BROKER_URL=${BROKER_URL:-http://localhost:8080}
AGENT_URL=${AGENT_URL:-http://localhost:8081}
AGENT_ID=${AGENT_ID:-agent_B}
COUNT=${COUNT:-20}
TIMEOUT=${TIMEOUT:-120}
FAULT=${FAULT:-"{\"drop_rate\":0.4,\"duplicate_rate\":0.3,\"delay_ms\":200,\"agent\":\"$AGENT_ID\"}"}

# stat FIELD: đọc một bộ đếm từ GET /stats của mock agent.
stat() {
  curl -fsS "$AGENT_URL/stats" | sed -E "s/.*\"$1\":([0-9]+).*/\1/"
}

processed_before=$(stat processed)
duplicates_before=$(stat duplicates)

echo "1) Bật fault injection: $FAULT"
curl -fsS -X PUT "$BROKER_URL/admin/fault" -H 'Content-Type: application/json' -d "$FAULT"
trap 'curl -fsS -o /dev/null -X DELETE "$BROKER_URL/admin/fault" && echo "Đã tắt fault injection."' EXIT

run=$(date +%s)
echo "2) Gửi $COUNT tin tới $AGENT_ID"
for i in $(seq 1 "$COUNT"); do
  id="demo-$run-$i"
  curl -fsS -o /dev/null -X POST "$BROKER_URL/send" \
    -H 'Content-Type: application/json' -H "X-Trace-Id: trace-$id" \
    -d "{\"message_id\":\"$id\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"from\":\"agent_A\",\"to\":\"$AGENT_ID\",\"type\":\"request\",\"payload\":{\"n\":$i}}"
done

echo "3) Chờ broker retry tới khi Agent B xử lý đủ $COUNT tin (tối đa ${TIMEOUT}s)..."
start=$(date +%s)
while :; do
  processed=$(( $(stat processed) - processed_before ))
  printf '\r   đã xử lý %d/%d' "$processed" "$COUNT"
  [ "$processed" -ge "$COUNT" ] && break
  if [ $(( $(date +%s) - start )) -ge "$TIMEOUT" ]; then
    echo
    echo "❌ Hết ${TIMEOUT}s mà Agent B mới xử lý $processed/$COUNT tin."
    exit 1
  fi
  sleep 1
done
echo

duplicates=$(( $(stat duplicates) - duplicates_before ))
echo "4) Kết quả"
echo "   Agent B: $(curl -fsS "$AGENT_URL/stats")"
echo "   Broker : $(curl -fsS "$BROKER_URL/admin/fault")"
echo "✅ $COUNT/$COUNT tin được xử lý đúng một lần; $duplicates lần giao trùng đã bị Agent B bỏ qua."
