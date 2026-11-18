#!/usr/bin/env bash
# Demo hai agent nói chuyện với nhau qua broker, chạy trên server (xem DEMO.md):
#   dieu_phoi  agent điều phối: gửi câu hỏi (request) tới tin_tuc, nhận câu trả lời (response)
#   tin_tuc    agent tin tức: nhận câu hỏi, "gọi AI" vài giây, gửi câu trả lời về
# Cả hai là examples/python/demo_agents.py chạy trong container python:3.12-alpine.
#
#   ./scripts/demo-agents.sh start            bật hai agent demo (build lại broker nếu code đổi)
#   ./scripts/demo-agents.sh ask [câu hỏi]    luồng bình thường: request -> ACK -> response -> ACK
#   ./scripts/demo-agents.sh retry            tin_tuc xử lý 7 giây (> 5 giây chờ ACK): broker gửi lại, agent lọc trùng
#   ./scripts/demo-agents.sh offline          tin_tuc tắt khi có tin: broker giữ tin, thử lại, giao khi agent bật lại
#   ./scripts/demo-agents.sh logs             xem log trực tiếp của broker và hai agent (Ctrl+C để thoát)
#   ./scripts/demo-agents.sh stop             tắt hai agent demo (broker vẫn chạy)
#
# ask, retry, offline hiện log trực tiếp của broker và hai agent, tự dừng khi dieu_phoi
# nhận được câu trả lời.
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_FILE=docker-compose.server.yml
export COMPOSE_PROFILES=demo
AGENTS=(demo-dieu-phoi demo-tin-tuc)
DEMO_ROUTES="dieu_phoi=http://demo-dieu-phoi:9101/webhook/receive,tin_tuc=http://demo-tin-tuc:9102/webhook/receive"
LOGS_PID=""
SINCE=""
MSG_ID=""

say() { printf '\n==> %s\n' "$*"; }
now() { date -u +%Y-%m-%dT%H:%M:%S.%NZ; }

# Thêm route của hai agent demo vào AGENT_ROUTES trong .env nếu chưa có.
ensure_env() {
  if [ ! -f .env ]; then
    cp .env.example .env
    say "Đã tạo .env từ .env.example"
  fi
  # Dòng AGENT_ROUTES trống hoặc không có: docker-compose.server.yml đã có sẵn route demo.
  if grep -q '^AGENT_ROUTES=.' .env && ! grep -q '^AGENT_ROUTES=.*dieu_phoi=' .env; then
    sed -i "s|^AGENT_ROUTES=.*|&,$DEMO_ROUTES|" .env
    say "Đã thêm route của dieu_phoi và tin_tuc vào AGENT_ROUTES trong .env"
  fi
}

# Chờ agent in dòng "nhận tin tại" (webhook đã mở) kể từ lần khởi động gần nhất.
wait_ready() {
  local id started
  id=$(docker compose ps -q "$1")
  started=$(docker inspect -f '{{.State.StartedAt}}' "$id")
  for _ in $(seq 1 60); do
    if docker compose logs --since "$started" "$1" 2>/dev/null | grep -q 'nhận tin tại'; then
      return 0
    fi
    sleep 0.5
  done
  echo "$1 chưa sẵn sàng sau 30 giây, xem: docker compose logs $1" >&2
  return 1
}

require_running() {
  local svc
  for svc in "${AGENTS[@]}"; do
    if [ -z "$(docker compose ps -q --status running "$svc" 2>/dev/null)" ]; then
      echo "$svc chưa chạy. Bật trước bằng: ./scripts/demo-agents.sh start" >&2
      exit 1
    fi
  done
}

# Hiện log trực tiếp của broker và hai agent từ thời điểm này.
begin_watch() {
  SINCE=$(now)
  docker compose logs -f --since "$SINCE" broker "${AGENTS[@]}" &
  LOGS_PID=$!
  sleep 1
}

end_watch() {
  if [ -n "$LOGS_PID" ]; then
    kill "$LOGS_PID" 2>/dev/null || true
    wait "$LOGS_PID" 2>/dev/null || true
    LOGS_PID=""
  fi
}
trap end_watch EXIT

# dieu_phoi gửi một câu hỏi tới tin_tuc qua broker; ghi lại message_id vào MSG_ID.
# In từng dòng ngay khi có (python -u, tee) để dòng "gửi REQUEST" hiện trước log của
# broker, đúng thứ tự thật.
send_ask() {
  local out
  out=$(mktemp)
  docker compose exec -T demo-dieu-phoi python -u demo_agents.py ask "$@" \
    | tee "$out" | grep --line-buffered -v '^message_id='
  MSG_ID=$(sed -n 's/^message_id=//p' "$out")
  rm -f "$out"
}

# Chờ tối đa $1 giây đến khi dieu_phoi nhận được câu trả lời cho MSG_ID.
wait_answer() {
  for _ in $(seq 1 $(($1 * 2))); do
    if docker compose logs --since "$SINCE" demo-dieu-phoi 2>/dev/null | grep -q "xong: .*$MSG_ID"; then
      sleep 1  # để kịp hiện mấy dòng ACK cuối
      end_watch
      say "Hoàn tất: dieu_phoi đã nhận câu trả lời cho câu hỏi $MSG_ID"
      return 0
    fi
    sleep 0.5
  done
  end_watch
  say "Sau $1 giây dieu_phoi vẫn chưa nhận được câu trả lời cho $MSG_ID" >&2
  return 1
}

cmd_start() {
  ensure_env
  say "Bật broker và hai agent demo (lần đầu sẽ tải image python, mất khoảng 1 phút)"
  docker compose up -d --build --wait --wait-timeout 180
  wait_ready demo-dieu-phoi
  wait_ready demo-tin-tuc
  say "Sẵn sàng. Thử: ./scripts/demo-agents.sh ask"
}

cmd_ask() {
  require_running
  say "dieu_phoi hỏi tin_tuc qua broker"
  begin_watch
  send_ask "$@"
  wait_answer 30
}

cmd_retry() {
  require_running
  say "tin_tuc xử lý mất 7 giây, lâu hơn 5 giây broker chờ ACK: broker sẽ gửi lại"
  begin_watch
  send_ask --delay 7 "tin công nghệ hôm nay"
  wait_answer 40
}

cmd_offline() {
  require_running
  say "Tắt tin_tuc (giả lập agent bị sập)"
  docker compose stop -t 2 demo-tin-tuc
  begin_watch
  send_ask "tin công nghệ hôm nay"
  say "tin_tuc đang tắt: broker giữ tin và cứ 5 giây thử gửi lại"
  sleep 12
  say "Bật lại tin_tuc"
  docker compose start demo-tin-tuc
  wait_answer 40
}

cmd_logs() {
  docker compose logs -f --tail 20 broker "${AGENTS[@]}"
}

cmd_stop() {
  docker compose rm -sf "${AGENTS[@]}"
  say "Đã tắt hai agent demo, broker vẫn chạy"
}

case "${1:-}" in
  start) cmd_start ;;
  ask) shift; cmd_ask "$@" ;;
  retry) cmd_retry ;;
  offline) cmd_offline ;;
  logs) cmd_logs ;;
  stop) cmd_stop ;;
  *)
    sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
