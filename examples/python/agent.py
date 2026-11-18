"""Agent mẫu kết nối Message Broker (Team 2). Chỉ dùng thư viện chuẩn, cần Python 3.8+.

Cách dùng:
    python agent.py serve                 chạy webhook nhận tin tại http://0.0.0.0:<PORT>/webhook/receive
    python agent.py send <to> [nội dung]  gửi một tin type=request tới agent <to>

Biến môi trường:
    BROKER_URL  địa chỉ broker (mặc định http://103.81.84.102:8080)
    AGENT_ID    agent_id của nhóm bạn (mặc định team_xx_agent)
    PORT        cổng webhook khi chạy serve (mặc định 9000)

Sửa hàm handle() để đưa logic của agent vào. Phần còn lại lo việc nhận tin,
lọc tin trùng và gửi ACK đúng cách.
"""

import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BROKER_URL = os.environ.get("BROKER_URL", "http://103.81.84.102:8080").rstrip("/")
AGENT_ID = os.environ.get("AGENT_ID", "team_xx_agent")
PORT = int(os.environ.get("PORT", "9000"))


def post_json(url, body, trace_id=None):
    """POST một JSON, trả về (mã HTTP, body đã giải mã)."""
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    if trace_id:
        request.add_header("X-Trace-Id", trace_id)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def send_message(to, payload, msg_type="request", reply_to=None, conversation_id=None, trace_id=None):
    """Gửi một tin qua POST /send. Broker trả 202 nghĩa là đã nhận và sẽ giao."""
    message_id = str(uuid.uuid4())
    envelope = {
        "message_id": message_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "from": AGENT_ID,
        "to": to,
        "type": msg_type,
        "payload": payload,
    }
    if reply_to:
        envelope["reply_to"] = reply_to
    if conversation_id:
        envelope["conversation_id"] = conversation_id
    status, body = post_json(BROKER_URL + "/send", envelope, trace_id or "trace-" + message_id)
    if status != 202:
        raise RuntimeError("/send trả HTTP %s: %s" % (status, body))
    return body


def ack(message_id, outcome, trace_id=None):
    """Báo broker kết quả xử lý: success thì dừng gửi lại, failed thì broker gửi lại sau."""
    url = "%s/messages/%s/ack" % (BROKER_URL, urllib.parse.quote(message_id, safe=""))
    status, body = post_json(url, {"agent_id": AGENT_ID, "status": outcome}, trace_id)
    if status != 200:
        print("ACK %s lỗi HTTP %s: %s" % (message_id, status, body))


def handle(envelope, trace_id):
    """Logic của agent: sửa hàm này. Ném exception nếu xử lý thất bại."""
    print("[%s] nhận %s %s từ %s: %s" % (
        AGENT_ID, envelope["type"], envelope["message_id"], envelope["from"],
        json.dumps(envelope["payload"], ensure_ascii=False)))
    if envelope["type"] == "request":
        # Trả lời: gửi tin mới về cho người gửi, ghi reply_to và giữ conversation_id, trace id.
        send_message(envelope["from"], {"answer": "đã nhận", "echo": envelope["payload"]},
                     msg_type="response", reply_to=envelope["message_id"],
                     conversation_id=envelope.get("conversation_id"), trace_id=trace_id)


# Trạng thái từng message_id: "processing" hoặc "done". Broker giao ít nhất một lần,
# nên cùng một tin có thể đến nhiều lần và phải lọc trùng ở đây.
states = {}
states_lock = threading.Lock()


def process(envelope, trace_id):
    message_id = envelope["message_id"]
    try:
        handle(envelope, trace_id)
    except Exception as error:
        print("Xử lý %s thất bại: %s" % (message_id, error))
        with states_lock:
            states.pop(message_id, None)  # cho phép xử lý lại khi broker gửi lại
        ack(message_id, "failed", trace_id)
        return
    with states_lock:
        states[message_id] = "done"
    ack(message_id, "success", trace_id)


class Webhook(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook/receive":
            self.send_error(404)
            return
        try:
            envelope = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            message_id = envelope["message_id"]
        except (ValueError, KeyError):
            self.send_error(400, "invalid message")
            return
        trace_id = self.headers.get("X-Trace-Id")

        # Trả 200 ngay (trong 5 giây) để báo "đã nhận"; xử lý và ACK ở luồng riêng.
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"received"}')

        with states_lock:
            state = states.get(message_id)
            if state is None:
                states[message_id] = "processing"
        if state is None:
            threading.Thread(target=process, args=(envelope, trace_id), daemon=True).start()
        elif state == "done":
            print("Tin trùng %s: bỏ qua, ACK lại" % message_id)
            threading.Thread(target=ack, args=(message_id, "success", trace_id), daemon=True).start()
        # state == "processing": đang xử lý, luồng kia sẽ tự ACK.

    def log_message(self, *args):
        pass


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "serve":
        print("Agent %s nhận tin tại http://0.0.0.0:%d/webhook/receive (broker %s)" % (AGENT_ID, PORT, BROKER_URL))
        ThreadingHTTPServer(("0.0.0.0", PORT), Webhook).serve_forever()
    elif len(sys.argv) >= 3 and sys.argv[1] == "send":
        text = " ".join(sys.argv[3:]) or "xin chào"
        result = send_message(sys.argv[2], {"text": text})
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
