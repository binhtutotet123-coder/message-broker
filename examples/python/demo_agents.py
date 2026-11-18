"""Demo hai agent nói chuyện với nhau qua broker, dùng lại agent mẫu agent.py.

Vai trò chọn bằng AGENT_ID:
    dieu_phoi  agent điều phối: gửi câu hỏi (request), nhận câu trả lời (response)
    tin_tuc    agent tin tức: nhận request, giả lập gọi AI vài giây, trả response

Cách dùng:
    python demo_agents.py serve                         chạy webhook của agent (theo AGENT_ID)
    python demo_agents.py ask [--delay N] [câu hỏi]     dieu_phoi gửi request tới tin_tuc

--delay N: nhờ tin_tuc "xử lý" N giây (mặc định DEMO_DELAY, 2 giây). Đặt N lớn hơn
RETRY_INTERVAL của broker (5 giây), ví dụ 7, để thấy broker gửi lại tin vì chưa có ACK
và agent lọc bỏ bản trùng.

Trên server dùng scripts/demo-agents.sh thay vì gọi trực tiếp (xem DEMO.md).
"""

import json
import os
import sys
import time
import uuid

import agent

DEFAULT_DELAY = float(os.environ.get("DEMO_DELAY", "2"))


def show(envelope):
    """Một dòng tóm tắt tin nhận được."""
    extra = " reply_to=%s" % envelope["reply_to"] if envelope.get("reply_to") else ""
    return "%s %s từ %s%s conversation=%s payload=%s" % (
        envelope["type"].upper(), envelope["message_id"], envelope["from"], extra,
        envelope.get("conversation_id", "-"), json.dumps(envelope["payload"], ensure_ascii=False))


def handle_tin_tuc(envelope, trace_id):
    """Agent tin tức: nhận câu hỏi, giả lập gọi AI, gửi danh sách tin về cho người hỏi."""
    print("[tin_tuc] nhận %s" % show(envelope))
    if envelope["type"] != "request":
        return
    delay = float(envelope["payload"].get("demo_delay", DEFAULT_DELAY))
    print("[tin_tuc] đang gọi AI tìm tin (%g giây)..." % delay)
    time.sleep(delay)
    articles = [
        "AI tạo sinh được dùng rộng rãi trong doanh nghiệp",
        "Ra mắt chip mới tiết kiệm điện",
        "Hệ thống phân tán: bài học từ các sự cố lớn",
    ]
    agent.send_message(envelope["from"], {"articles": articles}, msg_type="response",
                       reply_to=envelope["message_id"],
                       conversation_id=envelope.get("conversation_id"), trace_id=trace_id)
    print("[tin_tuc] đã gửi RESPONSE về %s (reply_to=%s)" % (envelope["from"], envelope["message_id"]))


def handle_dieu_phoi(envelope, trace_id):
    """Agent điều phối: in câu trả lời nhận được."""
    print("[dieu_phoi] nhận %s" % show(envelope))
    if envelope["type"] == "response":
        print("[dieu_phoi] xong: nhận %d tin cho câu hỏi %s" % (
            len(envelope["payload"].get("articles", [])), envelope.get("reply_to")))


def ask(args):
    payload = {}
    if len(args) >= 2 and args[0] == "--delay":
        payload["demo_delay"] = int(args[1]) if args[1].isdigit() else float(args[1])
        args = args[2:]
    payload["query"] = " ".join(args) or "tin công nghệ hôm nay"
    conversation = "conv-" + uuid.uuid4().hex[:6]
    trace = "demo-" + uuid.uuid4().hex[:8]
    print("[%s] gửi REQUEST tới tin_tuc: %s (conversation=%s, trace=%s)" % (
        agent.AGENT_ID, json.dumps(payload, ensure_ascii=False), conversation, trace))
    time.sleep(0.3)  # để dòng trên kịp hiện ra trước log của broker
    result = agent.send_message("tin_tuc", payload, msg_type="request",
                                conversation_id=conversation, trace_id=trace)
    print("[%s] broker trả 202 Accepted cho tin %s" % (agent.AGENT_ID, result["message_id"]))
    # Dòng cuối để scripts/demo-agents.sh biết chờ câu trả lời cho tin nào.
    print("message_id=%s" % result["message_id"])


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "ask":
        ask(sys.argv[2:])
        return
    if agent.AGENT_ID == "tin_tuc":
        agent.handle = handle_tin_tuc
    elif agent.AGENT_ID == "dieu_phoi":
        agent.handle = handle_dieu_phoi
    else:
        sys.exit("AGENT_ID phải là dieu_phoi hoặc tin_tuc")
    agent.main()


if __name__ == "__main__":
    main()
