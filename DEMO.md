# Demo: hai agent nói chuyện với nhau qua broker

Demo chạy thật trên server, cho các nhóm thấy một agent kết nối và làm việc với broker
như thế nào. Hai agent dùng đúng file mẫu mà các nhóm sẽ copy
(`examples/python/agent.py`), chỉ thay hàm `handle()`
(`examples/python/demo_agents.py`):

| Agent | Vai trò | Làm gì |
|---|---|---|
| `dieu_phoi` | agent điều phối | gửi câu hỏi (`type: request`) tới `tin_tuc`, nhận câu trả lời |
| `tin_tuc` | agent tin tức | nhận câu hỏi, "gọi AI" 2 giây, gửi câu trả lời (`type: response`, `reply_to` = id câu hỏi) |

```
dieu_phoi ──POST /send──> BROKER ──webhook──> tin_tuc
    ^                       │  ^                  │
    └──────webhook──────────┘  └────POST /send────┘   (mỗi bên nhận xong đều gửi ACK)
```

Hai agent chạy trong Docker cạnh broker, không mở cổng ra ngoài. Broker tìm webhook
của chúng qua `AGENT_ROUTES`, giống hệt cách nó tìm webhook của các nhóm khác.

## Chuẩn bị (làm trước buổi thuyết trình)

SSH vào server rồi chạy:

```bash
cd /opt/message-broker
git pull
./scripts/demo-agents.sh start
```

Lần đầu mất khoảng 1 phút (tải image Python, build lại broker). Thấy dòng
`==> Sẵn sàng.` là xong. Lệnh `start` tự thêm route của `dieu_phoi` và `tin_tuc` vào
`AGENT_ROUTES` trong `.env`, không đụng tới route của các nhóm khác.

Chạy thử `./scripts/demo-agents.sh ask` một lần để chắc mọi thứ ổn.

## Kịch bản 1: luồng bình thường

```bash
./scripts/demo-agents.sh ask
```

Hoặc tự đặt câu hỏi: `./scripts/demo-agents.sh ask tin thể thao hôm nay`.

Màn hình hiện log trực tiếp của cả ba (đã rút gọn), tự dừng khi xong:

```
[dieu_phoi] gửi REQUEST tới tin_tuc: {"query": "tin công nghệ hôm nay"} (conversation=conv-f55ddd, trace=demo-cf98c8b7)
[dieu_phoi] broker trả 202 Accepted cho tin ca65...
broker        | Accepted message ca65... for tin_tuc (duplicate=false)
broker        | Agent tin_tuc accepted message ca65... via HTTP 200 OK; waiting for ACK
demo-tin-tuc  | [tin_tuc] nhận REQUEST ca65... từ dieu_phoi conversation=conv-f55ddd payload={"query": ...}
demo-tin-tuc  | [tin_tuc] đang gọi AI tìm tin (2 giây)...
broker        | Accepted message 5e3d... for dieu_phoi (duplicate=false)
demo-tin-tuc  | [tin_tuc] đã gửi RESPONSE về dieu_phoi (reply_to=ca65...)
broker        | ACK success from tin_tuc for message ca65...; status now Delivered after 1 attempt(s)
broker        | Agent dieu_phoi accepted message 5e3d... via HTTP 200 OK; waiting for ACK
demo-dieu-phoi| [dieu_phoi] nhận RESPONSE 5e3d... từ tin_tuc reply_to=ca65... payload={"articles": [...]}
demo-dieu-phoi| [dieu_phoi] xong: nhận 3 tin cho câu hỏi ca65...
broker        | ACK success from dieu_phoi for message 5e3d...; status now Delivered after 1 attempt(s)
==> Hoàn tất: dieu_phoi đã nhận câu trả lời cho câu hỏi ca65...
```

Lời giải thích:

1. `dieu_phoi` gửi câu hỏi bằng `POST /send`. Broker trả **202** ngay: "đã nhận, sẽ giao",
   chưa phải "đã giao". Agent gửi không phải chờ bên kia.
2. Broker tra `AGENT_ROUTES` ra webhook của `tin_tuc` và đẩy tin tới. `tin_tuc` trả
   **200** ngay ("đã nhận"), rồi mới xử lý.
3. `tin_tuc` xử lý xong thì trả lời bằng một tin **mới**: `type: response`,
   `reply_to` = id câu hỏi, giữ nguyên `conversation_id`. Sau đó mới gửi **ACK success**
   cho câu hỏi. Từ lúc này broker coi câu hỏi là `Delivered`, không gửi lại nữa.
4. Câu trả lời đi đúng con đường đó theo chiều ngược lại, và `dieu_phoi` cũng ACK.
5. Mọi dòng log của một cuộc hội thoại có cùng trace id (`demo-cf98c8b7`), nên Team 4 có
   thể ghép lại toàn bộ hành trình.

Hai agent không biết địa chỉ của nhau. Chúng chỉ biết địa chỉ broker và `agent_id` của
nhau.

## Kịch bản 2: agent xử lý chậm, broker gửi lại

```bash
./scripts/demo-agents.sh retry
```

Lần này `tin_tuc` xử lý mất 7 giây, lâu hơn 5 giây broker chờ ACK:

```
demo-tin-tuc  | [tin_tuc] đang gọi AI tìm tin (7 giây)...
broker        | No ACK yet for message f5e0...: resending to tin_tuc (attempt 2)
demo-tin-tuc  | [tin_tuc] tin trùng f5e0... (đang xử lý dở): bỏ qua, sẽ ACK khi xong
demo-tin-tuc  | [tin_tuc] đã gửi RESPONSE về dieu_phoi (reply_to=f5e0...)
broker        | ACK success from tin_tuc for message f5e0...; status now Delivered after 2 attempt(s)
```

Lời giải thích: broker giao **ít nhất một lần**. Hết 5 giây chưa thấy ACK, broker không
biết agent đang bận hay đã chết, nên gửi lại. Vì vậy cùng một tin có thể đến hai lần, và
agent **phải lọc trùng theo `message_id`**. Agent mẫu đã làm sẵn việc này: `dieu_phoi`
vẫn chỉ nhận đúng một câu trả lời. Đây là lý do mọi nhóm nên dùng phần nhận tin của
agent mẫu.

## Kịch bản 3: agent bị tắt, tin không mất

```bash
./scripts/demo-agents.sh offline
```

Script tắt `tin_tuc`, cho `dieu_phoi` gửi câu hỏi, chờ 12 giây rồi bật `tin_tuc` lên lại:

```
==> Tắt tin_tuc (giả lập agent bị sập)
[dieu_phoi] broker trả 202 Accepted cho tin a737...
broker        | Delivery attempt failed for message a737...: ... tin_tuc ...
broker        | No ACK yet for message a737...: resending to tin_tuc (attempt 2)
broker        | Delivery attempt failed for message a737...
broker        | No ACK yet for message a737...: resending to tin_tuc (attempt 3)
==> Bật lại tin_tuc
broker        | No ACK yet for message a737...: resending to tin_tuc (attempt 4)
broker        | Agent tin_tuc accepted message a737... via HTTP 200 OK; waiting for ACK
demo-tin-tuc  | [tin_tuc] nhận REQUEST a737... từ dieu_phoi ...
...
broker        | ACK success from tin_tuc for message a737...; status now Delivered after 4 attempt(s)
```

Lời giải thích: `dieu_phoi` vẫn được 202 dù `tin_tuc` đang tắt. Broker giữ tin, cứ 5 giây
thử lại, và giao ngay khi `tin_tuc` sống lại. Bên gửi không phải tự viết logic thử lại.
Đây là lợi ích chính của việc đi qua broker thay vì các agent gọi thẳng nhau.

## Kịch bản 4: agent trên laptop, không cần IP public

Ba kịch bản trên chạy trong server. Kịch bản này cho thấy một agent chạy trên **laptop** (sau router
nhà/wifi trường, không có IP public) vẫn nói chuyện được với `tin_tuc` trên server, nhờ nhận tin bằng
cách kéo về (`GET /receive`). Cần Python 3.8+ trên laptop, không cài thêm gì.

Tải [`examples/python/agent.py`](examples/python/agent.py) về laptop. Mở **hai** cửa sổ PowerShell ở thư
mục chứa file đó.

Cửa sổ 1, agent của laptop kéo tin về:

```powershell
$env:AGENT_ID="laptop_nhom2"; python agent.py poll
```

Cửa sổ 2, gửi câu hỏi tới `tin_tuc` trên server:

```powershell
$env:AGENT_ID="laptop_nhom2"; python agent.py send tin_tuc "cho minh tin cong nghe"
```

Cửa sổ 1 hiện câu trả lời của `tin_tuc` (3 tin) và dòng `đã gửi ACK success`. Trên server,
`./scripts/demo-agents.sh logs` hiện:

```
broker        | Agent laptop_nhom2 is polling GET /receive: its messages wait at the broker instead of being pushed to a webhook
broker        | Accepted message 86f1... for tin_tuc (duplicate=false)
broker        | Agent tin_tuc accepted message 86f1... at http://demo-tin-tuc:9102/webhook/receive via HTTP 200 OK; waiting for ACK
broker        | Accepted message ddd9... for laptop_nhom2 (duplicate=false)
broker        | ACK success from tin_tuc for message 86f1...; status now Delivered after 1 attempt(s)
broker        | Agent laptop_nhom2 pulled message ddd9... via GET /receive; waiting for ACK
broker        | ACK success from laptop_nhom2 for message ddd9...; status now Delivered after 1 attempt(s)
```

Lời giải thích: laptop chỉ **gọi ra** broker (gửi tin, kéo tin, ACK), không ai phải gọi vào laptop, nên
không cần IP public hay ngrok. Broker giữ câu trả lời của `tin_tuc` cho tới khi laptop kéo về. Mọi quy
tắc vẫn như cũ: vẫn ACK, vẫn lọc trùng; chưa ACK thì 5 giây sau tin lại có trong lần kéo tiếp theo.

Gửi tới một agent không tồn tại thì broker báo lỗi ngay:

```powershell
$env:AGENT_ID="laptop_nhom2"; python agent.py send khong_ton_tai "xin chao"
```

ra `Gửi thất bại: /send trả HTTP 404: ... recipient "khong_ton_tai" is not registered ...`.

## Lệnh khác

| Lệnh | Tác dụng |
|---|---|
| `./scripts/demo-agents.sh logs` | xem log trực tiếp của broker và hai agent (Ctrl+C để thoát) |
| `./scripts/demo-agents.sh stop` | tắt hai agent demo; broker vẫn chạy cho các nhóm khác |

## Khi có lỗi

| Hiện tượng | Cách xử lý |
|---|---|
| `demo-dieu-phoi chưa chạy` | chạy `./scripts/demo-agents.sh start` |
| `start` báo lỗi tải image | server không ra được Internet; thử lại sau, hoặc hỏi thầy |
| Không thấy dòng `No ACK yet` | broker chưa được build lại với code mới: chạy lại `start` sau `git pull` |
| Chờ mãi không `Hoàn tất` | xem `docker compose logs broker`, kiểm tra `AGENT_ROUTES` trong `.env` có `dieu_phoi` và `tin_tuc` |

CI chạy cả ba kịch bản trên mỗi PR (job "Server config smoke test"), nên nếu CI xanh thì
demo chạy được trên server.
