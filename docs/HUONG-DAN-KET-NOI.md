# Hướng dẫn kết nối Message Broker (Team 2)

Broker là "bưu điện" chung của lớp: agent gửi tin cho broker, broker giữ tin cho người nhận và giao lại
cho tới khi người nhận xác nhận (ACK). Người nhận lấy tin theo một trong hai cách:

- **Kéo về (`GET /receive`)**: agent tự hỏi broker "có tin cho tôi không". Chỉ cần gọi ra ngoài, nên
  **không cần IP public**: chạy được trên laptop ở nhà, ở trường. Nên dùng cách này.
- **Webhook**: broker gọi vào địa chỉ của agent. Máy chạy agent phải có địa chỉ mà server gọi tới được.

- **Địa chỉ broker:** `http://103.81.84.102:8080`
- **Kiểm tra broker đang chạy:** mở `http://103.81.84.102:8080/healthz` thấy `ok`.
  Mở địa chỉ gốc `/` ra 404 là bình thường (broker là API, không có trang web).
- **Agent thử nghiệm:** `agent_B` luôn chạy sẵn trên server, gửi tin tới đây để kiểm tra phần gửi của nhóm bạn.
- **Đặc tả API đầy đủ:** [`docs/swagger.yaml`](swagger.yaml) (mở bằng https://editor.swagger.io).

## Tóm tắt 4 việc mỗi nhóm cần làm

1. Chọn **`agent_id`** cho agent của nhóm (mục 1).
2. **Nhận tin**: kéo về bằng `GET /receive` (không cần IP public), hoặc dựng webhook (mục 4).
3. Xử lý xong thì **gọi ACK** (mục 5) và **lọc tin trùng** theo `message_id` (mục 6).
4. Gửi tin cho agent khác qua **`POST /send`** (mục 3).

Muốn nhanh: dùng agent mẫu [`examples/python/agent.py`](../examples/python/agent.py) đã làm sẵn cả 4 việc (mục 8).

## 1. Đăng ký agent

**`agent_id`**: tên duy nhất của agent, không có dấu cách, ví dụ `team_08_news`. Nên dùng đúng
`agent_id` đã đăng ký ở Service Registry của Team 1.

- **Agent kéo tin về (`GET /receive`)**: không cần đăng ký gì với Team 2. Khi agent đang kéo tin, broker
  đã biết agent đó và nhận tin gửi cho nó. Muốn agent khác gửi được tin cho bạn cả lúc agent của bạn
  chưa chạy, nhắn Team 2 `agent_id` để thêm vào bảng định tuyến dưới dạng `team_08_news=pull`.
- **Agent nhận qua webhook**: gửi cho Team 2 `agent_id` và **URL webhook**, ví dụ
  `http://1.2.3.4:9000/webhook/receive`. Server `103.81.84.102` phải **gọi tới được** địa chỉ này:
  dùng IP public hoặc tên miền, không dùng `localhost` hay IP mạng nhà/wifi trường.

Broker cũng tra địa chỉ trong Service Registry của Team 1 (`GET /v1/agents/{agent_id}`, trường
`endpoint`); agent có trạng thái `UNREGISTERED` ở đó được coi là không tồn tại.

## 2. Định dạng tin nhắn

Mọi tin là một JSON object:

| Trường | Bắt buộc | Ý nghĩa |
|---|---|---|
| `message_id` | Có | ID duy nhất của tin, nên dùng UUID. Không có dấu cách ở đầu/cuối |
| `timestamp` | Có | Thời điểm tạo tin, chuẩn ISO 8601, ví dụ `2026-11-17T08:00:00Z` |
| `from` | Có | `agent_id` của người gửi (nhóm bạn) |
| `to` | Có | `agent_id` của người nhận |
| `type` | Có | `request` (yêu cầu), `response` (trả lời), hoặc `final` (kết quả cuối) |
| `payload` | Có | Nội dung, phải là một JSON object `{...}` (có thể rỗng `{}`) |
| `conversation_id` | Không | ID của cả cuộc hội thoại, giữ nguyên qua các tin hỏi/đáp |
| `reply_to` | Không | `message_id` của tin mà tin này đang trả lời |
| `ttl` | Không | Thời gian sống (giây). **Chưa được áp dụng** |

```json
{
  "message_id": "6f1c2e7a-0b9d-4a51-9a3e-2f7c1d8e4b10",
  "timestamp": "2026-11-17T08:00:00Z",
  "from": "team_08_news",
  "to": "agent_B",
  "type": "request",
  "conversation_id": "conv-42",
  "payload": { "query": "tin tức hôm nay" }
}
```

## 3. Gửi tin: `POST /send`

Header: `Content-Type: application/json`, và nên có `X-Trace-Id` (mã theo dõi, Team 4 dùng để lần theo
tin; giữ nguyên mã này qua cả cuộc hội thoại).

```bash
curl -X POST http://103.81.84.102:8080/send \
  -H 'Content-Type: application/json' -H 'X-Trace-Id: trace-demo-1' \
  -d '{"message_id":"demo-1","timestamp":"2026-11-17T08:00:00Z","from":"team_08_news","to":"agent_B","type":"request","payload":{"hello":"world"}}'
```

Trên Windows, chạy lệnh này trong Git Bash, hoặc dùng agent mẫu ở mục 8.

**Thành công: `202 Accepted`**, nghĩa là broker đã nhận tin và sẽ tự giao:

```json
{"delivery_status":"Pending","duplicate":false,"message":"Broker đã tiếp nhận và đang định tuyến","message_id":"demo-1","status":"Accepted","trace_id":"trace-demo-1"}
```

| Mã | Khi nào | Làm gì |
|---|---|---|
| `202` + `"duplicate": true` | Gửi lại đúng tin cũ (cùng `message_id`, cùng nội dung) | Không sao: broker không tạo bản thứ hai. Nên gửi lại kiểu này khi mạng lỗi |
| `400` | Thiếu trường, sai `type`, `payload` không phải object, JSON sai | Đọc trường `message` trong phản hồi để biết lỗi |
| `404` | Người nhận (`to`) chưa đăng ký ở đâu cả: không có trong bảng định tuyến, không có trong Registry (hoặc đã `UNREGISTERED`), và không đang kéo tin | Kiểm tra lại `to`; người nhận dùng `GET /receive` thì phải đang chạy |
| `409` | `message_id` đã dùng cho một tin **khác nội dung** | Tạo `message_id` mới |
| `500` | Lỗi phía broker | Thử lại sau, báo Team 2 |

Lỗi luôn có dạng `{"code": 400, "message": "type must be request, response, or final"}`.

Người nhận đã đăng ký nhưng đang offline thì broker vẫn trả `202`: tin được giữ lại và giao khi người
nhận online (webhook: thử lại mỗi 5 giây; kéo tin: lần kéo tiếp theo). Nếu Registry tạm thời không trả
lời, broker cũng trả `202` và thử lại sau.

## 4. Nhận tin

### Cách 1: kéo về bằng `GET /receive` (không cần IP public)

```bash
curl "http://103.81.84.102:8080/receive?agent_id=team_08_news&wait=20"
```

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `agent_id` | bắt buộc | `agent_id` của nhóm bạn |
| `limit` | `10` | Số tin tối đa mỗi lần (1–100) |
| `wait` | `0` | Chưa có tin thì broker chờ tối đa bao nhiêu giây (tối đa 30) rồi mới trả lời. Đặt `20` để tin tới gần như ngay mà không phải gọi liên tục |

Kết quả `200` là một mảng tin (mảng rỗng `[]` nếu chưa có tin). Mỗi tin là nguyên tin nhắn ở mục 2,
thêm trường `trace_id` (mã theo dõi của tin, dùng làm `X-Trace-Id` khi trả lời):

```json
[{"message_id":"m-123","timestamp":"2026-11-23T08:00:00Z","from":"agent_A","to":"team_08_news","type":"request","payload":{"query":"tin tức hôm nay"},"trace_id":"trace-42"}]
```

- Gọi lại `GET /receive` liên tục trong một vòng lặp (agent mẫu ở mục 8 làm sẵn).
- Tin kéo về **vẫn phải ACK** (mục 5). Chưa ACK thì sau 5 giây tin lại nằm trong lần kéo tiếp theo,
  nên vẫn phải **lọc tin trùng** (mục 6).
- Trong lúc agent đang kéo tin (và 60 giây sau lần kéo cuối), broker không gọi webhook của agent đó.

### Cách 2: webhook

Broker gửi `POST` tới URL webhook của bạn, body là nguyên tin nhắn ở mục 2, header `X-Trace-Id`.

- Trả về mã `2xx` (ví dụ `200`) **ngay**, trong vòng **5 giây**. Mã này chỉ có nghĩa "đã nhận được".
- Xử lý tin sau đó (có thể lâu), rồi gọi ACK (mục 5).
- Không trả `2xx`, quá 5 giây, hoặc không gọi được: broker coi là chưa giao và gửi lại sau 5 giây.

## 5. Xác nhận: ACK

Xử lý xong, gọi ACK. Ví dụ `team_08_news` vừa xử lý xong tin có `message_id` là `m-123`:

```bash
curl -X POST http://103.81.84.102:8080/messages/m-123/ack \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"team_08_news","status":"success"}'
```

- `agent_id` là **agent_id của nhóm bạn**, phải đúng là người nhận (`to`) của tin.
- `status`: `success` thì broker dừng gửi lại; `failed` (xử lý lỗi) thì broker gửi lại sau 5 giây.
- Cách khác: `POST /ack` với `message_id` nằm trong body.

| Mã | Ý nghĩa |
|---|---|
| `200` | Đã ghi nhận, ví dụ `{"attempts":1,"message_id":"m-123","status":"Delivered"}` |
| `400` | Thiếu `agent_id`, hoặc `status` không phải `success`/`failed` |
| `403` | `agent_id` không phải người nhận của tin này |
| `404` | Không có tin với `message_id` này |

**Không gọi ACK thì broker gửi lại tin mãi**, mỗi 5 giây một lần.

## 6. Bắt buộc: lọc tin trùng

Broker đảm bảo giao **ít nhất một lần**: nếu ACK bị mất hoặc đến chậm, cùng một tin có thể tới webhook
nhiều lần. Hãy lưu các `message_id` đã xử lý:

- gặp lại tin đã xử lý xong: **không xử lý lại**, nhưng vẫn **gọi ACK `success`** (lần ACK trước có thể đã mất);
- gặp lại tin đang xử lý dở: bỏ qua, lần xử lý kia sẽ tự ACK.

## 7. Trả lời một tin

Trả lời là gửi một **tin mới** qua `POST /send`:

- `to` = `from` của tin gốc;
- `type` = `response` (hoặc `final` nếu là kết quả cuối);
- `reply_to` = `message_id` của tin gốc;
- giữ nguyên `conversation_id` và header `X-Trace-Id` của tin gốc.

## 8. Agent mẫu Python

[`examples/python/agent.py`](../examples/python/agent.py): chỉ dùng thư viện chuẩn Python 3.8+, không cần
cài thêm gì. Đã làm sẵn nhận tin (kéo về hoặc webhook), lọc trùng, ACK, gửi tin, và tự trả lời mỗi
`request` bằng một `response`.

```bash
# Nhận tin bằng cách kéo về: chạy được trên laptop, không cần IP public
# (PowerShell: $env:AGENT_ID="team_08_news"; python agent.py poll)
AGENT_ID=team_08_news python agent.py poll

# Hoặc nhận tin qua webhook ở cổng 9000 (máy phải có IP public)
AGENT_ID=team_08_news PORT=9000 python agent.py serve

# Ở cửa sổ khác: gửi thử một tin tới agent_B
AGENT_ID=team_08_news python agent.py send agent_B xin chào
```

Logic của agent nằm trong hàm `handle()`: sửa hàm này, phần còn lại giữ nguyên.

## 9. Chưa có, sẽ bổ sung

- **Xác thực token (Team 3):** sau này mọi request cần header `Authorization: Bearer <token>`.
- **Nối với Service Registry của Team 1 trên server:** broker đã tra được Registry (đã chạy thử), đang
  chờ hai nhóm mở kết nối giữa hai server.
- **TTL và Dead-letter queue:** hiện tin không giao được sẽ được thử lại mãi.
- **Lưu database:** hiện broker lưu tin trong bộ nhớ, broker khởi động lại thì tin chưa giao bị mất.

Có vấn đề khi kết nối, nhắn Team 2 trong nhóm chat của lớp, kèm `message_id` và `X-Trace-Id` của tin.
