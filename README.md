# message-broker

Message Broker trung tâm (Team 2) cho hệ thống phân tán của lớp. Hợp đồng API nằm ở
[`docs/swagger.yaml`](docs/swagger.yaml).

## Chạy nhanh bằng Docker

```bash
docker compose up --build
```

| Service    | Cổng | Vai trò                                                    |
|------------|------|------------------------------------------------------------|
| `broker`   | 8080 | Message Broker                                             |
| `agent-b`  | 8081 | Agent nhận giả lập: tự ACK, lọc trùng, thống kê ở `/stats` |
| `registry` | 8082 | Service Registry giả lập (thay cho Team 1 khi chạy thử)    |

Gửi thử một tin:

```bash
curl -X POST localhost:8080/send -H 'Content-Type: application/json' -H 'X-Trace-Id: demo-1' \
  -d '{"message_id":"m1","timestamp":"2026-11-09T07:00:00Z","from":"agent_A","to":"agent_B","type":"request","payload":{"hello":"world"}}'
curl localhost:8081/stats
```

## Chạy không cần Docker

```bash
go run ./cmd/mock-agent-b   # terminal 1
go run .                    # terminal 2: mặc định gửi agent_B tới localhost:8081
```

Muốn thử định tuyến qua registry thì chạy thêm `go run ./cmd/mock-registry` và khởi động
broker với `REGISTRY_LOOKUP_URL='http://localhost:8082/agents/{agent_id}'`.

## Cấu hình broker

| Biến môi trường       | Mặc định                                        | Ý nghĩa |
|-----------------------|-------------------------------------------------|---------|
| `BROKER_ADDR`         | `:8080`                                         | Địa chỉ lắng nghe |
| `RETRY_INTERVAL`      | `5s`                                            | Chờ ACK bao lâu trước khi gửi lại |
| `AGENT_ROUTES`        | `agent_B=http://localhost:8081/webhook/receive` | Bảng định tuyến tĩnh `id=url,id=url`; đặt rỗng để chỉ dùng registry |
| `REGISTRY_LOOKUP_URL` | (trống)                                         | URL tra cứu registry, phải chứa `{agent_id}` |
| `REGISTRY_CACHE_TTL`  | `30s`                                           | Thời gian cache kết quả registry (`0s` = không cache) |
| `FAULT_INJECTION`     | `false`                                         | Bật API `/admin/fault` |

## Định tuyến

Broker gửi mỗi tin tới webhook của agent ghi trong trường `to`. Thứ tự tra cứu:
`AGENT_ROUTES` trước, sau đó tới Service Registry. Nếu không tìm thấy agent (hoặc
registry đang lỗi), tin vẫn ở trạng thái `Pending` và được thử lại sau `RETRY_INTERVAL`.

**Giả định về API của Team 1 (cần xác nhận):** `GET {REGISTRY_LOOKUP_URL}` trả về
`200 {"webhook_url": "http://..."}` (chấp nhận cả trường `endpoint` hoặc `url`), hoặc `404`
nếu agent chưa đăng ký. Nếu Team 1 dùng định dạng khác, chỉ cần sửa `parseRegistryResponse`
trong [`routing/resolver.go`](routing/resolver.go).

## Fault Injection (kiểm thử ACK và Retry)

Khi `FAULT_INJECTION=true`, broker có API điều khiển lỗi trên đường gửi webhook:

```bash
curl -X PUT localhost:8080/admin/fault -H 'Content-Type: application/json' \
  -d '{"drop_rate":0.4,"delay_ms":200,"duplicate_rate":0.3,"agent":"agent_B"}'
curl localhost:8080/admin/fault             # xem cấu hình và thống kê
curl -X DELETE localhost:8080/admin/fault   # tắt toàn bộ lỗi
```

| Trường           | Ý nghĩa |
|------------------|---------|
| `drop_rate`      | Xác suất (0–1) làm **mất** webhook: broker tưởng đã gửi, chỉ có thể retry nhờ thiếu ACK |
| `delay_ms`       | Làm **trễ** mỗi lần gửi; vượt 5000 ms thì lần gửi bị timeout và được retry |
| `duplicate_rate` | Xác suất gửi **trùng** webhook, để kiểm tra agent lọc trùng theo `message_id` |
| `agent`          | Chỉ gây lỗi cho tin gửi tới agent này (nên đặt khi broker dùng chung cả lớp) |

Phía agent, mock Agent B có `ACK_DROP_RATE` (giả lập mất ACK) và `ACK_DELAY` (ACK đến muộn).

Demo trọn kịch bản trên hệ thống đang chạy:

```bash
./scripts/fault-demo.sh
```

Script gửi 20 tin trong lúc bật lỗi rồi kiểm tra Agent B xử lý đủ 20 tin, mỗi tin đúng một
lần. Bản tự động của kịch bản này là test `TestFaultInjectionEveryMessageDeliveredAndProcessedOnce`.

## Lưu ý cho các team Agent

- Sau khi xử lý xong tin, **bắt buộc** gọi `POST /messages/{message_id}/ack`; nếu không,
  broker sẽ gửi lại tin.
- Broker đảm bảo giao **ít nhất một lần**, nên agent có thể nhận cùng một tin nhiều lần.
  Hãy lọc trùng theo `message_id` (xem `internal/mockagent`) và vẫn ACK lại tin trùng.

## Test

```bash
go test -race ./...
```
