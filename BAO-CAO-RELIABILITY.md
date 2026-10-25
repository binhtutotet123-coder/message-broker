# Báo cáo triển khai Reliability — Message Broker

## Mục tiêu

Báo cáo này mô tả phần Reliability của Message Broker: tiếp nhận ACK, quản lý trạng thái giao tin, retry khi chưa hoàn tất và chống xử lý lặp theo `message_id`.

## Luồng xử lý

1. Agent gửi một Message Envelope hợp lệ đến `POST /send`.
2. Broker lưu message ở trạng thái `Pending` và trả `202 Accepted`. Việc gửi webhook được xử lý bất đồng bộ.
3. Worker gửi webhook đến Agent nhận. HTTP 2xx chỉ xác nhận webhook đã phản hồi ở tầng HTTP; message vẫn `Pending` cho tới khi Broker nhận ACK thành công.
4. Nếu chưa có ACK thành công sau khoảng retry được cấu hình (hiện tại là 5 giây), worker thử gửi lại. Thời gian retry được tính từ lúc bắt đầu lần gửi trước. Nếu lần gửi kéo dài ít nhất bằng khoảng retry, lần thử tiếp theo bắt đầu ngay sau khi lần trước kết thúc.
5. ACK `success` chuyển message sang `Delivered`. ACK `failed` giữ message ở `Pending` và lên lịch thử lại.

## API liên quan

### Gửi message

`POST /send`

Body là Message Envelope JSON. Các trường `message_id`, `from`, `to`, `timestamp`, `type` và `payload` được kiểm tra. `message_id`, `from` và `to` không được rỗng hoặc có khoảng trắng ở đầu/cuối. Request phải chứa đúng một giá trị JSON; JSON thứ hai hoặc dữ liệu thừa sẽ bị từ chối.

### Gửi ACK

Hai dạng endpoint được hỗ trợ:

- `POST /ack`, với `message_id` trong JSON body.
- `POST /messages/{message_id}/ack`, với `message_id` trong đường dẫn.

ACK cần `agent_id` đúng với người nhận và `status` là `success` hoặc `failed`. Body cũng phải chứa đúng một giá trị JSON. ACK từ Agent không đúng hoặc dữ liệu không hợp lệ sẽ bị từ chối.

## Đảm bảo Reliability

- **Idempotency:** gửi lại cùng `message_id` với cùng nội dung không tạo message mới. Dùng lại ID với nội dung khác trả về conflict.
- **ACK:** chỉ Agent nhận message mới có thể ACK. ACK `success` cập nhật trạng thái `Delivered`; các lần gửi lặp sau đó không làm message quay lại `Pending`.
- **Retry:** worker tự động thử lại message đang `Pending`; mỗi lần gửi có timeout. Worker dùng lease token để ngăn một worker cũ giải phóng lease do worker khác sở hữu.
- **Thay thế backend:** Broker thao tác qua `MessageStore`; hiện triển khai dùng `MemoryStore`, nên có thể thay bằng backend khác mà không cần thay đổi phần điều phối chính.

## Kiểm thử và xác minh

Các test bao phủ luồng gửi/ACK, idempotency, message ID conflict, retry, ACK khi webhook đang chạy, lease hết hạn, định danh có khoảng trắng và JSON có dữ liệu thừa.

Các lệnh kiểm tra đã chạy thành công:

```text
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go build ./...
git diff --cached --check
```

Trên Windows, để chạy race detector trong môi trường hiện tại cần bật CGO và dùng GCC. GCC được tìm thấy tại `C:\msys64\ucrt64\bin\gcc.exe`.

## Giới hạn hiện tại

- `MemoryStore` chỉ lưu dữ liệu trong RAM. Khởi động lại Broker sẽ làm mất trạng thái và message đã nhận; lease chỉ hỗ trợ thu hồi claim trong thời gian process còn dùng cùng store.
- Worker hiện xử lý tuần tự trong mỗi vòng dispatch; webhook chậm có thể làm message khác phải chờ.
- URL webhook Agent B hiện được cấu hình cố định trong `main.go`; chưa tích hợp Service Registry.
- Báo cáo này không tuyên bố đã triển khai xác thực Bearer/JWT, cơ chế PULL `/receive`, hoặc fault injection; đây là các hạng mục tích hợp/hệ thống riêng.

## Thành phần chính

- `broker/broker.go`: API Broker, ACK, worker, timeout và lịch retry.
- `broker/store.go`: giao diện lưu trữ, `MemoryStore`, idempotency và lease.
- `api.go`: endpoint `/send`, `/ack`, `/messages/{message_id}/ack` và kiểm tra request.
- `broker/broker_test.go`, `main_test.go`: kiểm thử logic Reliability và API.
