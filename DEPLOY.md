# Triển khai broker lên server

Hướng dẫn cho server Ubuntu 24.04 thầy cấp (`103.81.84.102`). Server chỉ cho đăng nhập bằng SSH key:
ai cần vào thì tạo key (`ssh-keygen -t ed25519`) và gửi dòng trong file `.pub` cho người quản lý server
để thêm vào `/root/.ssh/authorized_keys`.

```bash
ssh root@103.81.84.102
```

Cấu hình server nằm ở `docker-compose.server.yml`:

- chỉ broker mở cổng ra ngoài (**8080**), Fault Injection luôn tắt;
- `test-agent` (agent_id `agent_B`) chạy nội bộ, không mở cổng, dùng để kiểm tra broker;
- container tự khởi động lại khi lỗi hoặc khi server reboot; log giới hạn 30 MB mỗi container.

## 1. Cài Docker (làm một lần)

```bash
curl -fsSL https://get.docker.com | sh
docker compose version
```

## 2. Lấy code và tạo file cấu hình (làm một lần)

```bash
git clone https://github.com/binhtutotet123-coder/message-broker.git /opt/message-broker
cd /opt/message-broker
cp .env.example .env
```

File `.env` đặt sẵn `COMPOSE_FILE=docker-compose.server.yml`, nên trong thư mục này mọi lệnh
`docker compose ...` đều chạy cấu hình server (không bật nhầm bản demo có Fault Injection).

## 3. Chạy

```bash
cd /opt/message-broker
docker compose up -d --build
docker compose ps
```

Cột STATUS của `broker` phải là `healthy`.

## 4. Kiểm tra

Trên server, gửi một tin tới `test-agent` (mỗi lần thử đổi `message_id`, trùng thì broker trả `duplicate: true`):

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/send -H 'Content-Type: application/json' -H 'X-Trace-Id: smoke-1' \
  -d '{"message_id":"smoke-1","timestamp":"2026-11-17T08:00:00Z","from":"admin","to":"agent_B","type":"request","payload":{"ping":true}}'
docker compose logs test-agent | tail -5
```

Log của `test-agent` phải có dòng `processed message smoke-1`.

Từ máy khác, mở `http://103.81.84.102:8080/healthz` trong trình duyệt, thấy `ok` là các nhóm đã kết nối
được. Nếu không vào được thì cổng 8080 đang bị tường lửa chặn: nhờ thầy mở cổng 8080 (TCP).

## 5. Thêm agent của nhóm khác

1. Nhóm agent gửi `agent_id` và URL webhook của họ. Server phải gọi tới được URL đó
   (IP public hoặc tên miền, không phải `localhost` hay IP mạng nhà).
2. Sửa `AGENT_ROUTES` trong `.env`, các agent cách nhau bởi dấu phẩy, giữ `agent_B`:

   ```text
   AGENT_ROUTES=agent_B=http://test-agent:8081/webhook/receive,team_08_news=http://1.2.3.4:9000/webhook/receive
   ```

3. Áp dụng: `docker compose up -d` (broker khởi động lại để đọc bảng mới).

Khi Team 1 có Service Registry, điền `REGISTRY_LOOKUP_URL` trong `.env` (phải chứa `{agent_id}`) rồi
`docker compose up -d`; agent không có trong `AGENT_ROUTES` sẽ được tra qua registry.

## 6. Cập nhật code mới

```bash
cd /opt/message-broker
git pull
docker compose up -d --build
```

## 7. Lệnh hay dùng

| Việc | Lệnh |
|---|---|
| Xem trạng thái | `docker compose ps` |
| Xem log broker trực tiếp | `docker compose logs -f broker` |
| Khởi động lại | `docker compose restart` |
| Dừng hẳn | `docker compose down` |

## Lưu ý

- Broker hiện lưu tin trong RAM: mỗi lần khởi động lại (thêm agent, cập nhật code) thì tin đang chờ
  giao sẽ mất. Sẽ hết khi có phần lưu database.
- Chưa có xác thực token: ai biết địa chỉ cũng gửi tin được. Sẽ bổ sung khi Team 3 chốt định dạng token.
- Nếu bật tường lửa `ufw` trên server: Docker tự mở cổng của container qua iptables, nên rule `ufw` không
  chặn được cổng 8080. Muốn chặn thì sửa `ports` trong `docker-compose.server.yml`.
