package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"message-broker/models"
)

// Địa chỉ giả lập của Agent B (sẽ chạy ở cổng 8081 để test)
var agentBURL = "http://localhost:8081/webhook/receive"

func sendHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Chỉ chấp nhận phương thức POST theo đúng swagger.yaml
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Đọc và parse JSON từ Request Body vào Struct
	var envelope models.MessageEnvelope
	err := json.NewDecoder(r.Body).Decode(&envelope)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(models.ErrorModel{
			Code:    400,
			Message: "Sai định dạng JSON: " + err.Error(),
		})
		return
	}

	log.Printf("[Broker] Nhận tin nhắn ID: %s từ [%s] gửi đến [%s]", envelope.MessageID, envelope.From, envelope.To)

	// 3. Chuẩn bị dữ liệu để đẩy tiếp sang Agent B qua Webhook
	payloadBytes, _ := json.Marshal(envelope)
	traceID := r.Header.Get("X-Trace-Id")

	// Gọi bất đồng bộ (goroutine) để chuyển tiếp tin nhắn sang Agent đích ngay lập tức
	go forwardMessage(agentBURL, payloadBytes, traceID)

	// 4. Trả về mã phản hồi 202 Accepted cho Agent nguồn theo swagger.yaml
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "Accepted", "message": "Broker đã nhận và đang định tuyến"}`))
}

// Hàm đẩy tin nhắn sang Agent B (Cơ chế PUSH Webhook)
func forwardMessage(targetURL string, payload []byte, traceID string) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("POST", targetURL, bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("[Broker Error] Không tạo được HTTP request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if traceID != "" {
		req.Header.Set("X-Trace-Id", traceID)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Broker Error] Không kết nối được tới Agent đích tại %s: %v", targetURL, err)
		return
	}
	defer resp.Body.Close()

	log.Printf("[Broker Success] Đã đẩy tin nhắn sang Agent B thành công! Trạng thái phản hồi: %s", resp.Status)
}

func main() {
	// Đăng ký endpoint /send cho Broker
	http.HandleFunc("/send", sendHandler)

	fmt.Println("🚀 Broker đang khởi động tại cổng :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}