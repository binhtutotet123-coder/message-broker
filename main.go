package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"message-broker/models"
)

var agentBURL = "http://localhost:8081/webhook/receive"

func sendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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

	// Lấy hoặc tự sinh mã Trace ID để phục vụ việc tracking
	traceID := r.Header.Get("X-Trace-Id")
	if traceID == "" {
		traceID = "default-trace-xyz"
	}

	log.Printf("[Broker][%s] Nhận tin nhắn ID: %s từ [%s] gửi đến [%s]", traceID, envelope.MessageID, envelope.From, envelope.To)

	payloadBytes, _ := json.Marshal(envelope)

	// Gọi bất đồng bộ (goroutine) chuyển tiếp sang Agent B kèm Trace ID
	go forwardMessage(agentBURL, payloadBytes, traceID)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf(`{"status": "Accepted", "trace_id": "%s", "message": "Broker đã tiếp nhận và đang định tuyến"}`, traceID)))
}

func forwardMessage(targetURL string, payload []byte, traceID string) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("POST", targetURL, bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("[Broker Error][%s] Không tạo được HTTP request tới Agent B: %v", traceID, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Trace-Id", traceID)

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Broker Error][%s] Không kết nối được tới Agent đích: %v", traceID, err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		log.Printf("[Broker Success][%s] Agent B đã xử lý thành công! Phản hồi: %s", traceID, string(respBody))
	} else {
		log.Printf("[Broker Warning][%s] Agent B phản hồi mã lỗi HTTP: %s", traceID, resp.Status)
	}
}

func main() {
	http.HandleFunc("/send", sendHandler)
	fmt.Println("🚀 Broker (Phiên bản Bước 3 - Tracing & ACK) đang khởi động tại cổng :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}