package main

import (
    "encoding/json"
    "fmt"
    "message-broker/models"
)

func main() {
    rawJSON := `{"message_id": "m123", "timestamp": "2026-10-19T07:40:00Z", "from": "agent_A", "to": "agent_B", "type": "request", "payload": {"action": "test"}}`

    var m models.MessageEnvelope
    err := json.Unmarshal([]byte(rawJSON), &m)
    if err != nil {
        fmt.Println("❌ Lỗi parse JSON:", err)
    } else {
        fmt.Printf("✅ Parse JSON thành công! MessageID nhận được: %s\n", m.MessageID)
    }
}