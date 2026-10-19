package models

import "time"

// MessageEnvelope đại diện cho cấu trúc JSON chuẩn truyền giữa các Agent và Broker
type MessageEnvelope struct {
    MessageID      string                 `json:"message_id"`
    ConversationID string                 `json:"conversation_id,omitempty"`
    Timestamp      time.Time              `json:"timestamp"`
    TTL            int                    `json:"ttl,omitempty"`
    From           string                 `json:"from"`
    To             string                 `json:"to"`
    Type           string                 `json:"type"` // request, response, final
    ReplyTo        string                 `json:"reply_to,omitempty"`
    Payload        map[string]interface{} `json:"payload"`
}

// ErrorModel dùng để trả về lỗi chuẩn
type ErrorModel struct {
    Code    int    `json:"code"`
    Message string `json:"message"`
}