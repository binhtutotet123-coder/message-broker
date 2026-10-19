package main

import (
    "fmt"
    "io"
    "net/http"
)

func main() {
    http.HandleFunc("/webhook/receive", func(w http.ResponseWriter, r *http.Request) {
        body, _ := io.ReadAll(r.Body)
        fmt.Printf("\n🎯 [Agent B ĐÃ NHẬN ĐƯỢC WEBHOOK]:\n%s\n\n", string(body))
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"status": "received"}`))
    })

    fmt.Println("🤖 Agent B mock đang lắng nghe tại :8081...")
    http.ListenAndServe(":8081", nil)
}