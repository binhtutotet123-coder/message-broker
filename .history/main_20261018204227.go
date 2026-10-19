package main

import (
    "fmt"
    "net/http"
)

func main() {
    // API /send tạm thời cho Thành viên 1
    http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, "Broker đã nhận được yêu cầu!")
    })

    fmt.Println("Server đang chạy tại cổng 8080...")
    http.ListenAndServe(":8080", nil)
}