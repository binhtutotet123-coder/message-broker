// Command mock-agent-b is a receiver used to exercise webhook delivery, ACK and retry.
//
// Environment variables:
//
//	AGENT_ID       agent_id to ACK as (default agent_B)
//	LISTEN_ADDR    listen address (default :8081)
//	BROKER_URL     broker base URL (default http://localhost:8080)
//	ACK_DROP_RATE  probability of not sending an ACK, 0..1 (default 0)
//	ACK_DELAY      delay before each ACK, e.g. 7s (default 0s)
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"message-broker/internal/mockagent"
)

// main starts the mock receiver.
func main() {
	ackDropRate, err := strconv.ParseFloat(getenv("ACK_DROP_RATE", "0"), 64)
	if err != nil {
		log.Fatalf("ACK_DROP_RATE: %v", err)
	}
	ackDelay, err := time.ParseDuration(getenv("ACK_DELAY", "0s"))
	if err != nil {
		log.Fatalf("ACK_DELAY: %v", err)
	}

	agentID := getenv("AGENT_ID", "agent_B")
	brokerURL := getenv("BROKER_URL", "http://localhost:8080")
	agent, err := mockagent.New(mockagent.Config{
		ID:          agentID,
		BrokerURL:   brokerURL,
		AckDropRate: ackDropRate,
		AckDelay:    ackDelay,
	})
	if err != nil {
		log.Fatal(err)
	}

	addr := getenv("LISTEN_ADDR", ":8081")
	fmt.Printf("🤖 Agent %s mock đang lắng nghe tại %s (broker %s, ack_drop_rate=%g, ack_delay=%s)...\n",
		agentID, addr, brokerURL, ackDropRate, ackDelay)
	log.Fatal(http.ListenAndServe(addr, agent.Handler()))
}

// getenv returns the variable's value or fallback when it is unset or empty.
func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
