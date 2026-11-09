// Command mock-registry stands in for Team 1's Service Registry during local and Docker runs.
//
// It answers GET /agents/{agent_id} with {"agent_id": "...", "webhook_url": "..."} or 404.
//
// Environment variables:
//
//	LISTEN_ADDR  listen address (default :8082)
//	AGENTS       agent_id=webhook_url pairs separated by commas
//	             (default agent_B=http://localhost:8081/webhook/receive)
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"

	"message-broker/models"
	"message-broker/routing"
)

type registryEntry struct {
	AgentID    string `json:"agent_id"`
	WebhookURL string `json:"webhook_url"`
}

// main serves the static registry table.
func main() {
	routes, err := routing.ParseRoutes(getenv("AGENTS", "agent_B=http://localhost:8081/webhook/receive"))
	if err != nil {
		log.Fatalf("AGENTS: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /agents/{agent_id}", func(w http.ResponseWriter, r *http.Request) {
		agentID := r.PathValue("agent_id")
		webhookURL, ok := routes[agentID]
		if !ok {
			log.Printf("[Registry] lookup %q: not found", agentID)
			writeJSON(w, http.StatusNotFound, models.ErrorModel{Code: http.StatusNotFound, Message: "agent not found"})
			return
		}
		log.Printf("[Registry] lookup %q -> %s", agentID, webhookURL)
		writeJSON(w, http.StatusOK, registryEntry{AgentID: agentID, WebhookURL: webhookURL})
	})
	mux.HandleFunc("GET /agents", func(w http.ResponseWriter, _ *http.Request) {
		entries := make([]registryEntry, 0, len(routes))
		for agentID, webhookURL := range routes {
			entries = append(entries, registryEntry{AgentID: agentID, WebhookURL: webhookURL})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].AgentID < entries[j].AgentID })
		writeJSON(w, http.StatusOK, entries)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})

	addr := getenv("LISTEN_ADDR", ":8082")
	fmt.Printf("📒 Mock Service Registry đang lắng nghe tại %s với %d agent...\n", addr, len(routes))
	log.Fatal(http.ListenAndServe(addr, mux))
}

// writeJSON encodes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("[Registry] encode response: %v", err)
	}
}

// getenv returns the variable's value or fallback when it is unset or empty.
func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
