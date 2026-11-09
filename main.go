package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"message-broker/broker"
	"message-broker/fault"
)

const (
	traceIDHeader  = "X-Trace-Id"
	defaultTraceID = "default-trace-xyz"
)

var agentHTTPClient = &http.Client{Timeout: 5 * time.Second}

// main loads configuration, wires routing and optional fault injection, and serves the API.
func main() {
	cfg, err := loadConfig(os.LookupEnv)
	if err != nil {
		log.Fatalf("Config: %v", err)
	}
	resolver, err := newResolver(cfg)
	if err != nil {
		log.Fatalf("Routing: %v", err)
	}

	deliver := webhookDelivery{resolver: resolver, client: agentHTTPClient}.deliver
	var injector *fault.Injector
	if cfg.FaultInjection {
		injector = fault.New(nil)
		deliver = injector.Wrap(deliver)
		log.Printf("Fault injection ENABLED: configure it with PUT /admin/fault")
	}

	messageBroker, err := broker.New(deliver, cfg.RetryInterval)
	if err != nil {
		log.Fatal(err)
	}
	go messageBroker.RunWorker(context.Background())

	log.Printf("Routing: static routes for %d agent(s), registry lookup %q", len(cfg.AgentRoutes), cfg.RegistryLookupURL)
	server := &apiServer{broker: messageBroker, faultInjector: injector}
	fmt.Printf("Broker đang khởi động tại %s...\n", cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, server.handler()))
}
