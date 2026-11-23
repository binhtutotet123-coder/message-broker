package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"message-broker/broker"
	"message-broker/fault"
	"message-broker/routing"
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
	var pullAgents []string
	for agentID, route := range cfg.AgentRoutes {
		if route == routing.PullRoute {
			pullAgents = append(pullAgents, agentID)
		}
	}
	messageBroker.MarkPullAgents(pullAgents...)
	go messageBroker.RunWorker(context.Background())

	log.Printf("Routing: static routes for %d agent(s) (%d pull-only), registry lookup %q",
		len(cfg.AgentRoutes), len(pullAgents), cfg.RegistryLookupURL)
	server := &apiServer{
		broker:         messageBroker,
		faultInjector:  injector,
		checkRecipient: recipientChecker(resolver, messageBroker),
	}
	fmt.Printf("Broker đang khởi động tại %s...\n", cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, server.handler()))
}

// recipientChecker reports routing.ErrAgentNotFound for an agent that is neither
// routed (AGENT_ROUTES or the Service Registry) nor currently polling GET /receive.
func recipientChecker(resolver routing.Resolver, messageBroker *broker.Broker) func(context.Context, string) error {
	return func(ctx context.Context, agentID string) error {
		if messageBroker.IsPulling(agentID) {
			return nil
		}
		if _, err := resolver.Resolve(ctx, agentID); err != nil && !errors.Is(err, routing.ErrPullOnly) {
			return err
		}
		return nil
	}
}
