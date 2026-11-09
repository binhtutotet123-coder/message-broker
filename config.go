package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"message-broker/routing"
)

// defaultAgentRoutes keeps `go run .` working next to the mock agent on localhost.
const defaultAgentRoutes = "agent_B=http://localhost:8081/webhook/receive"

// config holds every runtime setting; each field maps to one environment variable.
type config struct {
	// ListenAddr is BROKER_ADDR, the address the HTTP API listens on.
	ListenAddr string
	// RetryInterval is RETRY_INTERVAL, how long to wait for an ACK before resending.
	RetryInterval time.Duration
	// AgentRoutes is AGENT_ROUTES, a static agent_id=webhook_url table checked first.
	AgentRoutes map[string]string
	// RegistryLookupURL is REGISTRY_LOOKUP_URL, the Service Registry lookup template.
	RegistryLookupURL string
	// RegistryCacheTTL is REGISTRY_CACHE_TTL, how long registry answers are cached.
	RegistryCacheTTL time.Duration
	// FaultInjection is FAULT_INJECTION, which enables the /admin/fault API.
	FaultInjection bool
}

// loadConfig reads settings through lookup (os.LookupEnv in production).
// A variable that is set but empty overrides its default; AGENT_ROUTES="" disables
// static routes so that only the registry is used.
func loadConfig(lookup func(string) (string, bool)) (config, error) {
	get := func(key, fallback string) string {
		if value, ok := lookup(key); ok {
			return strings.TrimSpace(value)
		}
		return fallback
	}

	cfg := config{
		ListenAddr:        get("BROKER_ADDR", ":8080"),
		RegistryLookupURL: get("REGISTRY_LOOKUP_URL", ""),
	}
	if cfg.ListenAddr == "" {
		return config{}, errors.New("BROKER_ADDR must not be empty")
	}

	var err error
	if cfg.RetryInterval, err = time.ParseDuration(get("RETRY_INTERVAL", "5s")); err != nil || cfg.RetryInterval <= 0 {
		return config{}, fmt.Errorf("RETRY_INTERVAL=%q must be a positive duration such as 5s", get("RETRY_INTERVAL", ""))
	}
	if cfg.RegistryCacheTTL, err = time.ParseDuration(get("REGISTRY_CACHE_TTL", "30s")); err != nil || cfg.RegistryCacheTTL < 0 {
		return config{}, fmt.Errorf("REGISTRY_CACHE_TTL=%q must be a duration such as 30s (0s disables caching)", get("REGISTRY_CACHE_TTL", ""))
	}
	if cfg.AgentRoutes, err = routing.ParseRoutes(get("AGENT_ROUTES", defaultAgentRoutes)); err != nil {
		return config{}, fmt.Errorf("AGENT_ROUTES: %w", err)
	}
	if raw := get("FAULT_INJECTION", "false"); raw != "" {
		if cfg.FaultInjection, err = strconv.ParseBool(raw); err != nil {
			return config{}, fmt.Errorf("FAULT_INJECTION must be true or false: %w", err)
		}
	}

	if len(cfg.AgentRoutes) == 0 && cfg.RegistryLookupURL == "" {
		return config{}, errors.New("no routing configured: set AGENT_ROUTES and/or REGISTRY_LOOKUP_URL")
	}
	return cfg, nil
}

// newResolver builds the routing chain: static routes first, then the Service Registry.
func newResolver(cfg config) (routing.Resolver, error) {
	var chain routing.Chain
	if len(cfg.AgentRoutes) > 0 {
		static, err := routing.NewStaticResolver(cfg.AgentRoutes)
		if err != nil {
			return nil, err
		}
		chain = append(chain, static)
	}
	if cfg.RegistryLookupURL != "" {
		registry, err := routing.NewHTTPResolver(cfg.RegistryLookupURL, nil, cfg.RegistryCacheTTL)
		if err != nil {
			return nil, err
		}
		chain = append(chain, registry)
	}
	return chain, nil
}
