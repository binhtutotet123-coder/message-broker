package main

import (
	"context"
	"testing"
	"time"
)

// envLookup builds a lookup function over a fixed set of variables.
func envLookup(vars map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := vars[key]
		return value, ok
	}
}

// TestLoadConfigDefaults keeps `go run .` behaving like before for local development.
func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(envLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":8080" || cfg.RetryInterval != 5*time.Second || cfg.RegistryCacheTTL != 30*time.Second {
		t.Fatalf("defaults = %+v", cfg)
	}
	if cfg.AgentRoutes["agent_B"] != "http://localhost:8081/webhook/receive" || len(cfg.AgentRoutes) != 1 {
		t.Fatalf("default routes = %v", cfg.AgentRoutes)
	}
	if cfg.RegistryLookupURL != "" || cfg.FaultInjection {
		t.Fatalf("registry/fault defaults = %q/%t, want off", cfg.RegistryLookupURL, cfg.FaultInjection)
	}
}

// TestLoadConfigOverrides covers the settings docker-compose uses.
func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(envLookup(map[string]string{
		"BROKER_ADDR":         ":9090",
		"RETRY_INTERVAL":      "2s",
		"AGENT_ROUTES":        "",
		"REGISTRY_LOOKUP_URL": "http://registry:8082/agents/{agent_id}",
		"REGISTRY_CACHE_TTL":  "0s",
		"FAULT_INJECTION":     "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":9090" || cfg.RetryInterval != 2*time.Second || cfg.RegistryCacheTTL != 0 {
		t.Fatalf("overrides = %+v", cfg)
	}
	if len(cfg.AgentRoutes) != 0 {
		t.Fatalf("AGENT_ROUTES=\"\" left routes %v, want none", cfg.AgentRoutes)
	}
	if !cfg.FaultInjection || cfg.RegistryLookupURL == "" {
		t.Fatalf("registry/fault = %q/%t", cfg.RegistryLookupURL, cfg.FaultInjection)
	}
	if _, err := newResolver(cfg); err != nil {
		t.Fatalf("newResolver: %v", err)
	}
}

// TestLoadConfigRejectsInvalidValues reports misconfiguration at startup.
func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"retry not a duration": {"RETRY_INTERVAL": "5"},
		"retry zero":           {"RETRY_INTERVAL": "0s"},
		"negative cache ttl":   {"REGISTRY_CACHE_TTL": "-1s"},
		"bad route":            {"AGENT_ROUTES": "agent_B"},
		"bad fault flag":       {"FAULT_INJECTION": "yes please"},
		"no routing at all":    {"AGENT_ROUTES": ""},
		"empty listen addr":    {"BROKER_ADDR": ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(envLookup(vars)); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}

	cfg, err := loadConfig(envLookup(map[string]string{"REGISTRY_LOOKUP_URL": "http://registry/agents"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newResolver(cfg); err == nil {
		t.Fatal("registry URL without {agent_id} was accepted")
	}
}

// TestNewResolverPrefersStaticRoutes lets a local override win over the registry.
func TestNewResolverPrefersStaticRoutes(t *testing.T) {
	cfg, err := loadConfig(envLookup(map[string]string{
		"AGENT_ROUTES":        "agent_B=http://override:9000/hook",
		"REGISTRY_LOOKUP_URL": "http://127.0.0.1:1/agents/{agent_id}",
	}))
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := newResolver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resolver.Resolve(context.Background(), "agent_B"); err != nil || got != "http://override:9000/hook" {
		t.Fatalf("Resolve(agent_B) = (%q, %v), want static override", got, err)
	}
}
