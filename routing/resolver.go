// Package routing resolves agent identifiers to the webhook URLs that receive their messages.
package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// AgentIDPlaceholder is replaced by the escaped agent ID in a registry lookup URL template.
const AgentIDPlaceholder = "{agent_id}"

const maxRegistryResponseBytes = 1 << 20

// ErrAgentNotFound reports that no route is registered for an agent.
var ErrAgentNotFound = errors.New("agent not found in registry")

// Resolver maps an agent ID to the URL that receives its webhooks.
type Resolver interface {
	Resolve(ctx context.Context, agentID string) (string, error)
}

// StaticResolver serves routes from a fixed table, for example one loaded from AGENT_ROUTES.
type StaticResolver struct {
	routes map[string]string
}

// NewStaticResolver validates and copies a route table.
func NewStaticResolver(routes map[string]string) (*StaticResolver, error) {
	copied := make(map[string]string, len(routes))
	for agentID, webhookURL := range routes {
		if strings.TrimSpace(agentID) == "" {
			return nil, errors.New("route agent ID must not be empty")
		}
		if err := ValidateWebhookURL(webhookURL); err != nil {
			return nil, fmt.Errorf("route for %q: %w", agentID, err)
		}
		copied[agentID] = webhookURL
	}
	return &StaticResolver{routes: copied}, nil
}

// Resolve returns the configured URL for agentID or ErrAgentNotFound.
func (r *StaticResolver) Resolve(_ context.Context, agentID string) (string, error) {
	webhookURL, ok := r.routes[agentID]
	if !ok {
		return "", fmt.Errorf("%w: %q has no static route", ErrAgentNotFound, agentID)
	}
	return webhookURL, nil
}

// ParseRoutes parses "agent_B=http://host:8081/webhook/receive,agent_C=http://other/hook".
// Blank entries are ignored so that an empty string yields an empty table.
func ParseRoutes(spec string) (map[string]string, error) {
	routes := make(map[string]string)
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		agentID, webhookURL, ok := strings.Cut(entry, "=")
		agentID = strings.TrimSpace(agentID)
		webhookURL = strings.TrimSpace(webhookURL)
		if !ok || agentID == "" || webhookURL == "" {
			return nil, fmt.Errorf("route %q must look like agent_id=http://host:port/path", entry)
		}
		if _, duplicate := routes[agentID]; duplicate {
			return nil, fmt.Errorf("route for %q is defined more than once", agentID)
		}
		if err := ValidateWebhookURL(webhookURL); err != nil {
			return nil, fmt.Errorf("route for %q: %w", agentID, err)
		}
		routes[agentID] = webhookURL
	}
	return routes, nil
}

// ValidateWebhookURL requires an absolute http or https URL with a host.
func ValidateWebhookURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL %q must use http or https", raw)
	}
	if parsed.Host == "" {
		return fmt.Errorf("URL %q must include a host", raw)
	}
	return nil
}

// HTTPResolver looks agents up in the Service Registry and caches successful lookups.
//
// The lookup URL is a template such as "http://registry:8082/agents/{agent_id}". A 404
// response means the agent is not registered. A 2xx response must be a JSON object
// carrying the webhook URL in "webhook_url" (or, as fallbacks, "endpoint" or "url").
// Adjust parseRegistryResponse if the registry team settles on another shape.
type HTTPResolver struct {
	lookupURL string
	client    *http.Client
	cacheTTL  time.Duration
	now       func() time.Time

	mu    sync.Mutex
	cache map[string]cachedRoute
}

type cachedRoute struct {
	webhookURL string
	expiresAt  time.Time
}

// NewHTTPResolver creates a registry-backed resolver. A zero cacheTTL disables caching.
func NewHTTPResolver(lookupURL string, client *http.Client, cacheTTL time.Duration) (*HTTPResolver, error) {
	if !strings.Contains(lookupURL, AgentIDPlaceholder) {
		return nil, fmt.Errorf("registry lookup URL %q must contain %s", lookupURL, AgentIDPlaceholder)
	}
	if err := ValidateWebhookURL(strings.ReplaceAll(lookupURL, AgentIDPlaceholder, "agent")); err != nil {
		return nil, fmt.Errorf("registry lookup URL: %w", err)
	}
	if cacheTTL < 0 {
		return nil, errors.New("registry cache TTL must not be negative")
	}
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &HTTPResolver{
		lookupURL: lookupURL,
		client:    client,
		cacheTTL:  cacheTTL,
		now:       time.Now,
		cache:     make(map[string]cachedRoute),
	}, nil
}

// Resolve returns the registered webhook URL, using the cache when it is still fresh.
func (r *HTTPResolver) Resolve(ctx context.Context, agentID string) (string, error) {
	if webhookURL, ok := r.cached(agentID); ok {
		return webhookURL, nil
	}

	// QueryEscape plus %20 for spaces is safe in both path segments and query strings.
	escapedID := strings.ReplaceAll(url.QueryEscape(agentID), "+", "%20")
	lookupURL := strings.ReplaceAll(r.lookupURL, AgentIDPlaceholder, escapedID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, lookupURL, nil)
	if err != nil {
		return "", fmt.Errorf("create registry request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("query registry: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRegistryResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read registry response: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return "", fmt.Errorf("%w: registry has no entry for %q", ErrAgentNotFound, agentID)
	case resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices:
		return "", fmt.Errorf("registry returned HTTP %s for %q", resp.Status, agentID)
	}

	webhookURL, err := parseRegistryResponse(body)
	if err != nil {
		return "", fmt.Errorf("registry entry for %q: %w", agentID, err)
	}
	r.store(agentID, webhookURL)
	return webhookURL, nil
}

// cached returns a fresh cache entry for agentID, if any.
func (r *HTTPResolver) cached(agentID string) (string, bool) {
	if r.cacheTTL == 0 {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.cache[agentID]
	if !ok || !r.now().Before(entry.expiresAt) {
		return "", false
	}
	return entry.webhookURL, true
}

// store remembers a successful lookup until the cache TTL elapses.
func (r *HTTPResolver) store(agentID, webhookURL string) {
	if r.cacheTTL == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[agentID] = cachedRoute{webhookURL: webhookURL, expiresAt: r.now().Add(r.cacheTTL)}
}

// parseRegistryResponse extracts and validates the webhook URL from a registry entry.
func parseRegistryResponse(body []byte) (string, error) {
	var entry struct {
		WebhookURL string `json:"webhook_url"`
		Endpoint   string `json:"endpoint"`
		URL        string `json:"url"`
	}
	if err := json.Unmarshal(body, &entry); err != nil {
		return "", fmt.Errorf("decode JSON: %w", err)
	}
	for _, candidate := range []string{entry.WebhookURL, entry.Endpoint, entry.URL} {
		if candidate != "" {
			if err := ValidateWebhookURL(candidate); err != nil {
				return "", err
			}
			return candidate, nil
		}
	}
	return "", errors.New(`response has no "webhook_url", "endpoint" or "url" field`)
}

// Chain tries resolvers in order and returns the first route found.
type Chain []Resolver

// Resolve returns the first URL any resolver finds. If none finds one, it returns the
// first error other than ErrAgentNotFound (for example a registry outage), so the
// broker retries instead of treating a temporary failure as an unknown agent.
func (c Chain) Resolve(ctx context.Context, agentID string) (string, error) {
	var firstFailure error
	for _, resolver := range c {
		webhookURL, err := resolver.Resolve(ctx, agentID)
		if err == nil {
			return webhookURL, nil
		}
		if !errors.Is(err, ErrAgentNotFound) && firstFailure == nil {
			firstFailure = err
		}
	}
	if firstFailure != nil {
		return "", firstFailure
	}
	return "", fmt.Errorf("%w: %q", ErrAgentNotFound, agentID)
}
