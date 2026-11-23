package routing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestParseRoutes covers the AGENT_ROUTES format and its error cases.
func TestParseRoutes(t *testing.T) {
	routes, err := ParseRoutes(" agent_B=http://localhost:8081/webhook/receive , team_08=https://t8.example/hook ,")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || routes["agent_B"] != "http://localhost:8081/webhook/receive" || routes["team_08"] != "https://t8.example/hook" {
		t.Fatalf("routes = %v", routes)
	}

	if routes, err := ParseRoutes(""); err != nil || len(routes) != 0 {
		t.Fatalf("empty spec = (%v, %v), want empty table", routes, err)
	}

	for _, spec := range []string{
		"agent_B",
		"=http://host/hook",
		"agent_B=",
		"agent_B=ftp://host/hook",
		"agent_B=/relative/path",
		"agent_B=http://a/hook,agent_B=http://b/hook",
	} {
		if _, err := ParseRoutes(spec); err == nil {
			t.Errorf("ParseRoutes(%q) succeeded, want error", spec)
		}
	}
}

// TestStaticResolver returns configured routes and reports unknown agents.
func TestStaticResolver(t *testing.T) {
	resolver, err := NewStaticResolver(map[string]string{"agent_B": "http://agent-b:8081/webhook/receive"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resolver.Resolve(context.Background(), "agent_B"); err != nil || got != "http://agent-b:8081/webhook/receive" {
		t.Fatalf("Resolve(agent_B) = (%q, %v)", got, err)
	}
	if _, err := resolver.Resolve(context.Background(), "unknown"); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("Resolve(unknown) error = %v, want ErrAgentNotFound", err)
	}
	if _, err := NewStaticResolver(map[string]string{"agent_B": "not a url"}); err == nil {
		t.Fatal("invalid static route was accepted")
	}
}

// TestHTTPResolverLooksUpRegistryAndCaches verifies the request shape, response parsing and caching.
func TestHTTPResolverLooksUpRegistryAndCaches(t *testing.T) {
	var hits atomic.Int32
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.EscapedPath() {
		case "/agents/agent_B":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"agent_id":"agent_B","webhook_url":"http://agent-b:8081/webhook/receive"}`))
		case "/agents/team%2F08%20news":
			w.Write([]byte(`{"endpoint":"https://t8.example/hook"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer registry.Close()

	resolver, err := NewHTTPResolver(registry.URL+"/agents/{agent_id}", registry.Client(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	resolver.now = func() time.Time { return now }

	for range 3 {
		got, err := resolver.Resolve(context.Background(), "agent_B")
		if err != nil || got != "http://agent-b:8081/webhook/receive" {
			t.Fatalf("Resolve(agent_B) = (%q, %v)", got, err)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("registry hits with warm cache = %d, want 1", got)
	}

	now = now.Add(time.Minute)
	if _, err := resolver.Resolve(context.Background(), "agent_B"); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("registry hits after cache expiry = %d, want 2", got)
	}

	if got, err := resolver.Resolve(context.Background(), "team/08 news"); err != nil || got != "https://t8.example/hook" {
		t.Fatalf("Resolve with escaped ID = (%q, %v)", got, err)
	}

	if _, err := resolver.Resolve(context.Background(), "unknown"); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("Resolve(unknown) error = %v, want ErrAgentNotFound", err)
	}
}

// TestHTTPResolverReportsRegistryFailures keeps outages and bad data distinct from unknown agents.
func TestHTTPResolverReportsRegistryFailures(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "server error", status: http.StatusInternalServerError, body: `{}`},
		{name: "missing url", status: http.StatusOK, body: `{"agent_id":"x"}`},
		{name: "invalid url", status: http.StatusOK, body: `{"webhook_url":"agent-b:8081"}`},
		{name: "not json", status: http.StatusOK, body: `<html>`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(testCase.status)
				w.Write([]byte(testCase.body))
			}))
			defer registry.Close()

			resolver, err := NewHTTPResolver(registry.URL+"/agents/{agent_id}", registry.Client(), 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = resolver.Resolve(context.Background(), "x")
			if err == nil || errors.Is(err, ErrAgentNotFound) {
				t.Fatalf("error = %v, want a non-NotFound failure", err)
			}
		})
	}

	if _, err := NewHTTPResolver("http://registry/agents", nil, 0); err == nil {
		t.Fatal("lookup URL without placeholder was accepted")
	}
}

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Resolve(ctx context.Context, agentID string) (string, error) {
	return f(ctx, agentID)
}

// TestChainPrefersFoundRoutesAndSurfacesOutages checks fallback order and error priority.
func TestChainPrefersFoundRoutesAndSurfacesOutages(t *testing.T) {
	notFound := resolverFunc(func(context.Context, string) (string, error) { return "", ErrAgentNotFound })
	outage := resolverFunc(func(context.Context, string) (string, error) { return "", errors.New("registry down") })
	found := resolverFunc(func(context.Context, string) (string, error) { return "http://found/hook", nil })

	if got, err := (Chain{notFound, outage, found}).Resolve(context.Background(), "a"); err != nil || got != "http://found/hook" {
		t.Fatalf("chain with a route = (%q, %v), want found route", got, err)
	}
	if _, err := (Chain{notFound, outage}).Resolve(context.Background(), "a"); err == nil || errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("chain with outage error = %v, want outage error", err)
	}
	if _, err := (Chain{notFound, notFound}).Resolve(context.Background(), "a"); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("chain without routes error = %v, want ErrAgentNotFound", err)
	}
}

// TestPullRoutes covers "agent=pull" entries for agents without a webhook.
func TestPullRoutes(t *testing.T) {
	routes, err := ParseRoutes("agent_B=http://agent-b:8081/webhook/receive, laptop=pull, other=PULL")
	if err != nil {
		t.Fatal(err)
	}
	if routes["laptop"] != PullRoute || routes["other"] != PullRoute {
		t.Fatalf("pull routes = %v", routes)
	}
	resolver, err := NewStaticResolver(routes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), "laptop"); !errors.Is(err, ErrPullOnly) {
		t.Fatalf("Resolve(laptop) error = %v, want ErrPullOnly", err)
	}

	// A pull-only agent is known: the chain stops there instead of asking the registry.
	var asked atomic.Int32
	registry := resolverFunc(func(context.Context, string) (string, error) {
		asked.Add(1)
		return "http://registry-route/hook", nil
	})
	if _, err := (Chain{resolver, registry}).Resolve(context.Background(), "laptop"); !errors.Is(err, ErrPullOnly) || asked.Load() != 0 {
		t.Fatalf("chain for pull agent = %v (registry asked %d times), want ErrPullOnly without asking", err, asked.Load())
	}
}

// TestHTTPResolverHonoursRegistryStatus treats UNREGISTERED agents as unknown, as the
// Team 1 Agent Registry keeps them listed with that status after they leave.
func TestHTTPResolverHonoursRegistryStatus(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Path[len("/v1/agents/"):]
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"agent_id":"x","endpoint":"http://agent:9000/webhook/receive","status":"` + status + `"}`))
	}))
	defer registry.Close()

	resolver, err := NewHTTPResolver(registry.URL+"/v1/agents/{agent_id}", registry.Client(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), "UNREGISTERED"); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("UNREGISTERED agent error = %v, want ErrAgentNotFound", err)
	}
	for _, status := range []string{"ONLINE", "SUSPECT", "UNREACHABLE"} {
		if got, err := resolver.Resolve(context.Background(), status); err != nil || got != "http://agent:9000/webhook/receive" {
			t.Fatalf("%s agent = (%q, %v), want its endpoint", status, got, err)
		}
	}
}

// TestPullEndpointConvention covers agents that register https://pull.invalid because
// the Registry's Agent Card requires an endpoint and they have no webhook.
func TestPullEndpointConvention(t *testing.T) {
	for route, want := range map[string]bool{
		"pull":                             true,
		PullEndpoint:                       true,
		"http://PULL.invalid/anything":     true,
		"https://example.com/hook":         false,
		"https://pull.invalid.example.org": false,
	} {
		if got := IsPullRoute(route); got != want {
			t.Errorf("IsPullRoute(%q) = %t, want %t", route, got, want)
		}
	}

	static, err := NewStaticResolver(map[string]string{"laptop": PullEndpoint})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := static.Resolve(context.Background(), "laptop"); !errors.Is(err, ErrPullOnly) {
		t.Fatalf("static pull endpoint error = %v, want ErrPullOnly", err)
	}

	var hits atomic.Int32
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Write([]byte(`{"agent_id":"laptop","endpoint":"https://pull.invalid","status":"ONLINE"}`))
	}))
	defer registry.Close()
	resolver, err := NewHTTPResolver(registry.URL+"/v1/agents/{agent_id}", registry.Client(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := resolver.Resolve(context.Background(), "laptop"); !errors.Is(err, ErrPullOnly) {
			t.Fatalf("registry pull endpoint error = %v, want ErrPullOnly", err)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("registry hits = %d, want 1 (pull answers are cached too)", got)
	}
}
