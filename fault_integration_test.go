package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"message-broker/broker"
	"message-broker/fault"
	"message-broker/internal/mockagent"
	"message-broker/routing"
)

// TestFaultInjectionEveryMessageDeliveredAndProcessedOnce runs the broker and a mock agent
// over real HTTP while webhooks are dropped, delayed and duplicated and ACKs are lost.
// It checks the week-4 goal: every message ends Delivered and the agent processes each
// message exactly once even though it receives some of them several times.
func TestFaultInjectionEveryMessageDeliveredAndProcessedOnce(t *testing.T) {
	const messages = 20

	brokerServer := httptest.NewUnstartedServer(nil)
	brokerURL := "http://" + brokerServer.Listener.Addr().String()

	agent, err := mockagent.New(mockagent.Config{
		ID:          "agent_B",
		BrokerURL:   brokerURL,
		AckDropRate: 0.3,
		Random:      rand.New(rand.NewPCG(3, 4)).Float64,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentServer := httptest.NewServer(agent.Handler())
	defer agentServer.Close()

	resolver, err := routing.NewStaticResolver(map[string]string{"agent_B": agentServer.URL + "/webhook/receive"})
	if err != nil {
		t.Fatal(err)
	}
	injector := fault.New(rand.New(rand.NewPCG(1, 2)).Float64)
	if err := injector.SetConfig(fault.Config{DropRate: 0.4, DuplicateRate: 0.3, DelayMS: 5}); err != nil {
		t.Fatal(err)
	}
	deliver := injector.Wrap(webhookDelivery{resolver: resolver, client: &http.Client{Timeout: 2 * time.Second}}.deliver)
	messageBroker, err := broker.New(deliver, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	brokerServer.Config.Handler = (&apiServer{broker: messageBroker, faultInjector: injector}).handler()
	brokerServer.Start()
	defer brokerServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go messageBroker.RunWorker(ctx)

	ids := make([]string, messages)
	for i := range ids {
		ids[i] = fmt.Sprintf("fault-%02d", i)
		body := fmt.Sprintf(`{"message_id":%q,"timestamp":"2026-11-09T07:00:00Z","from":"agent_A","to":"agent_B","type":"request","payload":{"n":%d}}`, ids[i], i)
		req, err := http.NewRequest(http.MethodPost, brokerURL+"/send", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(traceIDHeader, "trace-"+ids[i])
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("/send %s status = %d, want 202", ids[i], resp.StatusCode)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		var pending []string
		for _, id := range ids {
			snapshot, found, err := messageBroker.Get(id)
			if err != nil || !found {
				t.Fatalf("lookup %s = (found=%t, err=%v)", id, found, err)
			}
			if snapshot.Status != broker.StatusDelivered {
				pending = append(pending, id)
			}
		}
		if len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still pending after 15s: %v (agent %+v, faults %+v)", pending, agent.Stats(), injector.Stats())
		}
		time.Sleep(50 * time.Millisecond)
	}

	agentStats := agent.Stats()
	faultStats := injector.Stats()
	t.Logf("agent %+v, faults %+v", agentStats, faultStats)

	if agentStats.Processed != messages {
		t.Fatalf("agent processed %d unique messages, want exactly %d", agentStats.Processed, messages)
	}
	for _, id := range ids {
		if !agent.Seen(id) {
			t.Fatalf("agent never processed %s", id)
		}
	}
	if faultStats.Dropped == 0 || faultStats.Duplicated == 0 || agentStats.AcksDropped == 0 {
		t.Fatalf("faults were not exercised: faults %+v, agent %+v", faultStats, agentStats)
	}
	if agentStats.Duplicates == 0 || agentStats.Received <= agentStats.Processed {
		t.Fatalf("agent saw no duplicate deliveries: %+v", agentStats)
	}
}
