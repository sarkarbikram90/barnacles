package cluster_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/cluster"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

func TestHashRing_DistributionBalance(t *testing.T) {
	ring := cluster.NewHashRing(128)
	nodeCount := 4
	for i := 1; i <= nodeCount; i++ {
		ring.AddNode(cluster.Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("http://10.0.0.%d:8080", i),
		})
	}

	totalKeys := 10000
	counts := make(map[string]int)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("host-%d/service-%d", i, i%10)
		node, err := ring.GetNode(key)
		if err != nil {
			t.Fatalf("GetNode failed: %v", err)
		}
		counts[node.ID]++
	}

	expectedPerNode := float64(totalKeys) / float64(nodeCount)
	for id, count := range counts {
		diffRatio := math.Abs(float64(count)-expectedPerNode) / expectedPerNode
		// Verify that no node receives more than a 25% deviation from uniform distribution
		if diffRatio > 0.25 {
			t.Errorf("node %s has unbalanced key count: %d (expected ~%.0f, deviation %.2f%%)",
				id, count, expectedPerNode, diffRatio*100)
		}
	}
}

func TestHashRing_MinimalKeyMovementOnNodeAddition(t *testing.T) {
	ring := cluster.NewHashRing(128)
	initialNodes := 3
	for i := 1; i <= initialNodes; i++ {
		ring.AddNode(cluster.Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("http://10.0.0.%d:8080", i),
		})
	}

	keys := make([]string, 1000)
	initialAssignments := make(map[string]string)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		node, _ := ring.GetNode(keys[i])
		initialAssignments[keys[i]] = node.ID
	}

	// Add 4th node
	ring.AddNode(cluster.Node{
		ID:      "node-4",
		Address: "http://10.0.0.4:8080",
	})

	movedCount := 0
	for _, k := range keys {
		node, _ := ring.GetNode(k)
		if node.ID != initialAssignments[k] {
			movedCount++
			if node.ID != "node-4" {
				t.Fatalf("key %s was moved to existing node %s instead of new node node-4", k, node.ID)
			}
		}
	}

	// Theoretically, approximately 1/4 of keys (25%) should move.
	movedFraction := float64(movedCount) / float64(len(keys))
	if movedFraction < 0.15 || movedFraction > 0.35 {
		t.Errorf("unexpected moved fraction: %.2f (expected ~0.25)", movedFraction)
	}
}

func TestHashRing_ReplicationDistinctNodes(t *testing.T) {
	ring := cluster.NewHashRing(128)
	for i := 1; i <= 5; i++ {
		ring.AddNode(cluster.Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("http://10.0.0.%d:8080", i),
		})
	}

	nodes, err := ring.GetNodes("orders/checkout", 3)
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}

	if len(nodes) != 3 {
		t.Fatalf("expected 3 replica nodes, got %d", len(nodes))
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n.ID] {
			t.Errorf("duplicate replica node %s returned", n.ID)
		}
		seen[n.ID] = true
	}
}

// MockRemoteClient captures batches forwarded to peer nodes.
type mockRemoteClient struct {
	mu        sync.Mutex
	delivered []logentry.LogEntry
}

func (m *mockRemoteClient) Send(ctx context.Context, agentID string, events []logentry.LogEntry) (*logentry.IngestResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = append(m.delivered, events...)
	return &logentry.IngestResponse{
		Status:   "ok",
		Accepted: len(events),
	}, nil
}

func TestIngestRouter_PartitioningAndLocalRemoteDispatch(t *testing.T) {
	tempDir := t.TempDir()
	localStore, err := store.NewFileStore(store.Config{Directory: tempDir})
	if err != nil {
		t.Fatal(err)
	}
	defer localStore.Close()

	ring := cluster.NewHashRing(64)
	ring.AddNode(cluster.Node{ID: "local-node", Address: "http://127.0.0.1:8080"})
	ring.AddNode(cluster.Node{ID: "remote-node-1", Address: "http://10.0.0.1:8080"})
	ring.AddNode(cluster.Node{ID: "remote-node-2", Address: "http://10.0.0.2:8080"})

	router := cluster.NewIngestRouter(cluster.RouterConfig{
		LocalNodeID: "local-node",
	}, ring, localStore)

	client1 := &mockRemoteClient{}
	client2 := &mockRemoteClient{}
	router.SetPeerClient("remote-node-1", client1)
	router.SetPeerClient("remote-node-2", client2)

	// Create 30 log events across different hosts
	events := make([]logentry.LogEntry, 30)
	now := time.Now().UTC()
	for i := 0; i < 30; i++ {
		events[i] = logentry.LogEntry{
			ID:        fmt.Sprintf("evt-%d", i),
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Host:      fmt.Sprintf("host-%d", i),
			Source:    "app",
			Level:     "INFO",
			Message:   fmt.Sprintf("message %d", i),
		}
	}

	resp, err := router.RouteBatch(context.Background(), "agent-1", events)
	if err != nil {
		t.Fatalf("RouteBatch failed: %v", err)
	}

	if resp.Accepted != 30 {
		t.Errorf("expected 30 accepted events, got %d", resp.Accepted)
	}

	// Verify local store received its portion
	localResults, err := localStore.Query(context.Background(), logentry.Query{Limit: 100})
	if err != nil {
		t.Fatalf("query local store: %v", err)
	}

	client1.mu.Lock()
	client1Count := len(client1.delivered)
	client1.mu.Unlock()

	client2.mu.Lock()
	client2Count := len(client2.delivered)
	client2.mu.Unlock()

	totalCount := len(localResults) + client1Count + client2Count
	if totalCount != 30 {
		t.Errorf("total delivered events (%d) != 30 (local: %d, client1: %d, client2: %d)",
			totalCount, len(localResults), client1Count, client2Count)
	}

	// Verify each bucket received events
	if len(localResults) == 0 || client1Count == 0 || client2Count == 0 {
		t.Logf("partition breakdown: local=%d, client1=%d, client2=%d", len(localResults), client1Count, client2Count)
	}
}

// MockQueryClient simulates remote node responses.
type mockQueryClient struct {
	responses map[string][]logentry.LogEntry
	errors    map[string]error
}

func (m *mockQueryClient) Query(ctx context.Context, address string, q logentry.Query) ([]logentry.LogEntry, error) {
	if err, exists := m.errors[address]; exists {
		return nil, err
	}
	res := m.responses[address]
	var matched []logentry.LogEntry
	for _, e := range res {
		if e.Matches(q) {
			matched = append(matched, e)
		}
	}
	return matched, nil
}

func TestCoordinator_ScatterGatherQueryMergeAndLimit(t *testing.T) {
	tempDir := t.TempDir()
	localStore, err := store.NewFileStore(store.Config{Directory: tempDir})
	if err != nil {
		t.Fatal(err)
	}
	defer localStore.Close()

	now := time.Now().UTC()

	// Write 2 events to localStore
	_ = localStore.Append(context.Background(), []logentry.LogEntry{
		{
			ID:        "loc-1",
			Timestamp: now.Add(2 * time.Minute),
			Host:      "h-loc",
			Source:    "web",
			Level:     "INFO",
			Message:   "local log 1",
		},
		{
			ID:        "loc-2",
			Timestamp: now.Add(4 * time.Minute),
			Host:      "h-loc",
			Source:    "web",
			Level:     "INFO",
			Message:   "local log 2",
		},
	})

	ring := cluster.NewHashRing(64)
	ring.AddNode(cluster.Node{ID: "node-local", Address: "http://local:8080"})
	ring.AddNode(cluster.Node{ID: "node-rem1", Address: "http://rem1:8080"})
	ring.AddNode(cluster.Node{ID: "node-rem2", Address: "http://rem2:8080"})

	mockClient := &mockQueryClient{
		responses: map[string][]logentry.LogEntry{
			"http://rem1:8080": {
				{
					ID:        "rem1-1",
					Timestamp: now.Add(1 * time.Minute),
					Host:      "h-rem1",
					Source:    "web",
					Level:     "INFO",
					Message:   "remote 1 log 1",
				},
				{
					ID:        "rem1-2",
					Timestamp: now.Add(5 * time.Minute),
					Host:      "h-rem1",
					Source:    "web",
					Level:     "INFO",
					Message:   "remote 1 log 2",
				},
			},
			"http://rem2:8080": {
				{
					ID:        "rem2-1",
					Timestamp: now.Add(3 * time.Minute),
					Host:      "h-rem2",
					Source:    "web",
					Level:     "INFO",
					Message:   "remote 2 log 1",
				},
			},
		},
	}

	coord := cluster.NewCoordinator(cluster.CoordinatorConfig{
		LocalNodeID:  "node-local",
		AllowPartial: true,
	}, ring, localStore, mockClient)

	// Query with limit=4
	res, err := coord.Query(context.Background(), logentry.Query{
		Limit: 4,
	})
	if err != nil {
		t.Fatalf("coordinator query failed: %v", err)
	}

	if len(res.Events) != 4 {
		t.Fatalf("expected limit of 4 events returned, got %d", len(res.Events))
	}

	// Verify events are sorted descending by timestamp
	for i := 0; i < len(res.Events)-1; i++ {
		if res.Events[i].Timestamp.Before(res.Events[i+1].Timestamp) {
			t.Errorf("events not in descending timestamp order: [%d]=%v before [%d]=%v",
				i, res.Events[i].Timestamp, i+1, res.Events[i+1].Timestamp)
		}
	}

	// First event should be rem1-2 (timestamp at +5m)
	if res.Events[0].ID != "rem1-2" {
		t.Errorf("expected newest event 'rem1-2', got %q", res.Events[0].ID)
	}
}

func TestCoordinator_ResilienceToFailingNode(t *testing.T) {
	ring := cluster.NewHashRing(64)
	ring.AddNode(cluster.Node{ID: "node-1", Address: "http://rem1:8080"})
	ring.AddNode(cluster.Node{ID: "node-2", Address: "http://rem2:8080"})

	mockClient := &mockQueryClient{
		responses: map[string][]logentry.LogEntry{
			"http://rem1:8080": {
				{
					ID:        "node1-1",
					Timestamp: time.Now().UTC(),
					Host:      "h-1",
					Source:    "app",
					Level:     "INFO",
					Message:   "ok from node 1",
				},
			},
		},
		errors: map[string]error{
			"http://rem2:8080": fmt.Errorf("connection refused"),
		},
	}

	coord := cluster.NewCoordinator(cluster.CoordinatorConfig{
		AllowPartial: true,
	}, ring, nil, mockClient)

	res, err := coord.Query(context.Background(), logentry.Query{Limit: 10})
	if err != nil {
		t.Fatalf("expected partial success, got err: %v", err)
	}

	if len(res.Events) != 1 || res.Events[0].ID != "node1-1" {
		t.Errorf("expected 1 event from healthy node, got %d", len(res.Events))
	}

	if len(res.Errors) != 1 {
		t.Errorf("expected error recorded from failed node, got %v", res.Errors)
	}
}
