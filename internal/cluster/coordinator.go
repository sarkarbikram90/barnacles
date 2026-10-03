package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

// NodeQueryStat contains query metrics returned by a single cluster node.
type NodeQueryStat struct {
	Duration time.Duration `json:"duration"`
	Count    int           `json:"count"`
	Error    string        `json:"error,omitempty"`
}

// QueryResult encapsulates the aggregated scatter-gather query results across cluster nodes.
type QueryResult struct {
	Events    []logentry.LogEntry      `json:"events"`
	NodeStats map[string]NodeQueryStat `json:"node_stats"`
	Errors    []string                 `json:"errors,omitempty"`
}

// QueryClient executes a log query against a remote node.
type QueryClient interface {
	Query(ctx context.Context, address string, q logentry.Query) ([]logentry.LogEntry, error)
}

// HTTPQueryClient implements QueryClient using standard HTTP.
type HTTPQueryClient struct {
	httpClient *http.Client
}

// NewHTTPQueryClient creates an HTTPQueryClient with the specified timeout.
func NewHTTPQueryClient(timeout time.Duration) *HTTPQueryClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPQueryClient{
		httpClient: &http.Client{Timeout: timeout},
	}
}

// Query executes a GET /api/v1/query request against the remote node address.
func (c *HTTPQueryClient) Query(ctx context.Context, address string, q logentry.Query) ([]logentry.LogEntry, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("invalid node address %q: %w", address, err)
	}

	u.Path = "/api/v1/query"
	queryVals := u.Query()
	if q.Host != "" {
		queryVals.Set("host", q.Host)
	}
	if q.Source != "" {
		queryVals.Set("source", q.Source)
	}
	if q.Level != "" {
		queryVals.Set("level", q.Level)
	}
	if q.Search != "" {
		queryVals.Set("search", q.Search)
	}
	if !q.StartTime.IsZero() {
		queryVals.Set("start_time", q.StartTime.Format(time.RFC3339))
	}
	if !q.EndTime.IsZero() {
		queryVals.Set("end_time", q.EndTime.Format(time.RFC3339))
	}
	if q.Limit > 0 {
		queryVals.Set("limit", strconv.Itoa(q.Limit))
	}
	u.RawQuery = queryVals.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create query request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query node %s: %w", address, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node %s returned status %d", address, resp.StatusCode)
	}

	var results struct {
		Events []logentry.LogEntry `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("decode query response from %s: %w", address, err)
	}

	return results.Events, nil
}

// CoordinatorConfig configures the scatter-gather query coordinator.
type CoordinatorConfig struct {
	LocalNodeID    string
	QueryTimeout   time.Duration
	AllowPartial   bool // If true, partial results are returned even if some nodes fail
	PartitionKeyFn PartitionKeyFunc
}

// Coordinator coordinates scatter-gather log queries across cluster nodes.
type Coordinator struct {
	cfg         CoordinatorConfig
	ring        *HashRing
	localStore  store.LogStore
	queryClient QueryClient
}

// NewCoordinator creates a new scatter-gather Query Coordinator.
func NewCoordinator(cfg CoordinatorConfig, ring *HashRing, localStore store.LogStore, client QueryClient) *Coordinator {
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 10 * time.Second
	}
	if cfg.PartitionKeyFn == nil {
		cfg.PartitionKeyFn = DefaultPartitionKey
	}
	if client == nil {
		client = NewHTTPQueryClient(cfg.QueryTimeout)
	}

	return &Coordinator{
		cfg:         cfg,
		ring:        ring,
		localStore:  localStore,
		queryClient: client,
	}
}

// Query executes a scatter-gather query across cluster nodes, performing parallel dispatch,
// timestamp-sorted merging, deduplication, and limit enforcement.
func (c *Coordinator) Query(ctx context.Context, q logentry.Query) (QueryResult, error) {
	q.Normalize()

	queryCtx, cancel := context.WithTimeout(ctx, c.cfg.QueryTimeout)
	defer cancel()

	// 1. Determine target nodes: partition pruning optimization
	var targetNodes []Node
	if q.Host != "" && q.Source != "" {
		key := q.Host + "/" + q.Source
		targetNode, err := c.ring.GetNode(key)
		if err == nil {
			targetNodes = []Node{targetNode}
		}
	}

	if len(targetNodes) == 0 {
		targetNodes = c.ring.Nodes()
	}

	if len(targetNodes) == 0 {
		// If ring is empty, fallback to local store if present
		if c.localStore != nil {
			events, err := c.localStore.Query(queryCtx, q)
			if err != nil {
				return QueryResult{Errors: []string{err.Error()}}, err
			}
			return QueryResult{
				Events: events,
				NodeStats: map[string]NodeQueryStat{
					c.cfg.LocalNodeID: {Count: len(events)},
				},
			}, nil
		}
		return QueryResult{Errors: []string{ErrEmptyRing.Error()}}, ErrEmptyRing
	}

	// 2. Scatter: execute sub-queries in parallel
	type nodeResult struct {
		nodeID   string
		events   []logentry.LogEntry
		duration time.Duration
		err      error
	}

	resultsCh := make(chan nodeResult, len(targetNodes))
	var wg sync.WaitGroup

	for _, node := range targetNodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			start := time.Now()

			if n.ID == c.cfg.LocalNodeID && c.localStore != nil {
				events, err := c.localStore.Query(queryCtx, q)
				resultsCh <- nodeResult{
					nodeID:   n.ID,
					events:   events,
					duration: time.Since(start),
					err:      err,
				}
				return
			}

			events, err := c.queryClient.Query(queryCtx, n.Address, q)
			resultsCh <- nodeResult{
				nodeID:   n.ID,
				events:   events,
				duration: time.Since(start),
				err:      err,
			}
		}(node)
	}

	wg.Wait()
	close(resultsCh)

	// 3. Gather: merge, deduplicate, and sort
	stats := make(map[string]NodeQueryStat, len(targetNodes))
	var (
		allEvents []logentry.LogEntry
		errors    []string
		seenIDs   = make(map[string]bool)
	)

	for res := range resultsCh {
		stat := NodeQueryStat{
			Duration: res.duration,
			Count:    len(res.events),
		}

		if res.err != nil {
			errMsg := fmt.Sprintf("node %s: %v", res.nodeID, res.err)
			stat.Error = errMsg
			errors = append(errors, errMsg)
		} else {
			for _, e := range res.events {
				if !seenIDs[e.ID] {
					seenIDs[e.ID] = true
					allEvents = append(allEvents, e)
				}
			}
		}

		stats[res.nodeID] = stat
	}

	if len(errors) > 0 && !c.cfg.AllowPartial && len(allEvents) == 0 {
		return QueryResult{NodeStats: stats, Errors: errors}, fmt.Errorf("scatter query failed: %s", errors[0])
	}

	// Sort globally by timestamp descending
	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].Timestamp.After(allEvents[j].Timestamp)
	})

	// Enforce global limit
	if q.Limit > 0 && len(allEvents) > q.Limit {
		allEvents = allEvents[:q.Limit]
	}

	return QueryResult{
		Events:    allEvents,
		NodeStats: stats,
		Errors:    errors,
	}, nil
}
