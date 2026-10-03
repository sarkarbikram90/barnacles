package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/sender"
	"github.com/sarkarbikram90/barnacles/internal/store"
)

// PartitionKeyFunc computes the hash key used to route a LogEntry to a storage node.
type PartitionKeyFunc func(entry *logentry.LogEntry) string

// DefaultPartitionKey partitions entries by host and source.
func DefaultPartitionKey(entry *logentry.LogEntry) string {
	return entry.Host + "/" + entry.Source
}

// RemoteClient sends log batches to a peer node.
type RemoteClient interface {
	Send(ctx context.Context, agentID string, events []logentry.LogEntry) (*logentry.IngestResponse, error)
}

// RouterConfig defines routing parameters for the IngestRouter.
type RouterConfig struct {
	LocalNodeID    string
	PartitionKeyFn PartitionKeyFunc
	SenderTimeout  time.Duration
	WireFormat     string // "protobuf" or "json"
}

// IngestRouter coordinates routing of incoming log batches across cluster nodes.
type IngestRouter struct {
	cfg         RouterConfig
	ring        *HashRing
	localStore  store.LogStore
	clientsMu   sync.RWMutex
	peerClients map[string]RemoteClient
}

// NewIngestRouter creates a new IngestRouter.
func NewIngestRouter(cfg RouterConfig, ring *HashRing, localStore store.LogStore) *IngestRouter {
	if cfg.PartitionKeyFn == nil {
		cfg.PartitionKeyFn = DefaultPartitionKey
	}
	if cfg.SenderTimeout <= 0 {
		cfg.SenderTimeout = 5 * time.Second
	}
	if cfg.WireFormat == "" {
		cfg.WireFormat = "protobuf"
	}

	return &IngestRouter{
		cfg:         cfg,
		ring:        ring,
		localStore:  localStore,
		peerClients: make(map[string]RemoteClient),
	}
}

// SetPeerClient sets a custom RemoteClient for a given node ID (useful for testing or custom transports).
func (r *IngestRouter) SetPeerClient(nodeID string, client RemoteClient) {
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	r.peerClients[nodeID] = client
}

func (r *IngestRouter) getClient(node Node) (RemoteClient, error) {
	r.clientsMu.RLock()
	client, exists := r.peerClients[node.ID]
	r.clientsMu.RUnlock()
	if exists {
		return client, nil
	}

	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	if client, exists = r.peerClients[node.ID]; exists {
		return client, nil
	}

	snd, err := sender.New(sender.Config{
		URL:         node.Address,
		Timeout:     r.cfg.SenderTimeout,
		Format:      r.cfg.WireFormat,
		Compression: "zstd",
	})
	if err != nil {
		return nil, fmt.Errorf("create sender for node %q at %s: %w", node.ID, node.Address, err)
	}

	r.peerClients[node.ID] = snd
	return snd, nil
}

// RouteBatch partitions an incoming batch across cluster storage nodes based on consistent hashing.
// Local events are written directly to localStore, while remote events are forwarded to peers.
func (r *IngestRouter) RouteBatch(ctx context.Context, agentID string, entries []logentry.LogEntry) (*logentry.IngestResponse, error) {
	if len(entries) == 0 {
		return &logentry.IngestResponse{Status: "ok", Accepted: 0}, nil
	}

	// 1. Group entries by destination node
	nodeBuckets := make(map[string][]logentry.LogEntry)
	nodeMap := make(map[string]Node)

	for i := range entries {
		entry := entries[i]
		key := r.cfg.PartitionKeyFn(&entry)
		targetNode, err := r.ring.GetNode(key)
		if err != nil {
			// If ring is empty or fails, route to local node if available
			targetNode = Node{ID: r.cfg.LocalNodeID}
		}

		nodeBuckets[targetNode.ID] = append(nodeBuckets[targetNode.ID], entry)
		nodeMap[targetNode.ID] = targetNode
	}

	// 2. Dispatch batches concurrently
	type batchResult struct {
		nodeID     string
		accepted   int
		duplicates int
		errors     []string
		err        error
	}

	resultsCh := make(chan batchResult, len(nodeBuckets))
	var wg sync.WaitGroup

	for nodeID, bucket := range nodeBuckets {
		wg.Add(1)
		go func(nid string, b []logentry.LogEntry, node Node) {
			defer wg.Done()

			if nid == r.cfg.LocalNodeID && r.localStore != nil {
				// Local write
				err := r.localStore.Append(ctx, b)
				if err != nil {
					resultsCh <- batchResult{
						nodeID: nid,
						errors: []string{fmt.Sprintf("local append error: %v", err)},
						err:    err,
					}
					return
				}
				resultsCh <- batchResult{
					nodeID:   nid,
					accepted: len(b),
				}
				return
			}

			// Remote peer dispatch
			client, err := r.getClient(node)
			if err != nil {
				resultsCh <- batchResult{
					nodeID: nid,
					errors: []string{err.Error()},
					err:    err,
				}
				return
			}

			resp, err := client.Send(ctx, agentID, b)
			if err != nil {
				resultsCh <- batchResult{
					nodeID: nid,
					errors: []string{fmt.Sprintf("forward to %s failed: %v", nid, err)},
					err:    err,
				}
				return
			}

			resultsCh <- batchResult{
				nodeID:     nid,
				accepted:   resp.Accepted,
				duplicates: resp.Duplicates,
				errors:     resp.Errors,
			}
		}(nodeID, bucket, nodeMap[nodeID])
	}

	wg.Wait()
	close(resultsCh)

	// 3. Aggregate response
	finalResp := &logentry.IngestResponse{
		Status: "ok",
	}

	var firstErr error
	for res := range resultsCh {
		finalResp.Accepted += res.accepted
		finalResp.Duplicates += res.duplicates
		finalResp.Errors = append(finalResp.Errors, res.errors...)
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
	}

	if len(finalResp.Errors) > 0 && finalResp.Accepted == 0 {
		finalResp.Status = "error"
		return finalResp, firstErr
	}

	return finalResp, nil
}
