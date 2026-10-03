// Package cluster provides distributed multi-node clustering capabilities for Barnacles,
// including virtual-node consistent hash routing and scatter-gather query aggregation.
package cluster

import (
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

var (
	// ErrEmptyRing is returned when lookups are attempted on a ring with no registered nodes.
	ErrEmptyRing = errors.New("hash ring contains no active nodes")
	// ErrNodeNotFound is returned when removing a node that does not exist.
	ErrNodeNotFound = errors.New("node not found in ring")
)

const (
	// DefaultVnodesPerNode is the default number of virtual node positions assigned per physical node.
	DefaultVnodesPerNode = 128
)

// Node represents a physical cluster member and its connection parameters.
type Node struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Weight   int               `json:"weight,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type vnode struct {
	hash   uint64
	nodeID string
}

// HashRing manages virtual node positions on a 64-bit integer ring to provide
// uniform key distribution and minimal key relocation during cluster membership changes.
type HashRing struct {
	mu            sync.RWMutex
	vnodesPerNode int
	nodes         map[string]Node
	ring          []vnode
}

// NewHashRing creates an empty HashRing with the specified virtual nodes per physical node.
func NewHashRing(vnodesPerNode int) *HashRing {
	if vnodesPerNode <= 0 {
		vnodesPerNode = DefaultVnodesPerNode
	}
	return &HashRing{
		vnodesPerNode: vnodesPerNode,
		nodes:         make(map[string]Node),
	}
}

// AddNode registers a physical node on the ring with its proportional virtual positions.
func (h *HashRing) AddNode(node Node) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if node.Weight <= 0 {
		node.Weight = 1
	}
	h.nodes[node.ID] = node
	h.rebuildRingLocked()
}

// RemoveNode unregisters a node and removes its virtual positions from the ring.
func (h *HashRing) RemoveNode(nodeID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.nodes[nodeID]; !exists {
		return ErrNodeNotFound
	}

	delete(h.nodes, nodeID)
	h.rebuildRingLocked()
	return nil
}

// GetNode looks up the primary physical node responsible for the specified routing key.
func (h *HashRing) GetNode(key string) (Node, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return Node{}, ErrEmptyRing
	}

	khash := xxhash.Sum64String(key)
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= khash
	})
	if idx == len(h.ring) {
		idx = 0
	}

	nodeID := h.ring[idx].nodeID
	return h.nodes[nodeID], nil
}

// GetNodes returns up to count distinct physical nodes for the key (useful for replication).
func (h *HashRing) GetNodes(key string, count int) ([]Node, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return nil, ErrEmptyRing
	}

	if count > len(h.nodes) {
		count = len(h.nodes)
	}
	if count <= 0 {
		count = 1
	}

	khash := xxhash.Sum64String(key)
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= khash
	})

	result := make([]Node, 0, count)
	seen := make(map[string]bool, count)

	ringLen := len(h.ring)
	for i := 0; i < ringLen && len(result) < count; i++ {
		curIdx := (idx + i) % ringLen
		nid := h.ring[curIdx].nodeID
		if !seen[nid] {
			seen[nid] = true
			result = append(result, h.nodes[nid])
		}
	}

	return result, nil
}

// Nodes returns a snapshot of all currently active physical nodes in the ring.
func (h *HashRing) Nodes() []Node {
	h.mu.RLock()
	defer h.mu.RUnlock()

	list := make([]Node, 0, len(h.nodes))
	for _, n := range h.nodes {
		list = append(list, n)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	return list
}

// NodeCount returns the total number of distinct physical nodes in the ring.
func (h *HashRing) NodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

func (h *HashRing) rebuildRingLocked() {
	totalVnodes := 0
	for _, n := range h.nodes {
		totalVnodes += h.vnodesPerNode * n.Weight
	}

	h.ring = make([]vnode, 0, totalVnodes)
	for id, n := range h.nodes {
		count := h.vnodesPerNode * n.Weight
		for i := 0; i < count; i++ {
			vnodeKey := id + "#" + strconv.Itoa(i)
			vhash := xxhash.Sum64String(vnodeKey)
			h.ring = append(h.ring, vnode{
				hash:   vhash,
				nodeID: id,
			})
		}
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i].hash < h.ring[j].hash
	})
}
