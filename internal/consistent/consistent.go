package consistent

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// This implementation doesnt use vnodes and a hash implementation.

type HashFn func(data []byte) uint32

type NodesConsistentMap struct {
	nodeKeys []int
	// key  :consistentHashFn value
	// value:nodes name
	nodes map[int]string

	replicas int

	consistentHashFn HashFn
}

const defaultReplicas = 1

func NewMap(hashFn HashFn) *NodesConsistentMap {
	m := &NodesConsistentMap{
		nodes:    make(map[int]string),
		replicas: defaultReplicas,
	}

	// non-cryptographic hashFn typically used for scenarios where malicious input is impossible.
	if hashFn == nil {
		m.consistentHashFn = crc32.ChecksumIEEE
	} else {
		m.consistentHashFn = hashFn
	}
	return m
}

// AddNode - adding a list of new nodes to the consistent hashing node chain.
func (m *NodesConsistentMap) AddNode(nodeKeys ...string) {
	for _, node := range nodeKeys {
		for replicaIndex := 0; replicaIndex < m.replicas; replicaIndex++ {
			hash := int(m.consistentHashFn([]byte(node + strconv.Itoa(replicaIndex))))
			m.nodeKeys = append(m.nodeKeys, hash)
			m.nodes[hash] = node
		}
	}
	sort.Ints(m.nodeKeys)
}

// GetNode - Gets the node info.
func (m *NodesConsistentMap) GetNode(key string) string {
	if len(m.nodeKeys) == 0 {
		return ""
	}
	hash := int(m.consistentHashFn([]byte(key)))

	// binary search through m.nodeKeys as its sorted.
	index := sort.Search(len(m.nodeKeys), func(i int) bool {
		return m.nodeKeys[i] >= hash
	})

	// get the node addr from the hash function value -> node addr.
	return m.nodes[m.nodeKeys[index%len(m.nodeKeys)]]
}
