package nodes

import (
	"DistributedCacheGo/internal/consistent"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// user will use key to find data from nodes

type NodeManager struct {
	// key 	 : node name
	// value : node host
	nodes map[string]string

	consistentHashMap *consistent.NodesConsistentMap
}

func NewManger() *NodeManager {
	return &NodeManager{
		nodes:             make(map[string]string),
		consistentHashMap: consistent.NewMap(nil),
	}
}

func (m *NodeManager) AddNode(nodeName string, nodeAddress string) {
	m.nodes[nodeName] = nodeAddress
}

func (m *NodeManager) Register() {
	var allNodes []string
	for k, _ := range m.nodes {
		allNodes = append(allNodes, k)
	}

	// the nodes need to be redistributed.
	m.consistentHashMap.AddNode(allNodes...)
}

func (m *NodeManager) Query(key string) ([]byte, error) {
	host := m.nodes[m.FindNode(key)]
	bytes, err := m.getValueFromRemoteNode(host, key)
	if err != nil {
		return nil, err
	}
	return bytes, err
}

func (m *NodeManager) FindNode(key string) string {
	// nodes name
	nodeName := m.consistentHashMap.GetNode(key)
	log.Printf("[NodeManager] find key in [Node: %v]", nodeName)
	return nodeName
}

func (m *NodeManager) getValueFromRemoteNode(nodeAddress string, key string) ([]byte, error) {
	url := fmt.Sprintf("%v/%v/?key=%v", "http://"+nodeAddress, "__jasmine__", key)
	get, err := http.Get(url)
	if err != nil {
		log.Printf("[NodeManager] %v", err)
		return nil, err
	}
	defer get.Body.Close()
	if get.StatusCode != 200 {
		log.Printf("[nodes %v] return %v", nodeAddress, get.StatusCode)
		return nil, &NodeNoResponse{}
	} else {
		bytes, err := ioutil.ReadAll(get.Body)
		if err != nil {
			return nil, nil
		}
		return bytes, nil
	}
}

func (m *NodeManager) StartManageServer(host string) {
	http.HandleFunc("/api/", func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Query().Get("key")
		log.Printf("[NodeManager] search key: %v", key)
		bytes, err := m.Query(key)
		if err != nil {
			log.Printf("[NodeManager] %v", err)
			http.Error(writer, err.Error(), 404)
			return
		}
		writer.Header().Set("Content-Type", "application/octet-stream")
		_, err = writer.Write(bytes)
		if err != nil {
			log.Printf("[NodeManager] %v", err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	if strings.HasPrefix(host, "localhost") {
		log.Printf("[NodeManager] start listen [ http://%v ]", host)
	} else if strings.HasPrefix(host, "http://") {
		log.Printf("[NodeManager] start listen [ %v ]", host)
	} else if strings.HasPrefix(host, ":") {
		log.Printf("[NodeManager] start listen [ http://localhost%v ]", host)
	} else {
		log.Printf("[NodeManager] start listen [ %v ]", host)
	}
	_ = http.ListenAndServe(host, nil)
}

type NodeNoResponse struct {
}

func (e *NodeNoResponse) Error() string {
	return "Node has no response"
}
