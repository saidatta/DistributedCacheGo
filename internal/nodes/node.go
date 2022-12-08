package nodes

import (
	"DistributedCacheGo/internal/cache"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type Node struct {
	name     string
	cache    *cache.Cache
	callback Callback
}

type Callback func(key string) ([]byte, error)

type OOMError struct {
	name string
}

func (e *OOMError) Error() string {
	return fmt.Sprintf("Node %v: Cache Out of Memory", e.name)
}

const defaultAutoClearTime = time.Hour * 1
const defaultRespiration = time.Hour * 3

func NewNode(name string, maxMemory int, callback Callback) *Node {
	node := &Node{
		name:     name,
		cache:    cache.NewCache(maxMemory),
		callback: callback,
	}
	go node.autoClearExpireCache(defaultAutoClearTime)
	return node
}

func (node *Node) autoClearExpireCache(t time.Duration) {
	ticker := time.NewTicker(t)
	for range ticker.C {
		go func() {
			node.cache.CleanUpExpiredCache()
		}()
	}
}

func (node *Node) Name() string {
	return node.name
}

func (node *Node) Memory() int {
	return node.cache.CacheMemory()
}

func (node *Node) Get(key string) ([]byte, error) {
	res, isFound := node.cache.Get(key)
	if isFound {
		return res, nil
	} else {
		cacheNormalizedValue, err := node.callback(key)
		if err != nil {
			return nil, err
		} else {
			err := node.Put(key, cacheNormalizedValue, defaultRespiration)
			if err != nil {
				return nil, &OOMError{node.name}
			} else {
				return cacheNormalizedValue, nil
			}
		}
	}
}

func (node *Node) Put(key string, value []byte, respiration time.Duration) error {
	f := node.cache.Put(key, value, int64(respiration/1000000000))
	if f {
		return nil
	} else {
		return &OOMError{
			name: node.name,
		}
	}
}

const defaultPrefix = "__disCache__"

func (node *Node) StartNodeServer(host string) {
	http.HandleFunc(fmt.Sprintf("/__%s__/", defaultPrefix), func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Query().Get("key")
		log.Printf("[Node: %v] search key: %v", node.name, key)
		v, err := node.Get(key)
		if err != nil {
			log.Printf("[Node: %v] %v", node.name, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(v) == 0 || v == nil {
			log.Printf("[Node: %v] %v", node.name, "effective key, empty value")
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			writer.Header().Set("X-Content-Type-Options", "nosniff")
			writer.WriteHeader(http.StatusNotFound)
			writer.Write([]byte("empty value"))
			return
		}
		writer.Header().Set("Content-Type", "application/octet-stream")
		_, err = writer.Write(v)
		if err != nil {
			log.Printf("[Node: %v] %v", node.name, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	if strings.HasPrefix(host, "localhost") {
		log.Printf("[Node: %v] start listen [ http://%v ]", node.name, host)
	} else if strings.HasPrefix(host, "http://") {
		log.Printf("[Node: %v] start listen [ %v ]", node.name, host)
	} else if strings.HasPrefix(host, ":") {
		log.Printf("[Node: %v] start listen [ http://localhost%v ]", node.name, host)
	} else {
		log.Printf("[Node: %v] start listen [ %v ]", node.name, host)
	}
	http.ListenAndServe(host, nil)
}
