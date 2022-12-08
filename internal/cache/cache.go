package cache

import (
	"encoding/binary"
	"sync"
	"time"
)

type Cache struct {
	core           map[string]*CacheItem
	memoryBytes    int
	maxMemoryBytes int
	mutex          sync.Mutex
}

func NewCache(maxMemory int) *Cache {
	// maxMemoryBytes in bytes.
	return &Cache{
		core:           make(map[string]*CacheItem),
		memoryBytes:    0,
		maxMemoryBytes: maxMemory,
	}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	if res, ok := c.core[key]; ok {
		if time.Now().Unix() > res.expiration {
			delete(c.core, key)
			c.memoryBytes -= binary.Size(res.value)
			return nil, false
		}
		return res.value, true
	} else {
		return nil, false
	}
}

// Put key, target value is the cache kv pair, and expiration is epoch.
func (c *Cache) Put(key string, targetValue []byte, expiration int64) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	size := binary.Size(len(targetValue))
	v, t := c.core[key]
	// key exist

	binarySizeStoredValue := binary.Size(len(v.value))
	binarySizeTargetValue := binary.Size(len(targetValue))

	if t {
		if c.maxMemoryBytes-c.memoryBytes >= binarySizeStoredValue-binarySizeTargetValue {
			c.memoryBytes = c.memoryBytes + (binarySizeStoredValue - binarySizeTargetValue)
			c.core[key] = NewCacheValue(targetValue, expiration)
			return true
		} else {
			return false
		}
	}
	if c.maxMemoryBytes-c.memoryBytes < size {
		return false
	}
	c.core[key] = NewCacheValue(targetValue, expiration)
	c.memoryBytes += size
	return true
}

func (c *Cache) CacheMemory() int {
	return c.memoryBytes
}

func (c *Cache) MaxCacheMemory() int {
	return c.maxMemoryBytes
}

func (c *Cache) CleanUpExpiredCache() {
	for k, v := range c.core {
		// if current time exceeded expiration.
		if time.Now().Unix() > v.expiration {
			delete(c.core, k)
			c.memoryBytes -= binary.Size(len(v.value))
		}
	}
}
