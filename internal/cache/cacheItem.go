package cache

import "time"

type CacheItem struct {
	value      []byte
	expiration int64
}

func NewCacheValue(value []byte, expiration int64) *CacheItem {
	return &CacheItem{
		value:      value,
		expiration: expiration + time.Now().Unix(),
	}
}
