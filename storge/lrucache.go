package storge

import (
	"github.com/golang/groupcache/lru"
	"sync"
)

type lruCache struct {
	cache *lru.Cache
	lock  *sync.RWMutex
}

func NewLruCache(maxEntries int) *lruCache{
	return &lruCache{cache: lru.New(maxEntries), lock:new(sync.RWMutex)}
}
func (this *lruCache) Get(key lru.Key) (value interface{}, b bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.cache.Get(key)
}

func (this *lruCache) Remove(key lru.Key) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.cache.Remove(key)
}
func (this *lruCache) Add(key lru.Key, value interface{}) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.cache.Add(key, value)
}
