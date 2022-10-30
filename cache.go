package ttlcache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Available eviction reasons.
const (
	EvictionReasonDeleted EvictionReason = iota + 1
	EvictionReasonCapacityReached
	EvictionReasonExpired
)

// EvictionReason is used to specify why a certain item was
// evicted/deleted.
type EvictionReason int

// Cache is a synchronised map of items that are automatically removed
// when they expire or the capacity is reached.
type Cache[K comparable, V any] struct {
	CacheItems struct {
		Mu     sync.RWMutex
		values map[K]*list.Element

		// a generic doubly linked list would be more convenient
		// (and more performant?). It's possible that this
		// will be introduced with/in go1.19+
		lru      *list.List
		expQueue expirationQueue[K, V]

		timerCh chan time.Duration
	}

	metricsMu sync.RWMutex
	metrics   Metrics

	events struct {
		insertion struct {
			mu     sync.RWMutex
			nextID uint64
			fns    map[uint64]func(*Item[K, V])
		}
		eviction struct {
			mu     sync.RWMutex
			nextID uint64
			fns    map[uint64]func(EvictionReason, *Item[K, V])
		}
	}

	stopCh  chan struct{}
	options options[K, V]
}

// New creates a new instance of cache.
func New[K comparable, V any](opts ...Option[K, V]) *Cache[K, V] {
	c := &Cache[K, V]{
		stopCh: make(chan struct{}),
	}
	c.CacheItems.values = make(map[K]*list.Element)
	c.CacheItems.lru = list.New()
	c.CacheItems.expQueue = newExpirationQueue[K, V]()
	c.CacheItems.timerCh = make(chan time.Duration, 1) // buffer is important
	c.events.insertion.fns = make(map[uint64]func(*Item[K, V]))
	c.events.eviction.fns = make(map[uint64]func(EvictionReason, *Item[K, V]))

	applyOptions(&c.options, opts...)

	return c
}

// Set creates a new item from the provided key and value, adds
// it to the cache and then returns it. If an item associated with the
// provided key already exists, the new item overwrites the existing one.
func (c *Cache[K, V]) Set(key K, value V) *Item[K, V] {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	return c.set(key, value, c.options.ttl, true)
}

// Set creates a new item from the provided key and value, adds
// it to the cache and then returns it. If an item associated with the
// provided key already exists, the new item overwrites the existing one.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) *Item[K, V] {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	return c.set(key, value, ttl, true)
}

// Set creates a new item from the provided key and value, adds
// it to the cache and then returns it. If an item associated with the
// provided key already exists, the new item overwrites the existing one.
// DOES NOT UPDATE EXPIRATIONS
func (c *Cache[K, V]) SetDontTouch(key K, value V) *Item[K, V] {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	return c.set(key, value, c.options.ttl, false)
}

// Set creates a new item from the provided key and value, adds
// it to the cache and then returns it. If an item associated with the
// provided key already exists, the new item overwrites the existing one.
// DOES NOT UPDATE EXPIRATIONS
func (c *Cache[K, V]) SetWithTTLDontTouch(key K, value V, ttl time.Duration) *Item[K, V] {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	return c.set(key, value, ttl, false)
}

// Get retrieves an item from the cache by the provided key.
// Unless this is disabled, it also extends/touches an item's
// expiration timestamp on successful retrieval.
// If the item is not found, a nil value is returned.
func (c *Cache[K, V]) Get(key K, opts ...Option[K, V]) *Item[K, V] {
	getOpts := options[K, V]{
		loader:            c.options.loader,
		disableTouchOnHit: c.options.disableTouchOnHit,
	}

	applyOptions(&getOpts, opts...)

	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
	}
	elem := c.get(key, !getOpts.disableTouchOnHit)
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Unlock()
	}

	if elem == nil {
		c.metricsMu.Lock()
		c.metrics.Misses++
		c.metricsMu.Unlock()

		if getOpts.loader != nil {
			return getOpts.loader.Load(c, key)
		}

		return nil
	}

	c.metricsMu.Lock()
	c.metrics.Hits++
	c.metricsMu.Unlock()

	return elem.Value.(*Item[K, V])
}

func (c *Cache[K, V]) Transaction(f func(c *Cache[K, V])) {
	c.CacheItems.Mu.Lock()
	defer c.CacheItems.Mu.Unlock()
	c.options.lockingDisabledForTransaction = true
	defer func() { c.options.lockingDisabledForTransaction = false }()

	f(c)
}

// Delete deletes an item from the cache. If the item associated with
// the key is not found, the method is no-op.
func (c *Cache[K, V]) Delete(key K) {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	elem := c.CacheItems.values[key]
	if elem == nil {
		return
	}

	c.evict(EvictionReasonDeleted, elem)
}

// DeleteAll deletes all items from the cache.
func (c *Cache[K, V]) DeleteAll() {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
	}
	c.evict(EvictionReasonDeleted)
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Unlock()
	}
}

// DeleteExpired deletes all expired items from the cache.
func (c *Cache[K, V]) DeleteExpired() {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
		defer c.CacheItems.Mu.Unlock()
	}

	if c.CacheItems.expQueue.isEmpty() {
		return
	}

	e := c.CacheItems.expQueue[0]
	for e.Value.(*Item[K, V]).isExpiredUnsafe() {
		c.evict(EvictionReasonExpired, e)

		if c.CacheItems.expQueue.isEmpty() {
			break
		}

		// expiration queue has a new root
		e = c.CacheItems.expQueue[0]
	}
}

// Touch simulates an item's retrieval without actually returning it.
// Its main purpose is to extend an item's expiration timestamp.
// If the item is not found, the method is no-op.
func (c *Cache[K, V]) Touch(key K) {
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Lock()
	}
	c.get(key, true)
	if !c.options.lockingDisabledForTransaction {
		c.CacheItems.Mu.Unlock()
	}
}

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	c.CacheItems.Mu.RLock()
	defer c.CacheItems.Mu.RUnlock()

	return len(c.CacheItems.values)
}

// Keys returns all keys currently present in the cache.
func (c *Cache[K, V]) Keys() []K {
	c.CacheItems.Mu.RLock()
	defer c.CacheItems.Mu.RUnlock()

	res := make([]K, 0, len(c.CacheItems.values))
	for k := range c.CacheItems.values {
		res = append(res, k)
	}

	return res
}

// Items returns a copy of all items in the cache.
// It does not update any expiration timestamps.
func (c *Cache[K, V]) Items() map[K]*Item[K, V] {
	c.CacheItems.Mu.RLock()
	defer c.CacheItems.Mu.RUnlock()

	items := make(map[K]*Item[K, V], len(c.CacheItems.values))
	for k := range c.CacheItems.values {
		item := c.get(k, false)
		if item != nil {
			items[k] = item.Value.(*Item[K, V])
		}
	}

	return items
}

// Metrics returns the metrics of the cache.
func (c *Cache[K, V]) Metrics() Metrics {
	c.metricsMu.RLock()
	defer c.metricsMu.RUnlock()

	return c.metrics
}

// Start starts an automatic cleanup process that
// periodically deletes expired items.
// It blocks until Stop is called.
func (c *Cache[K, V]) Start() {
	waitDur := func() time.Duration {
		c.CacheItems.Mu.RLock()
		defer c.CacheItems.Mu.RUnlock()

		if !c.CacheItems.expQueue.isEmpty() &&
			!c.CacheItems.expQueue[0].Value.(*Item[K, V]).expiresAt.IsZero() {
			d := time.Until(c.CacheItems.expQueue[0].Value.(*Item[K, V]).expiresAt)
			if d <= 0 {
				// execute immediately
				return time.Microsecond
			}

			return d
		}

		if c.options.ttl > 0 {
			return c.options.ttl
		}

		return time.Hour
	}

	timer := time.NewTimer(waitDur())
	stop := func() {
		if !timer.Stop() {
			// drain the timer chan
			select {
			case <-timer.C:
			default:
			}
		}
	}

	defer stop()

	for {
		select {
		case <-c.stopCh:
			return
		case d := <-c.CacheItems.timerCh:
			stop()
			timer.Reset(d)
		case <-timer.C:
			c.DeleteExpired()
			stop()
			timer.Reset(waitDur())
		}
	}
}

// Stop stops the automatic cleanup process.
// It blocks until the cleanup process exits.
func (c *Cache[K, V]) Stop() {
	c.stopCh <- struct{}{}
}

// OnInsertion adds the provided function to be executed when
// a new item is inserted into the cache. The function is executed
// on a separate goroutine and does not block the flow of the cache
// manager.
// The returned function may be called to delete the subscription function
// from the list of insertion subscribers.
// When the returned function is called, it blocks until all instances of
// the same subscription function return. A context is used to notify the
// subscription function when the returned/deletion function is called.
func (c *Cache[K, V]) OnInsertion(fn func(context.Context, *Item[K, V])) func() {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	c.events.insertion.mu.Lock()
	id := c.events.insertion.nextID
	c.events.insertion.fns[id] = func(item *Item[K, V]) {
		wg.Add(1)
		go func() {
			fn(ctx, item)
			wg.Done()
		}()
	}
	c.events.insertion.nextID++
	c.events.insertion.mu.Unlock()

	return func() {
		cancel()

		c.events.insertion.mu.Lock()
		delete(c.events.insertion.fns, id)
		c.events.insertion.mu.Unlock()

		wg.Wait()
	}
}

// OnEviction adds the provided function to be executed when
// an item is evicted/deleted from the cache. The function is executed
// on a separate goroutine and does not block the flow of the cache
// manager.
// The returned function may be called to delete the subscription function
// from the list of eviction subscribers.
// When the returned function is called, it blocks until all instances of
// the same subscription function return. A context is used to notify the
// subscription function when the returned/deletion function is called.
func (c *Cache[K, V]) OnEviction(fn func(context.Context, EvictionReason, *Item[K, V])) func() {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	c.events.eviction.mu.Lock()
	id := c.events.eviction.nextID
	c.events.eviction.fns[id] = func(r EvictionReason, item *Item[K, V]) {
		wg.Add(1)
		go func() {
			fn(ctx, r, item)
			wg.Done()
		}()
	}
	c.events.eviction.nextID++
	c.events.eviction.mu.Unlock()

	return func() {
		cancel()

		c.events.eviction.mu.Lock()
		delete(c.events.eviction.fns, id)
		c.events.eviction.mu.Unlock()

		wg.Wait()
	}
}

// Loader is an interface that handles missing data loading.
type Loader[K comparable, V any] interface {
	// Load should execute a custom item retrieval logic and
	// return the item that is associated with the key.
	// It should return nil if the item is not found/valid.
	// The method is allowed to fetch data from the cache instance
	// or update it for future use.
	Load(c *Cache[K, V], key K) *Item[K, V]
}

// LoaderFunc type is an adapter that allows the use of ordinary
// functions as data loaders.
type LoaderFunc[K comparable, V any] func(*Cache[K, V], K) *Item[K, V]

// Load executes a custom item retrieval logic and returns the item that
// is associated with the key.
// It returns nil if the item is not found/valid.
func (l LoaderFunc[K, V]) Load(c *Cache[K, V], key K) *Item[K, V] {
	return l(c, key)
}

// SuppressedLoader wraps another Loader and suppresses duplicate
// calls to its Load method.
type SuppressedLoader[K comparable, V any] struct {
	Loader[K, V]

	group *singleflight.Group
}

// Load executes a custom item retrieval logic and returns the item that
// is associated with the key.
// It returns nil if the item is not found/valid.
// It also ensures that only one execution of the wrapped Loader's Load
// method is in-flight for a given key at a time.
func (l *SuppressedLoader[K, V]) Load(c *Cache[K, V], key K) *Item[K, V] {
	// there should be a better/generic way to create a
	// singleflight Group's key. It's possible that a generic
	// singleflight.Group will be introduced with/in go1.19+
	strKey := fmt.Sprint(key)

	// the error can be discarded since the singleflight.Group
	// itself does not return any of its errors, it returns
	// the error that we return ourselves in the func below, which
	// is also nil
	res, _, _ := l.group.Do(strKey, func() (interface{}, error) {
		item := l.Loader.Load(c, key)
		if item == nil {
			return nil, nil
		}

		return item, nil
	})
	if res == nil {
		return nil
	}

	return res.(*Item[K, V])
}
