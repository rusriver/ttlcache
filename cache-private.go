package ttlcache

import (
	"container/list"
	"time"
)

// updateExpirations updates the expiration queue and notifies
// the cache auto cleaner if needed.
// Not concurrently safe.
func (c *Cache[K, V]) updateExpirations(fresh bool, elem *list.Element) {
	var oldExpiresAt time.Time

	if !c.CacheItems.expQueue.isEmpty() {
		oldExpiresAt = c.CacheItems.expQueue[0].Value.(*Item[K, V]).expiresAt
	}

	if fresh {
		c.CacheItems.expQueue.push(elem)
	} else {
		c.CacheItems.expQueue.update(elem)
	}

	newExpiresAt := c.CacheItems.expQueue[0].Value.(*Item[K, V]).expiresAt

	// check if the closest/soonest expiration timestamp changed
	if newExpiresAt.IsZero() || (!oldExpiresAt.IsZero() && !newExpiresAt.Before(oldExpiresAt)) {
		return
	}

	d := time.Until(newExpiresAt)

	// It's possible that the auto cleaner isn't active or
	// is busy, so we need to drain the channel before
	// sending a new value.
	// Also, since this method is called after locking the items' mutex,
	// we can be sure that there is no other concurrent call of this
	// method
	if len(c.CacheItems.timerCh) > 0 {
		// we need to drain this channel in a select with a default
		// case because it's possible that the auto cleaner
		// read this channel just after we entered this if
		select {
		case d1 := <-c.CacheItems.timerCh:
			if d1 < d {
				d = d1
			}
		default:
		}
	}

	// since the channel has a size 1 buffer, we can be sure
	// that the line below won't block (we can't overfill the buffer
	// because we just drained it)
	c.CacheItems.timerCh <- d
}

// set creates a new item, adds it to the cache and then returns it.
// Not concurrently safe.
func (c *Cache[K, V]) set(key K, value V, ttl time.Duration, touch bool) *Item[K, V] {
	if ttl == DefaultTTL {
		ttl = c.options.ttl
	}

	elem := c.get(key, false)
	if elem != nil {
		// update/overwrite an existing item
		item := elem.Value.(*Item[K, V])
		item.update(value, ttl)
		if touch {
			c.updateExpirations(false, elem)
		}

		return item
	}

	if c.options.capacity != 0 && uint64(len(c.CacheItems.values)) >= c.options.capacity {
		// delete the oldest item
		c.evict(EvictionReasonCapacityReached, c.CacheItems.lru.Back())
	}

	// create a new item
	item := newItem(key, value, ttl)
	elem = c.CacheItems.lru.PushFront(item)
	c.CacheItems.values[key] = elem
	c.updateExpirations(true, elem)

	c.metricsMu.Lock()
	c.metrics.Insertions++
	c.metricsMu.Unlock()

	c.events.insertion.mu.RLock()
	for _, fn := range c.events.insertion.fns {
		fn(item)
	}
	c.events.insertion.mu.RUnlock()

	return item
}

// get retrieves an item from the cache and extends its expiration
// time if 'touch' is set to true.
// It returns nil if the item is not found or is expired.
// Not concurrently safe.
func (c *Cache[K, V]) get(key K, touch bool) *list.Element {
	elem := c.CacheItems.values[key]
	if elem == nil {
		return nil
	}

	item := elem.Value.(*Item[K, V])
	if item.isExpiredUnsafe() {
		return nil
	}

	c.CacheItems.lru.MoveToFront(elem)

	if touch && item.ttl > 0 {
		item.touch()
		c.updateExpirations(false, elem)
	}

	return elem
}

// evict deletes items from the cache.
// If no items are provided, all currently present cache items
// are evicted.
// Not concurrently safe.
func (c *Cache[K, V]) evict(reason EvictionReason, elems ...*list.Element) {
	if len(elems) > 0 {
		c.metricsMu.Lock()
		c.metrics.Evictions += uint64(len(elems))
		c.metricsMu.Unlock()

		c.events.eviction.mu.RLock()
		for i := range elems {
			item := elems[i].Value.(*Item[K, V])
			delete(c.CacheItems.values, item.key)
			c.CacheItems.lru.Remove(elems[i])
			c.CacheItems.expQueue.remove(elems[i])

			for _, fn := range c.events.eviction.fns {
				fn(reason, item)
			}
		}
		c.events.eviction.mu.RUnlock()

		return
	}

	c.metricsMu.Lock()
	c.metrics.Evictions += uint64(len(c.CacheItems.values))
	c.metricsMu.Unlock()

	c.events.eviction.mu.RLock()
	for _, elem := range c.CacheItems.values {
		item := elem.Value.(*Item[K, V])

		for _, fn := range c.events.eviction.fns {
			fn(reason, item)
		}
	}
	c.events.eviction.mu.RUnlock()

	c.CacheItems.values = make(map[K]*list.Element)
	c.CacheItems.lru.Init()
	c.CacheItems.expQueue = newExpirationQueue[K, V]()
}
