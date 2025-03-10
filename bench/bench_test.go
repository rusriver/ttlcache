package bench

import (
	"fmt"
	"testing"
	"time"

	ttlcache "github.com/rusriver/ttlcache/v3"
)

func BenchmarkCacheSetWithoutTTL(b *testing.B) {
	cache := ttlcache.New[string, string]()

	for n := 0; n < b.N; n++ {
		cache.SetWithTTL(fmt.Sprint(n%1000000), "value", ttlcache.NoTTL)
	}
}

func BenchmarkCacheSetWithGlobalTTL(b *testing.B) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, string](50 * time.Millisecond),
	)

	for n := 0; n < b.N; n++ {
		cache.Set(fmt.Sprint(n%1000000), "value")
	}
}
