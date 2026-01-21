package scorecache

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"

	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Options controls cache behavior.
type Options struct {
	MaxEntries int
	// UsedFractionToRecompute: refresh rate. 0.5 means "recompute after >50% popped"
	UsedFractionToRecompute float64
}

type Cache struct {
	mu sync.Mutex

	maxEntries    int
	recomputeFrac float64

	items    map[ScoreKey]*entry
	inFlight map[ScoreKey]*inFlight
}

// entry keeps a sorted list and a cursor-like pop progress.
// We store only "remaining" list in memory and track how many have been popped.
type entry struct {
	feasibleHash uint64

	// remaining scores, sorted DESC by TotalScore
	remaining []framework.NodePluginScores

	// initial total length when computed (including the first returned top1)
	initialN int

	// popped count since last compute (including the ones returned to scheduler)
	popped int
}

type inFlight struct {
	done         chan struct{}
	result       []framework.NodePluginScores
	err          error
	feasibleHash uint64
}

func New(opts Options) *Cache {
	max := opts.MaxEntries
	if max <= 0 {
		max = 1024
	}
	f := opts.UsedFractionToRecompute
	if f <= 0 || f >= 1 {
		f = 0.5
	}
	return &Cache{
		maxEntries:    max,
		recomputeFrac: f,
		items:         make(map[ScoreKey]*entry, max),
		inFlight:      make(map[ScoreKey]*inFlight),
	}
}

// HashFeasibleNodes computes a stable hash for the feasible node set.
// Sort node names to make the hash independent of iteration order.
func HashFeasibleNodes(feasibleNodes []*framework.NodeInfo) uint64 {
	names := make([]string, 0, len(feasibleNodes))
	for _, ni := range feasibleNodes {
		if ni == nil || ni.Node() == nil {
			continue
		}
		names = append(names, ni.Node().Name)
	}
	sort.Strings(names)

	h := fnv.New64a()
	for _, n := range names {
		_, _ = h.Write([]byte(n))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// GetTop1AndPopOrCompute returns a length-1 slice containing the current top node,
// and pops it from the cached list.
// If cache miss / stale / over-used, it recomputes using compute() and seeds the cache.
func (c *Cache) GetTop1AndPopOrCompute(
	ctx context.Context,
	key ScoreKey,
	feasibleHash uint64,
	compute func() ([]framework.NodePluginScores, error),
) ([]framework.NodePluginScores, error) {

	// 1) Fast path: try hit
	if out, ok, err := c.tryGetTop1AndPop(key, feasibleHash); err != nil {
		return nil, err
	} else if ok {
		return out, nil
	}

	// 2) Miss: compute once (singleflight-lite)
	if _, err := c.computeOnce(ctx, key, feasibleHash, compute); err != nil {
		return nil, err
	}

	// 3) After seeding cache, try again (should hit)
	if out, ok, err := c.tryGetTop1AndPop(key, feasibleHash); err != nil {
		return nil, err
	} else if ok {
		return out, nil
	}

	return nil, errors.New("scorecache: unexpected miss after compute")
}

func (c *Cache) tryGetTop1AndPop(key ScoreKey, feasibleHash uint64) ([]framework.NodePluginScores, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.items[key]
	if e == nil {
		return nil, false, nil
	}

	// feasible set changed => drop
	if e.feasibleHash != feasibleHash {
		delete(c.items, key)
		return nil, false, nil
	}

	// invalid entry
	if e.initialN <= 0 {
		delete(c.items, key)
		return nil, false, nil
	}

	// If popped fraction exceeds threshold => drop and force recompute next time
	usedFrac := float64(e.popped) / float64(e.initialN)
	// over fresh rate >
	if usedFrac > c.recomputeFrac {
		delete(c.items, key)
		return nil, false, nil
	}

	// If nothing left => drop
	if len(e.remaining) == 0 {
		delete(c.items, key)
		return nil, false, nil
	}

	// Pop top of remaining
	top := e.remaining[0]
	e.remaining = e.remaining[1:]
	e.popped++

	// Save back (not strictly necessary since e is pointer, but keep explicit)
	c.items[key] = e

	return []framework.NodePluginScores{top}, true, nil
}

func (c *Cache) computeOnce(
	ctx context.Context,
	key ScoreKey,
	feasibleHash uint64,
	compute func() ([]framework.NodePluginScores, error),
) ([]framework.NodePluginScores, error) {

	// Check inflight / register inflight
	c.mu.Lock()
	if infl, ok := c.inFlight[key]; ok {
		done := infl.done
		c.mu.Unlock()

		select {
		case <-done:
			// Only accept if feasibleHash matches
			if infl.err != nil {
				return nil, infl.err
			}
			if infl.feasibleHash != feasibleHash {
				return nil, errors.New("scorecache: feasible set changed during inflight")
			}
			return clonePluginScores(infl.result), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	infl := &inFlight{done: make(chan struct{})}
	c.inFlight[key] = infl
	c.mu.Unlock()

	// Compute without holding lock
	res, err := compute()
	if err == nil {
		sortPluginScoresDesc(res)
		c.seedEntry(key, feasibleHash, res)
	}

	// Publish inflight result
	c.mu.Lock()
	infl.result = clonePluginScores(res)
	infl.err = err
	infl.feasibleHash = feasibleHash
	close(infl.done)
	delete(c.inFlight, key)
	c.mu.Unlock()

	return res, err
}

// seedEntry seeds cache with a sorted list, but does NOT return a result.
// The caller will immediately tryGetTop1AndPop again to return the first element.
func (c *Cache) seedEntry(key ScoreKey, feasibleHash uint64, sorted []framework.NodePluginScores) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Capacity control (simple eviction; you can replace with LRU later)
	if len(c.items) >= c.maxEntries {
		for k := range c.items {
			delete(c.items, k)
			break
		}
	}

	// If empty result, do not store
	if len(sorted) == 0 {
		delete(c.items, key)
		return
	}

	// Important: since GetTop1AndPop returns from "remaining",
	// we seed remaining with the WHOLE list, and popped=0.
	// Then the first tryGetTop1AndPop will pop the true top1.
	c.items[key] = &entry{
		feasibleHash: feasibleHash,
		remaining:    clonePluginScores(sorted),
		initialN:     len(sorted),
		popped:       0,
	}
}

func sortPluginScoresDesc(scores []framework.NodePluginScores) {
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].TotalScore == scores[j].TotalScore {
			// deterministic tie-break to avoid order bias
			return scores[i].Name < scores[j].Name
		}
		return scores[i].TotalScore > scores[j].TotalScore
	})
}

func clonePluginScores(in []framework.NodePluginScores) []framework.NodePluginScores {
	out := make([]framework.NodePluginScores, len(in))
	copy(out, in)
	return out
}
