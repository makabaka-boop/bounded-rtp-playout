package rtpreceiver

import (
	"container/heap"
	"sync"
	"time"
)

// Timer is a cancellable one-shot timer, mirroring the subset of *time.Timer
// used by the receiver.
type Timer interface {
	// Stop prevents the timer from firing. It returns true if the call
	// stopped the timer, false if it has already fired or been stopped.
	Stop() bool
}

// Clock is the time source used by the receiver. Production code uses a
// wall-clock implementation; tests drive a VirtualClock deterministically.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, fn func()) Timer
}

// wallClock adapts the standard library clock.
type wallClock struct{}

// WallClock returns a clock backed by the standard library.
func WallClock() Clock { return wallClock{} }

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) AfterFunc(d time.Duration, fn func()) Timer {
	return time.AfterFunc(d, fn)
}

// virtualTimer is a heap entry for VirtualClock.
type virtualTimer struct {
	deadline time.Time
	seq      uint64
	fn       func()
	clock    *VirtualClock
	active   bool
	index    int
}

func (t *virtualTimer) Stop() bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	heap.Remove(c.timers, t.index)
	return true
}

type timerHeap []*virtualTimer

func (h timerHeap) Len() int { return len(h) }

func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].seq < h[j].seq
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(x any) {
	t := x.(*virtualTimer)
	t.index = len(*h)
	*h = append(*h, t)
}

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return t
}

// VirtualClock is a manually driven clock for tests. Timers only fire when
// Advance is called, and they fire in deadline order. A timer scheduled
// while another timer is firing will fire in the same Advance pass if its
// deadline is due.
type VirtualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers *timerHeap
	nextID uint64
}

// NewVirtualClock creates a virtual clock initially at start.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{
		now:    start,
		timers: &timerHeap{},
	}
}

// Now returns the current virtual time.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules fn after at least d of virtual time. A non-positive
// delay makes the timer due at the current time.
func (c *VirtualClock) AfterFunc(d time.Duration, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	t := &virtualTimer{
		deadline: c.now.Add(d),
		seq:      c.nextID,
		fn:       fn,
		clock:    c,
		active:   true,
	}
	heap.Push(c.timers, t)
	return t
}

// Advance moves the clock forward by d, firing every timer whose deadline is
// reached (in deadline order). It returns the number of callbacks that ran.
// Time never moves backwards: a timer callback always observes the deadline
// of the running timer through Now.
func (c *VirtualClock) Advance(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	c.mu.Lock()
	target := c.now.Add(d)
	fired := 0
	for c.timers.Len() > 0 {
		t := (*c.timers)[0]
		if t.deadline.After(target) {
			break
		}
		heap.Pop(c.timers)
		t.active = false
		c.now = t.deadline
		fn := t.fn
		// Call user code without holding the lock: callbacks are allowed to
		// schedule new timers or stop existing ones.
		c.mu.Unlock()
		fn()
		c.mu.Lock()
		fired++
	}
	c.now = target
	c.mu.Unlock()
	return fired
}

// PendingTimers reports how many active timers remain, mostly for tests.
func (c *VirtualClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers.Len()
}
