package gateway

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var ErrFull = errors.New("waiting queue is full")

type Queue struct {
	mu                   sync.Mutex
	normal, urgent       list.List
	active, slots, limit int
	priority             bool
}

func NewQueue(slots, limit int, priority bool) *Queue {
	return &Queue{slots: slots, limit: limit, priority: priority}
}

// Acquire reserves one slot. Every successful caller must call Release once.
func (q *Queue) Acquire(ctx context.Context, urgent bool) error {
	q.mu.Lock()
	if err := ctx.Err(); err != nil {
		q.mu.Unlock()
		return err
	}
	if q.active < q.slots {
		q.active++
		q.mu.Unlock()
		return nil
	}
	if q.normal.Len()+q.urgent.Len() >= q.limit {
		q.mu.Unlock()
		return ErrFull
	}
	lane := &q.normal
	if q.priority && urgent {
		lane = &q.urgent
	}
	ready := make(chan struct{})
	entry := lane.PushBack(ready)
	q.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		q.mu.Lock()
		select {
		case <-ready:
			q.releaseLocked() // The slot was transferred while cancellation was pending.
		default:
			lane.Remove(entry)
		}
		q.mu.Unlock()
		return ctx.Err()
	}
}

func (q *Queue) Release() { q.mu.Lock(); defer q.mu.Unlock(); q.releaseLocked() }

func (q *Queue) releaseLocked() {
	for _, lane := range []*list.List{&q.urgent, &q.normal} {
		if e := lane.Front(); e != nil {
			close(lane.Remove(e).(chan struct{}))
			return
		}
	}
	q.active--
}
