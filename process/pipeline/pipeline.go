package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Job struct {
	ID         uint64
	Payload    []byte
	EnqueuedAt time.Time
}

type ProcessFunc func(Job) error

type Stats struct {
	itemsOK      atomic.Uint64
	_            [7]uint64
	itemsFailed  atomic.Uint64
	_            [7]uint64
	bytesOK      atomic.Uint64
	_            [7]uint64
	latencySumNs atomic.Uint64
	_            [7]uint64
	latencyMaxNs atomic.Uint64
}

func (s *Stats) raiseMax(v uint64) {
	for {
		cur := s.latencyMaxNs.Load()
		if v <= cur {
			return
		}
		if s.latencyMaxNs.CompareAndSwap(cur, v) {
			return
		}
	}
}

type Snapshot struct {
	ItemsOK     uint64
	ItemsFailed uint64
	BytesOK     uint64
	AvgLatency  time.Duration
	MaxLatency  time.Duration
}

func (s *Stats) Snapshot() Snapshot {
	items := s.itemsOK.Load()
	var avg time.Duration
	if items > 0 {
		avg = time.Duration(s.latencySumNs.Load() / items)
	}
	return Snapshot{
		ItemsOK:     items,
		ItemsFailed: s.itemsFailed.Load(),
		BytesOK:     s.bytesOK.Load(),
		AvgLatency:  avg,
		MaxLatency:  time.Duration(s.latencyMaxNs.Load()),
	}
}

type Pool struct {
	in      chan Job
	workers int
	process ProcessFunc
	stats   Stats
	wg      sync.WaitGroup
}

func NewPool(workers, queueSize int, fn ProcessFunc) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	return &Pool{
		in:      make(chan Job, queueSize),
		workers: workers,
		process: fn,
	}
}

func (p *Pool) Stats() *Stats { return &p.stats }

func (p *Pool) Start(ctx context.Context) {
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.worker(ctx)
	}
}

func (p *Pool) Submit(ctx context.Context, j Job) error {
	select {
	case p.in <- j:
		return nil
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.in <- j:
		return nil
	}
}

func (p *Pool) Close() { close(p.in) }

func (p *Pool) Wait() { p.wg.Wait() }

const flushBatch = 1024

type local struct {
	items, bytes, failed, latencySum, latencyMax uint64
	n                                            int
}

func (l *local) add(n int, latency time.Duration) {
	l.items++
	l.bytes += uint64(n)
	d := uint64(latency)
	l.latencySum += d
	if d > l.latencyMax {
		l.latencyMax = d
	}
	l.n++
}

func (l *local) fail() {
	l.failed++
	l.n++
}

func (l *local) flush(dst *Stats) {
	if l.items > 0 {
		dst.itemsOK.Add(l.items)
		dst.bytesOK.Add(l.bytes)
		dst.latencySumNs.Add(l.latencySum)
		dst.raiseMax(l.latencyMax)
	}
	if l.failed > 0 {
		dst.itemsFailed.Add(l.failed)
	}
	*l = local{}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	var loc local

	for p.runWorkerLoop(ctx, &loc) {
	}
	loc.flush(&p.stats)
}

func (p *Pool) runWorkerLoop(ctx context.Context, loc *local) (keepGoing bool) {
	defer func() {
		if r := recover(); r != nil {
			loc.fail()
			keepGoing = true
		}
	}()

	for job := range p.in {
		start := time.Now()

		if err := p.process(job); err != nil {
			loc.fail()
		} else {
			loc.add(len(job.Payload), time.Since(start))
		}

		if loc.n >= flushBatch {
			loc.flush(&p.stats)
			if ctx.Err() != nil {
				p.drain(loc)
				return false
			}
		}
	}
	return false
}

func (p *Pool) drain(loc *local) {
	for {
		select {
		case job, ok := <-p.in:
			if !ok {
				return
			}
			start := time.Now()
			if err := p.process(job); err != nil {
				loc.fail()
			} else {
				loc.add(len(job.Payload), time.Since(start))
			}
		default:
			return
		}
	}
}
