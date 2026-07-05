package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"process/pipeline"
)

const (
	recordSize    = 4 * 1024
	queueDepth    = 4096
	runFor        = 10 * time.Second
	producerCount = 4

	maxJobs = 5_000_000
)

var castTable = crc32.MakeTable(crc32.Castagnoli)

var bufPool = sync.Pool{
	New: func() any {
		return new([recordSize]byte)
	},
}

func main() {
	workers := runtime.NumCPU()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := pipeline.NewPool(workers, queueDepth, checksum)
	pool.Start(ctx)

	fmt.Printf("%d workers, %d producers, %d queue, %d limit\n\n",
		workers, producerCount, queueDepth, maxJobs)

	runCtx, cancelRun := context.WithTimeout(ctx, runFor)
	defer cancelRun()

	var produced atomic.Uint64
	var producers sync.WaitGroup
	start := time.Now()

	producers.Add(producerCount)
	for i := 0; i < producerCount; i++ {
		go func(seed uint64) {
			defer producers.Done()
			produce(runCtx, pool, &produced, seed)
		}(uint64(i + 1))
	}

	var reportWg sync.WaitGroup
	reportWg.Add(1)
	go func() {
		defer reportWg.Done()
		report(runCtx, pool, &produced)
	}()

	producers.Wait()
	pool.Close()
	pool.Wait()

	cancelRun()
	reportWg.Wait()

	final := pool.Stats().Snapshot()
	elapsed := time.Since(start)
	mbps := float64(final.BytesOK) / elapsed.Seconds() / (1024 * 1024)

	fmt.Println("\n--- FINAL REPORT ---")
	fmt.Printf("Elapsed Time:  %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Produced:      %d records\n", produced.Load())
	fmt.Printf("Processed OK:  %d records (%d bytes)\n", final.ItemsOK, final.BytesOK)
	fmt.Printf("Failed:        %d records\n", final.ItemsFailed)
	fmt.Printf("Throughput:    %.1f MB/s\n", mbps)
	fmt.Printf("Avg Latency:   %s\n", final.AvgLatency)
	fmt.Printf("Max Latency:   %s\n", final.MaxLatency)
}

func produce(ctx context.Context, pool *pipeline.Pool, produced *atomic.Uint64, seed uint64) {
	state := seed*0x9E3779B97F4A7C15 + 1
	var id uint64

	const batchSize = 256
	var localQuota int

	for {
		if localQuota == 0 {
			if produced.Add(batchSize) > maxJobs {
				return
			}
			localQuota = batchSize
		}
		localQuota--

		arrPtr := bufPool.Get().(*[recordSize]byte)
		buf := arrPtr[:]

		fillFast(buf, &state)

		id++
		job := pipeline.Job{
			ID:         id,
			Payload:    buf,
			EnqueuedAt: time.Now(),
		}

		if err := pool.Submit(ctx, job); err != nil {
			return
		}
	}
}

func fillFast(buf []byte, state *uint64) {
	for i := 0; i+8 <= len(buf); i += 8 {
		s := *state
		s ^= s << 13
		s ^= s >> 7
		s ^= s << 17
		*state = s
		binary.LittleEndian.PutUint64(buf[i:], s)
	}
}

func checksum(j pipeline.Job) error {
	_ = crc32.Checksum(j.Payload, castTable)

	arrPtr := (*[recordSize]byte)(j.Payload)
	bufPool.Put(arrPtr)

	return nil
}

func report(ctx context.Context, pool *pipeline.Pool, produced *atomic.Uint64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s := pool.Stats().Snapshot()
			elapsed := time.Since(start).Seconds()
			mbps := float64(s.BytesOK) / elapsed / (1024 * 1024)
			fmt.Printf("[%4.1fs] limit=%d processed=%-8d failed=%-4d avg=%-8s throughput=%.1f MB/s\n",
				elapsed, maxJobs, s.ItemsOK, s.ItemsFailed, s.AvgLatency, mbps)
		}
	}
}
