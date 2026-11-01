package workerpool

import (
	"context"
	"sync"
)

// Job represents a unit of work
type Job interface {
	Execute(ctx context.Context) error
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	mu          sync.RWMutex
	closed      bool
	workerCount int
	jobQueue    chan Job
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = 5 // Default
	}
	if queueSize <= 0 {
		queueSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan Job, queueSize),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the worker pool
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker is the worker goroutine
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobQueue {
		if job != nil {
			_ = job.Execute(wp.ctx)
		}
	}
}

// Submit submits a job to the pool
func (wp *WorkerPool) Submit(job Job) error {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	if wp.closed {
		return context.Canceled
	}
	select {
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	case wp.jobQueue <- job:
		return nil
	}
}

// Stop stops the worker pool gracefully
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return
	}
	wp.closed = true
	close(wp.jobQueue)
	wp.mu.Unlock()
	wp.wg.Wait()
	wp.cancel()
}

// Stats returns pool statistics
type Stats struct {
	WorkerCount int
	QueueLength int
}

func (wp *WorkerPool) Stats() Stats {
	return Stats{
		WorkerCount: wp.workerCount,
		QueueLength: len(wp.jobQueue),
	}
}
