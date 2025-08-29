package concurrency

import (
	"context"
	"sync"
)

// WorkerPool is a simple worker pool for concurrent processing.
type WorkerPool struct {
	workerCount int
	tasks       chan task
	wg          sync.WaitGroup
	handler     func(context.Context, []byte) error
}

type task struct {
	ctx context.Context
	msg []byte
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool(workerCount int, handler func(context.Context, []byte) error) *WorkerPool {
	pool := &WorkerPool{
		workerCount: workerCount,
		tasks:       make(chan task, workerCount*2),
		handler:     handler,
	}
	pool.start()
	return pool
}

func (p *WorkerPool) start() {
	for i := 0; i < p.workerCount; i++ {
		go func() {
			for t := range p.tasks {
				_ = p.handler(t.ctx, t.msg)
				p.wg.Done()
			}
		}()
	}
}

// Submit enqueues a task for processing.
func (p *WorkerPool) Submit(ctx context.Context, msg []byte) {
	p.wg.Add(1)
	p.tasks <- task{ctx: ctx, msg: msg}
}

// Shutdown waits for tasks to complete and closes the pool.
func (p *WorkerPool) Shutdown() {
	p.wg.Wait()
	close(p.tasks)
}
