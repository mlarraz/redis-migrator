package concurrency

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
)

type WorkerPool struct {
	jobs    []Job
	workers uint
}

type Job func(ctx context.Context) error

func NewWorkerPool(workers uint) *WorkerPool {
	return &WorkerPool{
		jobs:    make([]Job, 0),
		workers: workers,
	}
}
func (r *WorkerPool) AddJob(jobs ...Job) *WorkerPool {
	r.jobs = append(r.jobs, jobs...)
	return r
}

// Run runs the jobs then returns any errors with the same order as the jobs.
// The jobs will be cleared after execution.
func (r *WorkerPool) Run(ctx context.Context) ([]error, error) {
	if len(r.jobs) == 0 {
		return nil, fmt.Errorf("no jobs to run")
	}

	errors := make([]error, len(r.jobs))

	sem := semaphore.NewWeighted(int64(r.workers))

	for jobIndex, job := range r.jobs {
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		go func(resultIndex int, job Job) {
			defer sem.Release(1)
			err := job(ctx)
			errors[resultIndex] = err
		}(jobIndex, job)
	}

	// Wait until all acquired semaphores are released (all jobs are done)
	if err := sem.Acquire(ctx, int64(r.workers)); err != nil {
		return nil, err
	}

	r.jobs = make([]Job, 0)

	return errors, nil
}
