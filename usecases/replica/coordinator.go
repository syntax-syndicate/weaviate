//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
)

type (
	// readyOp asks a replica if it is ready to commit
	readyOp func(_ context.Context, host, requestID string) error

	// readyOp asks a replica to execute the actual operation
	commitOp[T any] func(_ context.Context, host, requestID string) (T, error)

	// readOp defines a generic read operation
	readOp[T any] func(_ context.Context, host string, fullRead bool, timeout time.Duration) (T, error)

	// coordinator coordinates replication of write and read requests
	coordinator[T any] struct {
		Client
		Resolver *resolver // node_name -> host_address
		log      logrus.FieldLogger
		Class    string
		Shard    string
		TxID     string // transaction ID
	}
)

// newCoordinator used by the replicator
func newCoordinator[T any](r *Replicator, shard, requestID string, l logrus.FieldLogger,
) *coordinator[T] {
	return &coordinator[T]{
		Client:   r.client,
		Resolver: r.resolver,
		log:      l,
		Class:    r.class,
		Shard:    shard,
		TxID:     requestID,
	}
}

// newCoordinator used by the Finder to read objects from replicas
func newReadCoordinator[T any](f *Finder, shard string) *coordinator[T] {
	return &coordinator[T]{
		Resolver: f.resolver,
		Class:    f.class,
		Shard:    shard,
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context,
	replicas []string,
	op readyOp, level int,
) <-chan string {
	// prepare tells replicas to be ready
	prepare := func() <-chan _Result[string] {
		resChan := make(chan _Result[string], len(replicas))
		f := func() { // broadcast
			defer close(resChan)
			var wg sync.WaitGroup
			wg.Add(len(replicas))
			for _, replica := range replicas {
				replica := replica
				g := func() {
					defer wg.Done()
					err := op(ctx, replica, c.TxID)
					resChan <- _Result[string]{replica, err}
				}
				enterrors.GoWrapper(g, c.log)
			}
			wg.Wait()
		}
		enterrors.GoWrapper(f, c.log)
		return resChan
	}

	// handle responses to prepare requests
	replicaCh := make(chan string, len(replicas))
	f := func() {
		defer close(replicaCh)
		actives := make([]string, 0, level) // cache for active replicas
		for r := range prepare() {
			if r.Err != nil { // connection error
				c.log.WithField("op", "broadcast").Error(r.Err)
				continue
			}

			level--
			if level > 0 { // cache since level has not been reached yet
				actives = append(actives, r.Value)
				continue
			}
			if level == 0 { // consistency level has been reached
				for _, x := range actives {
					replicaCh <- x
				}
			}
			replicaCh <- r.Value
		}
		if level > 0 { // abort: nothing has been sent to the caller
			fs := logrus.Fields{"op": "broadcast", "active": len(actives), "total": len(replicas)}
			c.log.WithFields(fs).Error("abort")
			for _, node := range replicas {
				c.Abort(ctx, node, c.Class, c.Shard, c.TxID)
			}
		}
	}
	enterrors.GoWrapper(f, c.log)
	return replicaCh
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context,
	replicaCh <-chan string,
	op commitOp[T],
) <-chan _Result[T] {
	replyCh := make(chan _Result[T], cap(replicaCh))
	f := func() { // tells active replicas to commit
		wg := sync.WaitGroup{}
		for replica := range replicaCh {
			wg.Add(1)
			replica := replica
			g := func() {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
				replyCh <- _Result[T]{resp, err}
			}
			enterrors.GoWrapper(g, c.log)
		}
		wg.Wait()
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh
}

// Push pushes updates to all replicas of a specific shard
func (c *coordinator[T]) Push(ctx context.Context,
	cl ConsistencyLevel,
	ask readyOp,
	com commitOp[T],
) (<-chan _Result[T], int, error) {
	state, err := c.Resolver.State(c.Shard, cl, "")
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
	ctxWithTimeout, _ := context.WithTimeout(context.Background(), 20*time.Second)
	nodeCh := c.broadcast(ctxWithTimeout, state.Hosts, ask, level)
	return c.commitAll(context.Background(), nodeCh, com), level, nil
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)
func (c *coordinator[T]) Pull(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level

	candidates := make([]string, len(state.Hosts))
	copy(candidates, state.Hosts)
	maxTimeout := 25 * time.Millisecond
	incomingCandidateIndices := make(chan int, len(candidates))
	candidateCurTimeouts := sync.Map{} // TODO just for testing
	for i := range candidates {
		incomingCandidateIndices <- i
		candidateCurTimeouts.Store(i, 10*time.Millisecond)
	}
	workersDone := sync.WaitGroup{}
	workersDone.Add(level)
	successfulReplies := atomic.Int32{}
	timeoutReplies := atomic.Int32{}
	timeoutResults := make(chan _Result[T], len(candidates))
	adjustWorkersMutex := sync.Mutex{}
	replyCh := make(chan _Result[T], len(candidates)) // TODO vs len level
	f := func() {
		for i := 0; i < level; i++ {
			workerFunc := func() {
				defer workersDone.Done()
				for {
					if successfulReplies.Load() >= int32(level) {
						break
					}
					idx := <-incomingCandidateIndices
					curTimeoutAny, ok := candidateCurTimeouts.Load(idx)
					if !ok {
						panic("TODO")
					}
					curTimeout := curTimeoutAny.(time.Duration)
					resp, err := op(ctx, candidates[idx], idx == 0, curTimeout)
					if err == nil {
						successfulReplies.Add(1)
						replyCh <- _Result[T]{resp, err}
						break
					}
					candidateCurTimeouts.Store(idx, curTimeout*2)
					if curTimeout <= maxTimeout {
						incomingCandidateIndices <- idx
					} else {
						timeoutReplies.Add(1)
						timeoutResults <- _Result[T]{resp, err}
					}
					// TODO this mutex is wrong, should simplify
					adjustWorkersMutex.Lock()
					s := successfulReplies.Load()
					numCandidatesRemaining := int32(len(candidates)) - (s + timeoutReplies.Load())
					numWorkers := level - int(s)
					if numWorkers > int(numCandidatesRemaining) {
						adjustWorkersMutex.Unlock()
						break
					}
					adjustWorkersMutex.Unlock()
				}
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		workersDone.Wait()
		if successfulReplies.Load() < int32(level) {
			r := <-timeoutResults
			if r.Err == nil {
				panic("TODO")
			}
			replyCh <- r
		}
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)
func (c *coordinator[T]) PullAll(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level

	candidates := make([]string, len(state.Hosts))
	copy(candidates, state.Hosts)
	maxTimeout := 25 * time.Millisecond
	replyCh := make(chan _Result[T], len(candidates)) // TODO vs len level
	f := func() {
		workersDone := sync.WaitGroup{}
		workersDone.Add(len(candidates))
		successfulReplies := atomic.Int32{}
		timeoutResults := make(chan _Result[T], len(candidates))
		for i := range candidates { // Ask direct candidate first
			idx := i
			workerFunc := func() {
				defer workersDone.Done()
				curTimeout := 10 * time.Millisecond
				for {
					if successfulReplies.Load() >= int32(level) {
						break
					}
					resp, err := op(ctx, candidates[idx], idx == 0, curTimeout)
					if err == nil {
						successfulReplies.Add(1)
						replyCh <- _Result[T]{resp, err}
						break
					}
					if curTimeout > maxTimeout {
						timeoutResults <- _Result[T]{resp, err}
						break
					}
					curTimeout *= 2
				}
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		workersDone.Wait()
		if successfulReplies.Load() < int32(level) {
			r := <-timeoutResults
			if r.Err == nil {
				panic("TODO")
			}
			replyCh <- r
		}
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}
