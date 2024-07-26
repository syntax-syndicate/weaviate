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
	"math/rand/v2"
	"sync"
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
	readOp[T any] func(_ context.Context, host string, fullRead bool) (T, error)

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
	replyCh := make(chan _Result[T], level)

	candidatePool := make(chan string, len(state.Hosts)) // remaining ones
	for _, replica := range state.Hosts {
		candidatePool <- replica
	}

	// TODO can i ensure fullRead tries the direct candidate node first? this seems important for perf reasons
	// failBackOff := time.Second * 20
	// initialBackOff := time.Millisecond * 250
	failBackOff := time.Millisecond * 2
	initialBackOff := time.Millisecond * 1
	f := func() {
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ { // Ask direct candidate first
			doFullRead := i == 0
			f := func() {
				retryCounter := make(map[string]time.Duration)
				failedNodes := make(map[string]_Result[T])
				defer wg.Done()
				for delegate := range candidatePool {
					if f, ok := failedNodes[delegate]; ok {
						replyCh <- f
						break
					}
					if r, ok := retryCounter[delegate]; ok {
						timer := time.NewTimer(r)
						select {
						case <-ctx.Done():
							timer.Stop()
							break
						case <-timer.C:
						}
						timer.Stop()
					}
					resp, err := op(ctx, delegate, doFullRead)
					// fmt.Println("NATEE op", i, delegate, doFullRead, err)
					if err == nil {
						replyCh <- _Result[T]{resp, err}
						break
					}

					candidatePool <- delegate
					var newDelay time.Duration
					if d, ok := retryCounter[delegate]; ok {
						newDelay = backOff(d)
					} else {
						newDelay = initialBackOff
					}
					if newDelay > failBackOff {
						failedNodes[delegate] = _Result[T]{resp, err}
					}
					retryCounter[delegate] = newDelay
					continue
				}
			}
			enterrors.GoWrapper(f, c.log)
		}

		wg.Wait()
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}

// TODO dry
// backOff return a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func backOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}
