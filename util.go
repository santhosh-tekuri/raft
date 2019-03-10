package raft

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"time"
)

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// ------------------------------------------------------

type safeTimer struct {
	*time.Timer

	// tells we need to receive from channel
	// NOTE: after receiving from channel, this
	// must be set to false
	receive bool
}

func newSafeTimer() *safeTimer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return &safeTimer{t, false}
}

func (t *safeTimer) stop() {
	if !t.Timer.Stop() {
		if t.receive {
			<-t.C
		}
	}
	t.receive = false
}

func (t *safeTimer) reset(d time.Duration) {
	t.stop()
	t.Timer.Reset(d)
	t.receive = true
}

// backOff ------------------------------------------------

const (
	maxAppendEntries = 64 // todo: should be configurable
	maxFailureScale  = 12
	failureWait      = 10 * time.Millisecond
)

// backOff is used to compute an exponential backOff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backOff(round uint64) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

// randTime -----------------------------------------------------------------

type randTime struct {
	r *rand.Rand
}

func newRandTime() randTime {
	var seed int64
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		fmt.Printf("failed to read random bytes: %v\n", err)
		seed = time.Now().UnixNano()
	} else {
		seed = r.Int64()
	}
	return randTime{rand.New(rand.NewSource(seed))}
}

func (rt randTime) duration(min time.Duration) time.Duration {
	return min + time.Duration(rt.r.Int63())%min
}

func (rt randTime) after(min time.Duration) <-chan time.Time {
	return time.After(rt.duration(min))
}

// -------------------------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
