// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/big"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func safeClose(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	e := d.Close()
	if err != nil {
		return err
	}
	return e
}

func size(buffs net.Buffers) int64 {
	var size int64
	for _, buff := range buffs {
		size += int64(len(buff))
	}
	return size
}

// safeTimer ------------------------------------------------------

type safeTimer struct {
	timer *time.Timer
	C     <-chan time.Time

	// active is true if timer is started, but not yet received from channel.
	// NOTE: must be set to false, after receiving from channel
	active bool
}

// newSafeTimer creates stopped timer
func newSafeTimer() *safeTimer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return &safeTimer{t, t.C, false}
}

func (t *safeTimer) stop() {
	if !t.timer.Stop() {
		if t.active {
			<-t.C
		}
	}
	t.active = false
}

func (t *safeTimer) reset(d time.Duration) {
	t.stop()
	t.timer.Reset(d)
	t.active = true
}

// backOff ------------------------------------------------

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// backOff is used to compute an exponential backOff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor. If backOff exceeds
// given max, it returns max
func backOff(round uint64, max time.Duration) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	if base > max {
		return max
	}
	return base
}

// randTime -----------------------------------------------------------------

type randTime struct {
	r *rand.Rand
}

func newRandTime() randTime {
	var seed int64
	if r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64)); err != nil {
		seed = time.Now().UnixNano()
	} else {
		seed = r.Int64()
	}
	return randTime{rand.New(rand.NewSource(seed))}
}

func (rt randTime) duration(min time.Duration) time.Duration {
	return min + time.Duration(rt.r.Int63())%min
}

func (rt randTime) deadline(min time.Duration) time.Time {
	return time.Now().Add(rt.duration(min))
}

func (rt randTime) after(min time.Duration) <-chan time.Time {
	return time.After(rt.duration(min))
}

// -------------------------------------------------------------------------

func lockDir(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	tempFile, err := ioutil.TempFile(dir, "lock*.tmp")
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()
	if _, err := io.WriteString(tempFile, fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	lockFile := filepath.Join(dir, "lock")
	if err := os.Link(tempFile.Name(), lockFile); err != nil {
		if os.IsExist(err) {
			return ErrLockExists
		}
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	tempInfo, err := os.Lstat(tempFile.Name())
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	lockInfo, err := os.Lstat(lockFile)
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	if !os.SameFile(tempInfo, lockInfo) {
		return ErrLockExists
	}
	return nil
}

func unlockDir(dir string) error {
	return os.RemoveAll(filepath.Join(dir, "lock"))
}

// -------------------------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// -------------------------------------------------------------------------

func durationFor(bandwidth int64, n int64) time.Duration {
	seconds := float64(n) / float64(bandwidth)
	return time.Duration(1e9 * seconds)
}

// -------------------------------------------------------------------------

func bug(calldepth int, format string, v ...interface{}) error {
	_, file, line, ok := runtime.Caller(calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	if i := strings.LastIndex(file, "/"); i != -1 {
		file = file[i+1:]
	}
	prefix := fmt.Sprintf("[RAFT-BUG] %s:%d ", file, line)
	return fmt.Errorf(prefix+format, v...)
}

func trimPrefix(err error) string {
	return strings.TrimPrefix(err.Error(), "raft: ")
}
