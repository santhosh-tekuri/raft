// +build debug

package raft

import (
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
)

var colorT = color.New(color.BgBlue, color.FgHiYellow)
var colorL = color.New(color.FgWhite)
var colorC = color.New(color.FgRed)
var colorF = color.New(color.FgCyan)
var colorR = color.New(color.FgYellow)
var colorU = color.New(color.FgHiWhite)

var barrierCh = make(chan struct{}, 1)
var messages = func() chan string {
	ch := make(chan string, 10000)
	go func() {
		for msg := range ch {
			if isBarrier(msg) {
				// signal that barrier reached
				barrierCh <- struct{}{}
				continue
			}
			switch {
			case strings.Index(msg, "[testing]") != -1:
				colorT.Print(strings.TrimSpace(msg))
				fmt.Println()
			case strings.Index(msg, " L | M") != -1:
				colorR.Print(msg)
			case strings.Index(msg, " L | ") != -1:
				colorL.Print(msg)
			case strings.Index(msg, " C | ") != -1:
				colorC.Print(msg)
			case strings.Index(msg, " F | ") != -1:
				colorF.Print(msg)
			default:
				colorU.Print(msg)
			}
		}
	}()
	return ch
}()

var boot = time.Now()

func isBarrier(msg string) bool {
	return strings.HasSuffix(msg, "barrier\n")
}

func debug(args ...interface{}) {
	// uncomment this to print only debug lines with first argument "xxx"
	// this is useful for to print only specific debug lines

	//if args[0] != "xxx" {
	//	return
	//}

	ms := time.Now().Sub(boot).Nanoseconds() / 1e6
	msg := fmt.Sprintln(append([]interface{}{ms}, args...)...)
	messages <- msg
	if isBarrier(msg) {
		// wait all pending messages are printed
		<-barrierCh
	}
}

func assert(b bool, format string, args ...interface{}) {
	if !b {
		// wait until all pending debug messages are printed to stdout
		debug("barrier")
		panic(fmt.Errorf(format, args...))
	}
}

func fatal(format string, args ...interface{}) {
	assert(false, format, args...)
}

// ----------------------------------------------------------

func (r *Raft) String() string {
	return fmt.Sprintf("M%d %d %s |", r.nid, r.term, string(r.state))
}

func (f *flr) String() string {
	return fmt.Sprintf("%s %d %d %d |", f.str, f.matchIndex, f.nextIndex, f.ldrLastIndex)
}

func (fsm *stateMachine) String() string {
	return fmt.Sprintf("M%d", fsm.id)
}

func (u leaderUpdate) String() string {
	return fmt.Sprintf("leaderUpdate{last:%d, commit:%d, config: %v}", u.lastIndex, u.commitIndex, u.config)
}

func (i *liveInfo) String() string {
	return fmt.Sprintf("M%d %d %s |", i.NID(), i.Term(), string(i.State()))
}

func (i *cachedInfo) String() string {
	return fmt.Sprintf("M%d %d %s |", i.NID(), i.Term(), string(i.State()))
}

func (ne newEntry) String() string {
	switch ne.typ {
	case entryUpdate:
		return fmt.Sprintf("update{%s}", string(ne.data))
	case entryRead:
		return fmt.Sprintf("read{%s}", string(ne.data))
	case entryBarrier:
		return "barrier"
	default:
		return fmt.Sprintf("%#v", ne)
	}
}

func (t bootstrap) String() string {
	return fmt.Sprintf("bootstrap{%s}", t.config)
}

func (t changeConfig) String() string {
	return fmt.Sprintf("changeConfig{%s}", t.newConf)
}

func (t takeSnapshot) String() string {
	return fmt.Sprintf("takeSnapshot{%d}", t.threshold)
}

func (t transferLdr) String() string {
	return fmt.Sprintf("transferLdr{M%d %s}", t.target, t.timeout)
}
