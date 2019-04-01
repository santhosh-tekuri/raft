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

// Stringers ----------------------------------------------------------

func (resp resp) String() string {
	if resp.result == unexpectedErr {
		return fmt.Sprintf("T%d %s %v", resp.term, resp.result, resp.err)
	}
	return fmt.Sprintf("T%d %s", resp.term, resp.result)
}

func (req *identityReq) String() string {
	format := "identityReq{T%d M%d C%d M%d}"
	return fmt.Sprintf(format, req.term, req.src, req.cid, req.nid)
}

func (resp *identityResp) String() string {
	return fmt.Sprintf("identityResp{%v}", resp.resp)
}

func (req *voteReq) String() string {
	format := "voteReq{T%d M%d last:(%d,%d)}"
	return fmt.Sprintf(format, req.term, req.src, req.lastLogIndex, req.lastLogTerm)
}

func (resp *voteResp) String() string {
	return fmt.Sprintf("voteResp{%s}", resp.resp)
}

func (req *appendEntriesReq) String() string {
	format := "appendEntriesReq{T%d M%d prev:(%d,%d), #entries: %d, commit:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.prevLogIndex, req.prevLogTerm, req.numEntries, req.ldrCommitIndex)
}

func (resp *appendEntriesResp) String() string {
	format := "appendEntriesResp{%v last:%d}"
	return fmt.Sprintf(format, resp.resp, resp.lastLogIndex)
}

func (req *installSnapReq) String() string {
	format := "installSnapReq{T%d M%d last:(%d,%d), size:%d}"
	return fmt.Sprintf(format, req.term, req.src, req.lastIndex, req.lastIndex, req.size)
}

func (resp *installSnapResp) String() string {
	return fmt.Sprintf("installSnapResp{%v}", resp.resp)
}

func (req *timeoutNowReq) String() string {
	return fmt.Sprintf("timeoutNowReq{T%d M%d}", req.term, req.src)
}

func (resp *timeoutNowResp) String() string {
	return fmt.Sprintf("timeoutNowResp{%v}", resp.resp)
}

func (n Node) String() string {
	return fmt.Sprintf("M%d", n.ID)
}

//go:norace
func (r *Raft) String() string {
	return fmt.Sprintf("M%d %d %s |", r.nid, r.term, string(r.state))
}

//go:norace
func (f *flr) String() string {
	return fmt.Sprintf("%s %d %d %d |", f.str, f.matchIndex, f.nextIndex, f.ldrLastIndex)
}

func (fsm *stateMachine) String() string {
	return fmt.Sprintf("M%d", fsm.id)
}

func (u leaderUpdate) String() string {
	return fmt.Sprintf("leaderUpdate{last:%d, commit:%d, config: %v}", u.log.LastIndex(), u.commitIndex, u.config)
}

func (info *liveInfo) String() string {
	return fmt.Sprintf("M%d %d %s |", info.NID(), info.Term(), string(info.State()))
}

func (info *cachedInfo) String() string {
	return fmt.Sprintf("M%d %d %s |", info.NID(), info.Term(), string(info.State()))
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

func (t changeConfig) String() string {
	return fmt.Sprintf("changeConfig{%s}", t.newConf)
}

func (t takeSnapshot) String() string {
	return fmt.Sprintf("takeSnapshot{%d}", t.threshold)
}

func (t transferLdr) String() string {
	return fmt.Sprintf("transferLdr{M%d %s}", t.target, t.timeout)
}

func (r rpcResponse) String() string {
	return fmt.Sprintf("M%d << %s err: %v", r.from, r.response, r.err)
}

func (u matchIndex) String() string {
	return fmt.Sprintf("replUpdate{M%d matchIndex:%d}", u.status.id, u.val)
}

func (u noContact) String() string {
	if u.time.IsZero() {
		return fmt.Sprintf("replUpdate{M%d yesContact}", u.status.id)
	}
	return fmt.Sprintf("replUpdate{M%d noContact err:%v}", u.status.id, u.err)
}

func (u removeLTE) String() string {
	return fmt.Sprintf("replUpdate{M%d removeLTE:%d}", u.status.id, u.val)
}

func (t fsmApply) String() string {
	var newEntries string
	if t.newEntries != nil && t.newEntries.Len() > 0 {
		front := t.newEntries.Front().Value.(newEntry).index
		back := t.newEntries.Back().Value.(newEntry).index
		newEntries = fmt.Sprintf(", newEntries[%d..%d]", front, back)
	}
	return fmt.Sprintf("fsmApply{commitIndex:%d%s}", t.log.LastIndex(), newEntries)
}

func (t fsmSnapReq) String() string {
	return fmt.Sprintf("fsmSnapReq{index:%d}", t.index)
}

func (t fsmRestoreReq) String() string {
	return "fsmRestoreReq{}"
}

func (t lastApplied) String() string {
	return "lastApplied{}"
}
