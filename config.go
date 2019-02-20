package raft

import (
	"sort"
	"time"
)

type configuration interface {
	members() map[string]*member
	isQuorum(addrs map[string]struct{}) bool
	majorityMatchIndex() uint64
	isQuorumReachable(d time.Duration) bool
}

// ----------------------------------------------------------

type config struct {
	voters map[string]*member
}

func (c *config) members() map[string]*member {
	return c.voters
}

func (c *config) isQuorum(addrs map[string]struct{}) bool {
	// delete addrs which are not in config
	for addr := range addrs {
		if _, ok := c.voters[addr]; !ok {
			delete(addrs, addr)
		}
	}

	return len(addrs) >= c.quorumSize()
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (c *config) majorityMatchIndex() uint64 {
	if len(c.voters) == 1 {
		for _, v := range c.voters {
			return v.matchIndex
		}
	}

	matched := make(decrUint64Slice, len(c.voters))
	i := 0
	for _, v := range c.voters {
		matched[i] = v.matchIndex
		i++
	}
	// sort in decrease order
	sort.Sort(matched)
	return matched[c.quorumSize()-1]
}

func (c *config) isQuorumReachable(d time.Duration) bool {
	t := time.Now().Add(-d)
	reachable := 0
	for _, v := range c.voters {
		if v.contactedAfter(t) {
			reachable++
		}
	}
	return reachable >= c.quorumSize()
}

func (c *config) quorumSize() int {
	return len(c.voters)/2 + 1
}

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
