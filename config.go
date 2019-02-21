package raft

import (
	"bytes"
	"sort"
	"time"
)

type config interface {
	members() map[string]*member
	isQuorum(addrs map[string]struct{}) bool
	majorityMatchIndex() uint64
	isQuorumReachable(t time.Time) bool
	canStartElection(addr string) bool
}

// ----------------------------------------------------------

type stableConfig struct {
	voters map[string]*member
}

func (c *stableConfig) members() map[string]*member {
	return c.voters
}

func (c *stableConfig) isQuorum(addrs map[string]struct{}) bool {
	n := 0
	for addr := range addrs {
		if _, ok := c.voters[addr]; ok {
			n++
		}
	}

	return n >= c.quorumSize()
}

// computes N such that, a majority of matchIndex[i] ≥ N
func (c *stableConfig) majorityMatchIndex() uint64 {
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

// is quorum of nodes reachable after time t
func (c *stableConfig) isQuorumReachable(t time.Time) bool {
	reachable := 0
	for _, v := range c.voters {
		if v.contactedAfter(t) {
			reachable++
		}
	}
	return reachable >= c.quorumSize()
}

func (c *stableConfig) quorumSize() int {
	return len(c.voters)/2 + 1
}

func (c *stableConfig) canStartElection(addr string) bool {
	_, ok := c.voters[addr]
	return ok
}

// -------------------------------------------------------

type jointConfig struct {
	old, new *stableConfig
	voters   map[string]*member
}

func newJointConfig(old, new *stableConfig) *jointConfig {
	voters := make(map[string]*member)
	for addr, m := range old.members() {
		voters[addr] = m
	}
	for addr, m := range new.members() {
		voters[addr] = m
	}
	return &jointConfig{old: old, new: new, voters: voters}
}

func (c *jointConfig) members() map[string]*member {
	return c.voters
}

func (c *jointConfig) isQuorum(addrs map[string]struct{}) bool {
	return c.old.isQuorum(addrs) && c.new.isQuorum(addrs)
}

func (c *jointConfig) majorityMatchIndex() uint64 {
	return min(c.old.majorityMatchIndex(), c.new.majorityMatchIndex())
}

func (c *jointConfig) isQuorumReachable(t time.Time) bool {
	return c.old.isQuorumReachable(t) && c.new.isQuorumReachable(t)
}

func (c *jointConfig) canStartElection(addr string) bool {
	// the leader transition occurs when Cnew is committed because this is
	// the first point when the new configuration can operate independently.
	//
	// before this point, it may be the case that only a server from Cold
	// can be elected leader.
	return c.old.canStartElection(addr)
}

// -------------------------------------------------------

func encodeConfig(c config) ([]byte, error) {
	buf := new(bytes.Buffer)
	writeConfig := func(c *stableConfig) error {
		if err := writeUint32(buf, uint32(len(c.voters))); err != nil {
			return err
		}
		for addr := range c.voters {
			if err := writeString(buf, addr); err != nil {
				return err
			}
		}
		return nil
	}

	jc, ok := c.(*jointConfig)
	if !ok {
		jc = &jointConfig{
			old: c.(*stableConfig),
			new: &stableConfig{},
		}
	}
	if err := writeConfig(jc.old); err != nil {
		return nil, err
	}
	if err := writeConfig(jc.new); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeConfig(b []byte, dialFn dialFn, timeout time.Duration) (config, error) {
	r := bytes.NewBuffer(b)
	readConfig := func() (*stableConfig, error) {
		size, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		c := &stableConfig{voters: make(map[string]*member)}
		for size > 0 {
			addr, err := readString(r)
			if err != nil {
				return nil, err
			}
			c.voters[addr] = &member{
				addr:    addr,
				dialFn:  dialFn,
				timeout: timeout,
			}
			size--
		}
		return c, nil
	}

	old, err := readConfig()
	if err != nil {
		return nil, err
	}
	new, err := readConfig()
	if err != nil {
		return nil, err
	}
	if new == nil {
		return old, nil
	}
	return newJointConfig(old, new), nil
}

// -------------------------------------------------------

type decrUint64Slice []uint64

func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
