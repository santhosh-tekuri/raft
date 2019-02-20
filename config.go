package raft

import (
	"bytes"
	"sort"
	"time"
)

type configuration interface {
	members() map[string]*member
	isQuorum(addrs map[string]struct{}) bool
	majorityMatchIndex() uint64
	isQuorumReachable(t time.Time) bool
}

// ----------------------------------------------------------

type config struct {
	voters map[string]*member
}

func (c *config) members() map[string]*member {
	return c.voters
}

func (c *config) isQuorum(addrs map[string]struct{}) bool {
	n := 0
	for addr := range addrs {
		if _, ok := c.voters[addr]; ok {
			n++
		}
	}

	return n >= c.quorumSize()
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

// is quorum of nodes reachable after time t
func (c *config) isQuorumReachable(t time.Time) bool {
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

type jointConfig struct {
	old, new *config
	voters   map[string]*member
}

func newJointConfig(old, new *config) *jointConfig {
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

// -------------------------------------------------------

func encodeConfig(c configuration) ([]byte, error) {
	buf := new(bytes.Buffer)
	writeConfig := func(c *config) error {
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
			old: c.(*config),
			new: &config{},
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

func decodeConfig(b []byte, dialFn dialFn, timeout time.Duration) (configuration, error) {
	r := bytes.NewBuffer(b)
	readConfig := func() (*config, error) {
		size, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		c := &config{voters: make(map[string]*member)}
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
