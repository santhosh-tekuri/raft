package raft

type Stable interface {
	Get() (term uint64, votedFor string, err error)
	Set(term uint64, votedFor string) error
}
