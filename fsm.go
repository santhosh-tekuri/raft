package raft

type FSM interface {
	Apply(cmd []byte) interface{}
}
