// +build !debug

package raft

import "fmt"

func debug(args ...interface{}) {}
func assert(b bool, format string, args ...interface{}) {
	if !b {
		panic(fmt.Errorf(format, args...))
	}
}

func fatal(format string, args ...interface{}) {
	assert(false, format, args...)
}
