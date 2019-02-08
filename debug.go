// +build debug

package raft

import "fmt"

func debug(args ...interface{}) {
	fmt.Println(args...)
}
