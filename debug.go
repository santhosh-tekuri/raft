// +build debug

package raft

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
)

var colorL = color.New(color.FgWhite)
var colorC = color.New(color.FgRed)
var colorF = color.New(color.FgCyan)
var colorR = color.New(color.FgYellow)
var colorU = color.New(color.FgHiWhite)

var messages = func() chan string {
	ch := make(chan string, 10000)
	go func() {
		for msg := range ch {
			switch {
			case strings.Index(msg, " L | ") != -1:
				i := strings.Index(msg, " L | ")
				if strings.HasPrefix(msg[i+6:], ":8888") {
					colorR.Print(msg)
				} else {
					colorL.Print(msg)
				}
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

func debug(args ...interface{}) {
	messages <- fmt.Sprintln(args...)
}

func assert(b bool, format string, args ...interface{}) {
	if !b {
		panic(fmt.Errorf(format, args))
	}
}
