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
var colorU = color.New(color.FgBlue)

func debug(args ...interface{}) {
	switch msg := fmt.Sprintln(args...); {
	case strings.Index(msg, " L | ") != -1:
		i := strings.Index(msg, " L | ")
		if strings.HasPrefix(msg[i+5:], "localhost:") {
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
