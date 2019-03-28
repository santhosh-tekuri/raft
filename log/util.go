package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func fileExists(name string) (bool, error) {
	info, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("log: directory found at %s", name)
	}
	return true, nil
}

func createSegment(name string, opt Options) (err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, opt.FileMode)
	if err != nil {
		return
	}
	defer func() {
		if e := f.Close(); err == nil {
			err = e
		}
		if err != nil {
			if e := os.Remove(name); err == nil {
				err = e
			}
		}
	}()
	size := int64(opt.SegmentSize)
	if err = f.Truncate(size); err != nil {
		return
	}
	if _, err = f.WriteAt(make([]byte, 16), size-16); err != nil {
		return
	}
	err = f.Sync()
	return
}

func segmentFile(dir string, prevIndex uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d.log", prevIndex))
}

func segments(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	var offs []uint64
	for _, m := range matches {
		m = filepath.Base(m)
		m = strings.TrimSuffix(m, ".log")
		i, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			return nil, err
		}
		offs = append(offs, i)
	}
	sort.Slice(offs, func(i, j int) bool {
		return offs[i] < offs[j]
	})
	return offs, nil
}

func openSegments(dir string, opt Options) (first, last *segment, err error) {
	offs, err := segments(dir)
	if err != nil {
		return
	}
	if len(offs) == 0 {
		offs = append(offs, 0)
	}

	if first, err = openSegment(dir, offs[0], opt); err != nil {
		return
	}
	last = first
	offs = offs[1:]

	var s *segment
	for _, off := range offs {
		if last.n > 0 && off == last.lastIndex() {
			if s, err = openSegment(dir, off, opt); err != nil {
				return
			}
			connect(last, s)
			last = s
		} else {
			// dangling segment: remove it
			if err = os.Remove(segmentFile(dir, off)); err == nil {
				return
			}
		}
	}
	return
}

func connect(s1, s2 *segment) {
	s1.next = s2
	s2.prev = s1
}

func disconnect(s1, s2 *segment) {
	s1.next = nil
	s2.prev = nil
}
