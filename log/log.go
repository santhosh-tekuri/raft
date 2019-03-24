package log

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/raft/vars"
)

type NotFoundError uint64

func (e NotFoundError) Error() string {
	return fmt.Sprintf("log: entry %d not found", uint64(e))
}

// -------------------------------------------------------------------------------

type Log struct {
	dir      string
	ref      *vars.Var
	seg      *segment
	maxCount uint64
	maxSize  int64
}

func New(dir string, maxSegmentEntries uint64, maxSegmentSize int64) (*Log, error) {
	if maxSegmentSize <= 0 {
		return nil, fmt.Errorf("log: maxSegmentEntries is <=0")
	}
	if maxSegmentSize <= 0 {
		return nil, fmt.Errorf("log: maxSegmentSize is <=0")
	}
	ref, err := vars.NewVar(filepath.Join(dir, "segments.ref"))
	if err != nil {
		return nil, err
	}
	success := false
	var seg *segment
	defer func() {
		if !success {
			_ = ref.Close()
			s := seg
			for s != nil {
				_ = s.close()
				s = s.prev
			}
		}
	}()
	first, last, err := ref.Get()
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*.index"))
	if err != nil {
		return nil, err
	}
	var offs []uint64
	for _, m := range matches {
		m = strings.TrimSuffix(m, ".index")
		i, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			return nil, err
		}
		if i >= first && i <= last {
			offs = append(offs, i)
		}
	}
	sort.Slice(offs, func(i, j int) bool {
		return offs[i] < offs[j]
	})
	if len(offs) == 0 && first == 0 && last == 0 {
		offs = append(offs, 0)
	}
	if offs[0] != first {
		return nil, fmt.Errorf("log: %d.index is missing in %s", first, dir)
	}
	if offs[len(offs)-1] != last {
		return nil, fmt.Errorf("log: %d.index is missing in %s", last, dir)
	}
	for _, off := range offs {
		s, err := newSegment(dir, off, maxSegmentEntries, maxSegmentSize)
		if err != nil {
			return nil, err
		}
		s.prev, seg = seg, s
	}
	success = true
	return &Log{
		dir:      dir,
		ref:      ref,
		seg:      seg,
		maxCount: maxSegmentEntries,
		maxSize:  maxSegmentSize,
	}, nil
}

func (l *Log) LastIndex() (uint64, error) {
	return l.seg.lastIndex(), nil
}

func (l *Log) Count() (uint64, error) {
	if l.seg.prev == nil {
		return l.seg.idx.n, nil
	}
	first, _, err := l.ref.Get()
	if err != nil {
		return 0, err
	}
	return (l.seg.off - first) + uint64(l.seg.idx.n), nil
}

func (l *Log) Get(i uint64) ([]byte, error) {
	s := l.seg
	for s != nil {
		if i >= s.off {
			return s.get(i)
		}
		s = s.prev
	}
	return nil, NotFoundError(i)
}

func (l *Log) Append(d []byte) (err error) {
	if l.seg.isFull(len(d)) {
		s, err := newSegment(l.dir, l.seg.lastIndex()+1, l.maxCount, l.maxSize)
		if err != nil {
			return err
		}
		s.prev, l.seg = l.seg, s
	}
	return l.seg.append(d)
}

func (l *Log) RemoveGTE(i uint64) error {
	first, last, err := l.ref.Get()
	if err != nil {
		return err
	}
	for {
		if l.seg.off > i { // has to remove this segment
			if l.seg.prev == nil {
				prev, err := newSegment(l.dir, i, l.maxCount, l.maxSize)
				if err != nil {
					return err
				}
				if err := l.ref.Set(i, i); err != nil {
					return err
				}
				_ = l.seg.close()
				_ = l.seg.remove()
				l.seg = prev
				return nil
			} else {
				prev := l.seg.prev
				last = prev.off
				if err := l.ref.Set(first, last); err != nil {
					return err
				}
				_ = l.seg.close()
				_ = l.seg.remove()
				l.seg.prev = nil
				l.seg = prev
			}
		} else if l.seg.off <= i {
			return l.seg.removeGTE(i)
		}
	}
}

func (l *Log) RemoveLTE(i uint64) error {
	first, last, err := l.ref.Get()
	if err != nil {
		return err
	}
	s := l.seg
	for s != nil {
		if s.off <= i+1 {
			break
		}
		s = s.prev
	}
	if s == nil {
		return nil
	}

	// remove all segments prior to s
	if s.prev != nil {
		first = s.off
		if err := l.ref.Set(first, last); err != nil {
			return err
		}
		prev := s.prev
		s.prev = nil
		for prev != nil {
			_ = prev.close()
			_ = prev.remove()
			prev = prev.prev
		}
	}

	s = l.seg
	if s.prev == nil && s.idx.n > 0 && s.lastIndex() <= i { // clear this segment
		if s != l.seg {
			panic("s should be last segment")
		}
		next, err := newSegment(l.dir, s.lastIndex()+1, l.maxCount, l.maxSize)
		if err != nil {
			return err
		}
		if err := l.ref.Set(i, i); err != nil {
			return err
		}
		_ = l.seg.close()
		_ = l.seg.remove()
		l.seg = next
	}

	return nil
}

func (l *Log) Close() error {
	err := l.ref.Close()
	s := l.seg
	for s != nil {
		err1 := s.close()
		if err != nil {
			err = err1
		}
		s = s.prev
	}
	return err
}
