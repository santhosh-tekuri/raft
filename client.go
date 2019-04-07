// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"
)

type Client struct {
	addr string
	dial dialFn
}

func NewClient(addr string) Client {
	return Client{addr, net.DialTimeout}
}

func (c Client) getConn() (*conn, error) {
	netConn, err := c.dial("tcp", c.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &conn{
		rwc:  netConn,
		bufr: bufio.NewReader(netConn),
		bufw: bufio.NewWriter(netConn),
	}, nil
}

func (c Client) Info() (Info, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskInfo)); err != nil {
		return nil, err
	}
	if err = conn.bufw.Flush(); err != nil {
		return nil, err
	}
	result, err := decodeTaskResp(taskInfo, conn.bufr)
	if err != nil {
		return nil, err
	}
	return result.(Info), nil
}

func (c Client) ChangeConfig(config Config) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskChangeConfig)); err != nil {
		return err
	}
	_ = config.encode().encode(conn.bufw)
	if err = conn.bufw.Flush(); err != nil {
		return err
	}
	_, err = decodeTaskResp(taskChangeConfig, conn.bufr)
	return err
}

type taskType byte

const (
	taskInfo taskType = math.MaxInt8 - iota
	taskChangeConfig
)

func (t taskType) isValid() bool {
	switch t {
	case taskInfo, taskChangeConfig:
		return true
	}
	return false
}

func decodeTaskResp(typ taskType, r io.Reader) (interface{}, error) {
	s, err := readString(r)
	if err != nil {
		return nil, err
	}
	if s != "" {
		return nil, errors.New(s)
	}
	switch typ {
	case taskInfo:
		json := json{}
		if json.CID, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.NID, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.Addr, err = readString(r); err != nil {
			return nil, err
		}
		if json.Term, err = readUint64(r); err != nil {
			return nil, err
		}
		b, err := readUint8(r)
		if err != nil {
			return nil, err
		}
		json.State = State(b)
		if json.Leader, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.FirstLogIndex, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.LastLogIndex, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.LastLogTerm, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.Committed, err = readUint64(r); err != nil {
			return nil, err
		}
		if json.LastApplied, err = readUint64(r); err != nil {
			return nil, err
		}
		e := &entry{}
		if err = e.decode(r); err != nil {
			return nil, err
		}
		if err = json.Configs.Committed.decode(e); err != nil {
			return nil, err
		}
		if err = e.decode(r); err != nil {
			return nil, err
		}
		if err = json.Configs.Latest.decode(e); err != nil {
			return nil, err
		}
		json.Followers = map[uint64]FlrStatus{}
		sz, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		for sz > 0 {
			sz--
			status := FlrStatus{}
			if status.ID, err = readUint64(r); err != nil {
				return nil, err
			}
			if status.MatchIndex, err = readUint64(r); err != nil {
				return nil, err
			}
			unixNano, err := readUint64(r)
			if err != nil {
				return nil, err
			}
			status.Unreachable = time.Unix(0, int64(unixNano))
			if status.ErrMessage, err = readString(r); err != nil {
				return nil, err
			}
			if status.ErrMessage != "" {
				status.Err = errors.New(status.ErrMessage)
			}
			if status.Round, err = readUint64(r); err != nil {
				return nil, err
			}
			json.Followers[status.ID] = status
		}
		return cachedInfo{json}, nil
	case taskChangeConfig:
		return nil, nil
	}
	return nil, errors.New("invalidTaskType")
}

func encodeTaskResp(t Task, w *bufio.Writer) (err error) {
	defer func() {
		if err == nil {
			err = w.Flush()
		}
	}()
	if t.Err() != nil {
		_ = writeString(w, t.Err().Error())
		return
	}
	_ = writeString(w, "")
	switch r := t.Result().(type) {
	case nil:
		return
	case Info:
		_ = writeUint64(w, r.CID())
		_ = writeUint64(w, r.NID())
		_ = writeString(w, r.Addr())
		_ = writeUint64(w, r.Term())
		_ = writeUint8(w, uint8(r.State()))
		_ = writeUint64(w, r.Leader())
		_ = writeUint64(w, r.FirstLogIndex())
		_ = writeUint64(w, r.LastLogIndex())
		_ = writeUint64(w, r.LastLogTerm())
		_ = writeUint64(w, r.Committed())
		_ = writeUint64(w, r.LastApplied())
		configs := r.Configs()
		if err = configs.Committed.encode().encode(w); err != nil {
			return err
		}
		if err = configs.Latest.encode().encode(w); err != nil {
			return err
		}
		flrs := r.Followers()
		_ = writeUint32(w, uint32(len(flrs)))
		for _, flr := range flrs {
			_ = writeUint64(w, flr.ID)
			_ = writeUint64(w, flr.MatchIndex)
			_ = writeUint64(w, uint64(flr.Unreachable.UnixNano()))
			_ = writeString(w, flr.ErrMessage)
			_ = writeUint64(w, flr.Round)
		}
		return
	}
	return fmt.Errorf("unknown type: %T", t.Result())
}

func (s State) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(s.String())), nil
}
