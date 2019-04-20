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

// Client is an RPC client used for performing admin tasks,
// such as membership change, transfer leadership etc.
type Client struct {
	addr string
	dial dialFn
}

// NewClient creates new client for given raft server.
func NewClient(addr string) *Client {
	return &Client{addr, net.DialTimeout}
}

func (c *Client) getConn() (*conn, error) {
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

// GetInfo return Info containing raft current state.
func (c *Client) GetInfo() (Info, error) {
	conn, err := c.getConn()
	if err != nil {
		return Info{}, err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskInfo)); err != nil {
		return Info{}, err
	}
	if err = conn.bufw.Flush(); err != nil {
		return Info{}, err
	}
	result, err := decodeTaskResp(taskInfo, conn.bufr)
	if err != nil {
		return Info{}, err
	}
	return result.(Info), nil
}

// ChangeConfig is used to change raft server config.
// If this method return nil, it means the change is accepted
// by quorum of nodes, but changes may not be still applied.
// The changes are carried as per raft protocol and changes
// are carried out by new leader if leader changes meanwhile.
// So it is guaranteed that submitted config change is completed
// in spite of leader change.
//
// use WaitForStableConfig, in order to wait for the changes
// submitted for completion.
func (c *Client) ChangeConfig(config Config) error {
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

// WaitForStableConfig blocks the caller until all config changes are
// completed. if there are no config changes left, this method returns
// immediately.
func (c *Client) WaitForStableConfig() (Config, error) {
	conn, err := c.getConn()
	if err != nil {
		return Config{}, err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskWaitForStableConfig)); err != nil {
		return Config{}, err
	}
	if err = conn.bufw.Flush(); err != nil {
		return Config{}, err
	}
	result, err := decodeTaskResp(taskWaitForStableConfig, conn.bufr)
	if err != nil {
		return Config{}, err
	}
	return result.(Config), nil
}

func (c *Client) TakeSnapshot(threshold uint64) (snapIndex uint64, err error) {
	conn, err := c.getConn()
	if err != nil {
		return 0, err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskTakeSnapshot)); err != nil {
		return 0, err
	}
	if err = writeUint64(conn.bufw, threshold); err != nil {
		return 0, err
	}
	if err = conn.bufw.Flush(); err != nil {
		return 0, err
	}
	result, err := decodeTaskResp(taskTakeSnapshot, conn.bufr)
	if err != nil {
		return 0, err
	}
	return result.(uint64), nil
}

func (c *Client) TransferLeadership(target uint64, timeout time.Duration) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}
	defer conn.rwc.Close()

	if err = conn.bufw.WriteByte(byte(taskTransferLdr)); err != nil {
		return err
	}
	if err = writeUint64(conn.bufw, target); err != nil {
		return err
	}
	if err = writeUint64(conn.bufw, uint64(timeout)); err != nil {
		return err
	}
	if err = conn.bufw.Flush(); err != nil {
		return err
	}
	_, err = decodeTaskResp(taskTransferLdr, conn.bufr)
	return err
}

// ------------------------------------------------------------------------

type taskType byte

const (
	taskInfo taskType = math.MaxInt8 - iota
	taskChangeConfig
	taskWaitForStableConfig
	taskTakeSnapshot
	taskTransferLdr
)

func (t taskType) isValid() bool {
	switch t {
	case taskInfo, taskChangeConfig, taskWaitForStableConfig, taskTakeSnapshot, taskTransferLdr:
		return true
	}
	return false
}

func decodeTaskResp(typ taskType, r io.Reader) (interface{}, error) {
	errType, err := readString(r)
	if err != nil {
		return nil, err
	}
	if errType != "" {
		switch errType {
		case "raft.NotLeaderError":
			node := Node{}
			if err := node.decode(r); err != nil {
				return nil, err
			}
			lost, err := readBool(r)
			if err != nil {
				return nil, err
			}
			return nil, NotLeaderError{node, lost}
		default:
			s, err := readString(r)
			if err != nil {
				return nil, err
			}
			switch errType {
			case "raft.plainError":
				return nil, plainError(s)
			case "raft.temporaryError":
				return nil, temporaryError(s)
			case "raft.InProgressError":
				return nil, InProgressError(s)
			default:
				return nil, errors.New(s)
			}
		}
	}
	switch typ {
	case taskInfo:
		info := Info{}
		err = info.decode(r)
		return info, err
	case taskWaitForStableConfig:
		e := &entry{}
		if err = e.decode(r); err != nil {
			return nil, err
		}
		var config Config
		if err = config.decode(e); err != nil {
			return nil, err
		}
		return config, nil
	case taskChangeConfig, taskTransferLdr:
		return nil, nil
	case taskTakeSnapshot:
		return readUint64(r)
	}
	return nil, errors.New("invalidTaskType")
}

func encodeTaskResp(t Task, w io.Writer) error {
	if t.Err() != nil {
		if err := writeString(w, fmt.Sprintf("%T", t.Err())); err != nil {
			return err
		}
		switch err := t.Err().(type) {
		case NotLeaderError:
			if err := err.Leader.encode(w); err != nil {
				return err
			}
			return writeBool(w, err.Lost)
		default:
			return writeString(w, err.Error())
		}
	}
	if err := writeString(w, ""); err != nil {
		return err
	}
	switch r := t.Result().(type) {
	case nil:
		return nil
	case uint64:
		return writeUint64(w, r)
	case Config:
		return r.encode().encode(w)
	case Info:
		return r.encode(w)
	}
	return fmt.Errorf("unknown type: %T", t.Result())
}

// MarshalJSON implements the json.Marshaler interface.
func (s State) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(s.String())), nil
}

// MarshalJSON implements the json.Marshaler interface.
func (a Action) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(a.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (a *Action) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*a = None
		return nil
	}
	if len(data) < 2 || data[0] != '"' {
		return errors.New("configAction must be json string")
	}
	s, err := strconv.Unquote(string(data))
	if err != nil {
		return err
	}
	for _, ca := range []Action{None, Promote, Demote, Remove, ForceRemove} {
		if ca.String() == s {
			*a = ca
			return nil
		}
	}
	return fmt.Errorf("%q is not a valid configAction", s)
}
