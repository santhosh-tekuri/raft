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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type value struct {
	dir string
	ext string
	v1  uint64
	v2  uint64
}

func openValue(dir, ext string, mode os.FileMode) (*value, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*"+ext))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		f, err := os.OpenFile(valueFile(dir, ext, 0, 0), os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		if err := syncDir(dir); err != nil {
			return nil, err
		}
		matches = []string{f.Name()}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("raft: more than one file with ext %s in dir %s", ext, dir)
	}
	s := strings.TrimSuffix(filepath.Base(matches[0]), ext)
	i := strings.IndexByte(s, '-')
	if i == -1 {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	v1, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	v2, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	return &value{
		dir: dir,
		ext: ext,
		v1:  uint64(v1),
		v2:  uint64(v2),
	}, nil
}

func (v *value) get() (uint64, uint64) {
	return v.v1, v.v2
}

func (v *value) set(v1, v2 uint64) error {
	if v1 == v.v1 && v2 == v.v2 {
		return nil
	}
	curPath := valueFile(v.dir, v.ext, v.v1, v.v2)
	newPath := valueFile(v.dir, v.ext, v1, v2)
	if err := os.Rename(curPath, newPath); err != nil {
		return err
	}
	if err := syncDir(v.dir); err != nil {
		return err
	}
	v.v1, v.v2 = v1, v2
	return nil
}

func valueFile(dir, ext string, v1, v2 uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d-%d%s", v1, v2, ext))
}
