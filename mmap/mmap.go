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

// Package mmap allows mapping files into memory.
package mmap

import (
	"os"
)

// File represents a file mapped into memory.
type File struct {
	name string
	// Data is the mmaped data of the file
	Data   []byte
	handle interface{}
}

// OpenFile maps given file into memory. The arguments are same as os.OpenFile.
func OpenFile(name string, flag int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return openFile(f, flag, int(info.Size()))
}

// Name returns the name of the file as presented to OpenFile.
func (f *File) Name() string {
	return f.name
}
