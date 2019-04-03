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
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
)

func TestBinary(t *testing.T) {
	for _, test := range []uint64{0, 123, math.MaxUint64} {
		name := fmt.Sprintf("uint64(%d)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			_ = writeUint64(b, test)
			if b.Len() != 8 {
				t.Fatalf("wrote %d bytes. want %d bytes", b.Len(), 8)
			}
			v, err := readUint64(b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != test {
				t.Fatalf("got %d, want %d", v, test)
			}
			if b.Len() != 0 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 0)
			}
		})
	}

	for _, test := range []uint32{0, 123, math.MaxUint32} {
		name := fmt.Sprintf("uint32(%d)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			_ = writeUint32(b, test)
			if b.Len() != 4 {
				t.Fatalf("wrote %d bytes. want %d bytes", b.Len(), 4)
			}
			v, err := readUint32(b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != test {
				t.Fatalf("got %d, want %d", v, test)
			}
			if b.Len() != 0 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 0)
			}
		})
	}

	for _, test := range []uint8{0, 100, math.MaxUint8} {
		name := fmt.Sprintf("uint8(%d)", test)
		t.Run(name, func(t *testing.T) {
			bb := []interface {
				io.ReadWriter
				Len() int
			}{
				new(bytes.Buffer),
				readWriter{new(bytes.Buffer)},
			}
			for _, b := range bb {
				_ = writeUint8(b, test)
				if b.Len() != 1 {
					t.Fatalf("wrote %d bytes. want %d bytes", b.Len(), 1)
				}
				v, err := readUint8(b)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if v != test {
					t.Fatalf("got %d, want %d", v, test)
				}
				if b.Len() != 0 {
					t.Fatalf("bytes left. got %d, want %d", b.Len(), 0)
				}
			}
		})
	}

	for _, test := range []bool{true, false} {
		name := fmt.Sprintf("bool(%v)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			_ = writeBool(b, test)
			if b.Len() != 1 {
				t.Fatalf("wrote %d bytes. want %d bytes", b.Len(), 1)
			}
			v, err := readBool(b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != test {
				t.Fatalf("got %v, want %v", v, test)
			}
			if b.Len() != 0 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 0)
			}
		})
	}

	for _, test := range []string{"", "nonempty"} {
		name := fmt.Sprintf("bytes(%q)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			_ = writeBytes(b, []byte(test))
			b.WriteString("junk")
			v, err := readBytes(b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(v) != test {
				t.Fatalf("got %q, want %q", string(v), test)
			}
			if b.Len() != 4 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 4)
			}
		})
	}

	for _, test := range []string{"", "nonempty"} {
		name := fmt.Sprintf("string(%q)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			_ = writeString(b, test)
			b.WriteString("junk")
			v, err := readString(b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != test {
				t.Fatalf("got %v, want %v", v, test)
			}
			if b.Len() != 4 {
				t.Fatalf("bytes left. got %d, want %d", b.Len(), 4)
			}
		})
	}
}

// helpers --------------------------------------------

type readWriter struct {
	buf *bytes.Buffer
}

func (rw readWriter) Read(b []byte) (int, error) {
	return rw.buf.Read(b)
}
func (rw readWriter) Write(b []byte) (int, error) {
	return rw.buf.Write(b)
}
func (rw readWriter) Len() int {
	return rw.buf.Len()
}
