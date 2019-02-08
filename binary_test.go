package raft

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
)

func TestUint64(t *testing.T) {
	tests := []uint64{0, 123, math.MaxUint64}
	for _, test := range tests {
		name := fmt.Sprintf("uint64(%d)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			writeUint64(b, test)
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
}

func TestUint32(t *testing.T) {
	tests := []uint32{0, 123, math.MaxUint32}
	for _, test := range tests {
		name := fmt.Sprintf("uint32(%d)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			writeUint32(b, test)
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
}

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

func TestUint8(t *testing.T) {
	tests := []uint8{0, 100, math.MaxUint8}
	for _, test := range tests {
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
				writeUint8(b, test)
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
}

func TestBool(t *testing.T) {
	tests := []bool{true, false}
	for _, test := range tests {
		name := fmt.Sprintf("bool(%v)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			writeBool(b, test)
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
}

func TestBytes(t *testing.T) {
	tests := []string{"", "nonempty"}
	for _, test := range tests {
		name := fmt.Sprintf("bytes(%q)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			writeBytes(b, []byte(test))
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
}

func TestString(t *testing.T) {
	tests := []string{"", "nonempty"}
	for _, test := range tests {
		name := fmt.Sprintf("string(%q)", test)
		t.Run(name, func(t *testing.T) {
			b := new(bytes.Buffer)
			writeString(b, test)
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
