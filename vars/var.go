package vars

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
)

var byteOrder = binary.LittleEndian

type Var struct {
	f   *os.File
	off int64
	b   []byte
}

func NewVar(file string) (*Var, error) {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		b := [33]byte{1}
		if err := ioutil.WriteFile(file, b[:], 0600); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(file, os.O_RDWR, 644)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(b) != 33 {
		_ = f.Close()
		return nil, fmt.Errorf("vars: size(%s)=%d, want %d", file, len(b), 33)
	}
	off := int64(b[0])
	if off != 1 && off != 17 {
		_ = f.Close()
		return nil, fmt.Errorf("vars: off(%s)=%d, want 1 or 17", file, off)
	}
	b = append([]byte(nil), b[off:off+16]...)
	return &Var{f, off, b}, nil
}

func (v *Var) Close() error {
	return v.f.Close()
}

func (v *Var) Get() (v1, v2 uint64, err error) {
	return byteOrder.Uint64(v.b[:]), byteOrder.Uint64(v.b[8:]), nil
}

func (v *Var) Set(v1, v2 uint64) error {
	b := make([]byte, 16)
	copy(b, v.b)
	byteOrder.PutUint64(b, v1)
	byteOrder.PutUint64(b[8:], v2)

	off := int64(1)
	if v.off == 1 {
		off = 17
	}
	if err := v.writeAt(b, off); err != nil {
		return err
	}
	if err := v.writeAt([]byte{byte(off)}, 0); err != nil {
		return err
	}
	v.off, v.b = off, b
	return nil
}

func (v *Var) writeAt(b []byte, off int64) error {
	if _, err := v.f.WriteAt(b, off); err != nil {
		return err
	}
	return v.f.Sync()
}
