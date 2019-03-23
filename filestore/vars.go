package filestore

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
)

var byteOrder = binary.LittleEndian

type Vars struct {
	f   *os.File
	off int64
	b   []byte
}

func NewVars(file string) (*Vars, error) {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		b := [1 + 4*8 + 4*8]byte{1}
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
	if len(b) != 1+4*8+4*8 {
		_ = f.Close()
		return nil, fmt.Errorf("filestore.vars: size(%s)=%d, want %d", file, len(b), 1+4*8+4*8)
	}
	off := int64(b[0])
	if off != 1 && off != 33 {
		_ = f.Close()
		return nil, fmt.Errorf("filestore.vars: off=%d, want 1 or 33", off)
	}
	b = append([]byte(nil), b[off:off+4*8]...)
	return &Vars{f, off, b}, nil
}

func (v *Vars) Close() error {
	return v.f.Close()
}

func (v *Vars) GetIdentity() (cid, nid uint64, err error) {
	return byteOrder.Uint64(v.b[:]), byteOrder.Uint64(v.b[8:]), nil
}

func (v *Vars) SetIdentity(cid, nid uint64) error {
	b := make([]byte, 32)
	copy(b, v.b)
	byteOrder.PutUint64(b, cid)
	byteOrder.PutUint64(b[8:], nid)
	return v.update(b)
}

func (v *Vars) GetVote() (term, vote uint64, err error) {
	return byteOrder.Uint64(v.b[16:]), byteOrder.Uint64(v.b[24:]), nil
}

func (v *Vars) SetVote(term, vote uint64) error {
	b := append([]byte(nil), v.b...)
	byteOrder.PutUint64(b[16:], term)
	byteOrder.PutUint64(b[24:], vote)
	return v.update(b)
}

func (v *Vars) update(b []byte) error {
	off := int64(1)
	if v.off == 1 {
		off = 33
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

func (v *Vars) writeAt(b []byte, off int64) error {
	if _, err := v.f.WriteAt(b, off); err != nil {
		return err
	}
	return v.f.Sync()
}
