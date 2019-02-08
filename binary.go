package raft

import (
	"encoding/binary"
	"io"
)

// byteOrder used for encode/decode
var byteOrder = binary.LittleEndian

func readUint64(r io.Reader) (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(b), nil
}

func readUint32(r io.Reader) (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(b), nil
}

func readUint8(r io.Reader) (uint8, error) {
	if r, ok := r.(io.ByteReader); ok {
		return r.ReadByte()
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readBool(r io.Reader) (bool, error) {
	b, err := readUint8(r)
	return b > 0, err
}

func readBytes(r io.Reader) ([]byte, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func readString(r io.Reader) (string, error) {
	b, err := readBytes(r)
	return string(b), err
}

// -----------------------------------------------------

func writeUint64(w io.Writer, v uint64) error {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, v)
	_, err := w.Write(b)
	return err
}

func writeUint32(w io.Writer, v uint32) error {
	b := make([]byte, 4)
	byteOrder.PutUint32(b, v)
	_, err := w.Write(b)
	return err
}

func writeUint8(w io.Writer, v uint8) error {
	if w, ok := w.(io.ByteWriter); ok {
		return w.WriteByte(v)
	}
	b := []byte{v}
	_, err := w.Write(b)
	return err
}

func writeBool(w io.Writer, v bool) error {
	if v {
		return writeUint8(w, 1)
	}
	return writeUint8(w, 0)
}

func writeBytes(w io.Writer, b []byte) error {
	if err := writeUint32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeString(w io.Writer, s string) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}
