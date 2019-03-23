package filestore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func tempFile(pattern string, content []byte) (string, error) {
	f, err := ioutil.TempFile("", pattern)
	if err != nil {
		return "", fmt.Errorf("create tempFile: %v", err)
	}
	if content == nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
	} else {
		defer f.Close()
		if _, err := f.Write(content); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return "", fmt.Errorf("init tempFile: %v", err)
		}
	}
	return f.Name(), nil
}

func slice(b [65]byte) []byte { return b[:] }

func TestNewVars(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		valid   bool
	}{
		{"noFile", nil, true},
		{"zeroSize", make([]byte, 0), false},
		{"smallSize", make([]byte, 20), false},
		{"bigSize", make([]byte, 100), false},
		{"zeroOff", slice([65]byte{0}), false},
		{"wrongOff", slice([65]byte{5}), false},
		{"off1", slice([65]byte{1}), true},
		{"off33", slice([65]byte{33}), true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fname, err := tempFile(".vars", test.content)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(fname)
			v, err := NewVars(fname)
			if valid := err == nil; valid != test.valid {
				t.Fatalf("valid: got %v, want %v : %v", valid, test.valid, err)
			}
			if test.valid {
				defer v.Close()
				cid, nid, err := v.GetIdentity()
				if err != nil {
					t.Fatalf("getIdentity: %v", err)
				}
				if cid != 0 {
					t.Fatalf("cid: got %v, want %v", cid, 0)
				}
				if nid != 0 {
					t.Fatalf("cid: got %v, want %v", cid, 0)
				}

				term, vote, err := v.GetVote()
				if err != nil {
					t.Fatalf("getVote: %v", err)
				}
				if term != 0 {
					t.Fatalf("term: got %v, want %v", cid, 0)
				}
				if vote != 0 {
					t.Fatalf("vote: got %v, want %v", cid, 0)
				}
			}
		})
	}
}
