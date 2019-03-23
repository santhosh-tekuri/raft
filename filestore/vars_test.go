package filestore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

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
		{"zeroOff", slice65(0), false},
		{"wrongOff", slice65(5), false},
		{"off1", slice65(1), true},
		{"off33", slice65(33), true},
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
				if err = ensureVars(v, 0, 0, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// helpers ------------------------------------------------------------

// slice65 returns a slice of length 65, with first byte initialized to given value.
func slice65(first byte) []byte {
	b := [65]byte{first}
	return b[:]
}

// tempFile creates a tempFile with given content, and returns its name.
// if content is nil, it does not create file, but returns free name.
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

func ensureVars(vars *Vars, cid, nid, term, vote uint64) error {
	c, n, err := vars.GetIdentity()
	if err != nil {
		return fmt.Errorf("getIdentity: %v", err)
	}
	if c != cid {
		return fmt.Errorf("cid: got %v, want %v", c, cid)
	}
	if n != nid {
		return fmt.Errorf("nid: got %v, want %v", n, nid)
	}

	t, v, err := vars.GetVote()
	if err != nil {
		return fmt.Errorf("getVote: %v", err)
	}
	if t != term {
		return fmt.Errorf("term: got %v, want %v", t, term)
	}
	if v != vote {
		return fmt.Errorf("vote: got %v, want %v", v, vote)
	}

	return nil
}
