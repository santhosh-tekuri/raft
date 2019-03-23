package vars_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/santhosh-tekuri/raft/vars"
)

func TestNewVar(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		valid   bool
	}{
		{"noFile", nil, true},
		{"zeroSize", make([]byte, 0), false},
		{"smallSize", make([]byte, 20), false},
		{"bigSize", make([]byte, 100), false},
		{"zeroOff", slice33(0), false},
		{"wrongOff", slice33(5), false},
		{"off1", slice33(1), true},
		{"off17", slice33(17), true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fname, err := tempFile(".var", test.content)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(fname)
			v, err := vars.NewVar(fname)
			if valid := err == nil; valid != test.valid {
				t.Fatalf("valid: got %v, want %v : %v", valid, test.valid, err)
			}
			if test.valid {
				defer v.Close()
				if err = ensureVar(v, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVars(t *testing.T) {
	fname, err := tempFile(".var", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(fname)
	v, err := vars.NewVar(fname)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err = v.Set(i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = ensureVar(v, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = v.Close(); err != nil {
			t.Fatal(err)
		}
		if v, err = vars.NewVar(fname); err != nil {
			t.Fatal(err)
		}
		if err = ensureVar(v, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
	}
}

// helpers ------------------------------------------------------------

// slice33 returns a slice of length 33, with first byte initialized to given value.
func slice33(first byte) []byte {
	b := [33]byte{first}
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

func ensureVar(v *vars.Var, w1, w2 uint64) error {
	g1, g2, err := v.Get()
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}
	if g1 != w1 {
		return fmt.Errorf("v1: got %v, want %v", g1, w1)
	}
	if g2 != w2 {
		return fmt.Errorf("v2: got %v, want %v", g2, w2)
	}
	return nil
}
