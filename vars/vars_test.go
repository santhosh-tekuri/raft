package vars_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/santhosh-tekuri/raft/vars"
)

func TestVars_identity(t *testing.T) {
	dir, err := ioutil.TempDir("", ".vars")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	v, err := vars.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err = v.SetIdentity(i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = ensureVars(v, i*2, i*2+1, 0, 0); err != nil {
			t.Fatal(err)
		}
		if err = v.Close(); err != nil {
			t.Fatal(err)
		}
		if v, err = vars.New(dir); err != nil {
			t.Fatal(err)
		}
		if err = ensureVars(v, i*2, i*2+1, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVars_vote(t *testing.T) {
	dir, err := ioutil.TempDir("", ".vars")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	v, err := vars.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err = v.SetVote(i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = ensureVars(v, 0, 0, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
		if err = v.Close(); err != nil {
			t.Fatal(err)
		}
		if v, err = vars.New(dir); err != nil {
			t.Fatal(err)
		}
		if err = ensureVars(v, 0, 0, i*2, i*2+1); err != nil {
			t.Fatal(err)
		}
	}
}

// helpers ------------------------------------------------------------

func ensureVars(vars *vars.Vars, cid, nid, term, vote uint64) error {
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
