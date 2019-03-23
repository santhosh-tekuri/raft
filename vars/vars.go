package vars

import (
	"path/filepath"
)

type Vars struct {
	identity *Var
	vote     *Var
}

func New(dir string) (*Vars, error) {
	identity, err := NewVar(filepath.Join(dir, "identity"))
	if err != nil {
		return nil, err
	}
	vote, err := NewVar(filepath.Join(dir, "vote"))
	if err != nil {
		return nil, err
	}
	return &Vars{identity, vote}, nil
}

func (v *Vars) Close() error {
	err1 := v.identity.Close()
	err2 := v.vote.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (v *Vars) GetIdentity() (cid, nid uint64, err error) {
	return v.identity.Get()
}

func (v *Vars) SetIdentity(cid, nid uint64) error {
	return v.identity.Set(cid, nid)
}

func (v *Vars) GetVote() (term, vote uint64, err error) {
	return v.vote.Get()
}

func (v *Vars) SetVote(term, vote uint64) error {
	return v.vote.Set(term, vote)
}
