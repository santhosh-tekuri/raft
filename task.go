package raft

type Task interface {
	execute(r *Raft)
	Done() <-chan struct{}
	Err() error
	Result() interface{}
	reply(interface{})
}

type task struct {
	fn     func(t Task, r *Raft)
	result interface{}
	done   chan struct{}
}

func (t *task) execute(r *Raft) {
	t.fn(t, r)
}

func (t *task) Done() <-chan struct{} {
	return t.done
}

func (t *task) Err() error {
	if err, ok := t.result.(error); ok {
		return err
	}
	return nil
}

func (t *task) Result() interface{} {
	if _, ok := t.result.(error); ok {
		return nil
	}
	return t.result
}

func (t *task) reply(result interface{}) {
	if t != nil {
		t.result = result
		if t.done != nil {
			close(t.done)
		}
	}
}

// ------------------------------------------------------------------------

func ApplyEntry(data []byte) Task {
	return &task{
		fn: func(t Task, r *Raft) {
			applyEntry(t, r, data)
		},
		done: make(chan struct{}),
	}
}

func applyEntry(t Task, r *Raft, data []byte) {
	if r.state != leader {
		t.reply(NotLeaderError{r.leaderID})
		return
	}

	ldr := r.ldrState
	typ := entryCommand
	if ldr.startIndex == ldr.lastLogIndex+1 {
		typ = entryNoop
	}

	ne := newEntry{
		entry: &entry{
			typ:   typ,
			data:  data,
			index: ldr.lastLogIndex + 1,
			term:  ldr.term,
		},
		task: t,
	}

	// append entry to local log
	if ne.typ == entryNoop {
		debug(r, "log.append noop", ne.index)
	} else {
		debug(r, "log.append cmd", ne.index)
	}
	ldr.storage.append(ne.entry)
	ldr.lastLogIndex, ldr.lastLogTerm = ne.index, ne.term
	ldr.newEntries.PushBack(ne)

	// we updated lastLogIndex, so notify replicators
	ldr.notifyReplicators()
}

// ------------------------------------------------------------------------
