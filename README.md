# raft

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![GoDoc](https://godoc.org/github.com/santhosh-tekuri/raft?status.svg)](https://godoc.org/github.com/santhosh-tekuri/raft)
[![Go Report Card](https://goreportcard.com/badge/github.com/santhosh-tekuri/raft)](https://goreportcard.com/report/github.com/santhosh-tekuri/raft)
[![Build Status](https://travis-ci.org/santhosh-tekuri/raft.svg?branch=master)](https://travis-ci.org/santhosh-tekuri/raft) 
[![codecov.io](https://codecov.io/github/santhosh-tekuri/raft/coverage.svg?branch=master)](https://codecov.io/github/santhosh-tekuri/raft?branch=master)

Package raft implements Raft Concensus Algorithm as described in https://raft.github.io/raft.pdf.

Features implemented:
- Leader Election
- Log Replication
- Membership Changes
- Log Compaction
- `raftctl` command line tool to inspect and modify cluster

see example/kvstore for usage
