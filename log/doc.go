// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package log implements append only list of entries persisted to disk.
//
// Log is list of entries which are persisted to disk. It is append only list,
// but RemoveLTE and RemoveGTE can be used to remove entries from the beginning
// or ending of log.
//
// Index of log starts from 1. Entries start from Log.PrevIndex+1 to Log.LastIndex. Log.Count gives number of entries.
// In empty log, both PrevIndex and LastIndex is zero. Entry is []byte which is opaque
// to log and is not interpreted.
//
// On Disk Structure
//
// Log is persisted on disk in the directory specified in Open call as a series of
// segment files. The size of each segment file is specified in Options.SegmentSize.
// The segment file is pre allocated, that means if Options.SegmentSize is 16MB,
// then when segment file is created its size will be 16MB. The segment files are
// mapped into memory as read-write.
//
// The segment file name tells prevIndex of the segment. for example segment file named
// 100.log contains entries from 101. So the first segment file will be named 0.log.
//
// The segment file structure is as follows:
//
//     entry1 entry2 entry3 ................................................
//     .....................................................................
//     .....................................................................
//     .....................................................................
//     .................................. offset2(8B) offset1(8B) header(8B)
//
// Entries are stored in beginning of file sequentially. This allows GetN to return
// multiple entries from mmapped segment files easier.
//
// Last 8 bytes in file is the header. Header tells number of entries in the segment file.
// It is encoded as binary.LittleEndian.
//
// Before header are the offsets of each entry in reverse order. Each offset is 8 bytes encoded
// as binary.LittleEndian. If segment file has n entries, there will be n+1 offsets.
// The last offset tells where the last entry ends(or where new entry will start)
//
// Appending Entries
//
// New entries are appended using Log.Append. When current segment is full it automatically creates
// new segment file. Log.Commit must be called to ensure that the entry added is persisted to disk
//
//  if err := l.Append([]byte("hello-world"); err!=nil {
//      panic(err)
//  }
//  if err := l.Commit(); err!=nil {
//      panic(err)
//  }
//
// Appending multiple entries and calling Log.Commit at end, gives better performance. When current segment
// is full, before creating new segment current segment is committed implicitly. This means only last segment
// will be dirty. Implicit commit also happens on calls to RemoveLTE, RemoveGTE, Close.
//
// Transactions and rollbacks are not supported. But RemoveGTE can be used to mimic rollback.
//
// If implicit/explicit commit is not done and program crashes, only entries and offsets in last segment may be lost.
// Some times the last segment may still has the new entries and offsets, but header will not reflect the new entries.
// The header is updated only on commit.
//
// Removing Entries
//
// RemoveGTE(i) removes all entries >=i. GTE stands for Greater Than Equals. After call to RemoveGTE(i), Log.LastIndex
// will become i-1. RemoveIndex(i) is nop, if i>Log.LastIndex.
//
// To remove entries in beginning of log use RemoveLTE. LTE stands for Less Than Equals. RemoveLTE is little bit tricky.
//
// Consider a log file with  with 3 segments 0.log, 121.log, 210.log and lastIndex is 250. The library only allows
// to remove previous segments. i.e, you can remove entries <=121 or <=210. RemoveLTE(121) deletes 0.log file.
// RemoveLTE(210) deletes 0.log and 121.log files. If you call RemoveLTE(150), it still removes only entries <=121.
// You can think of RemoveLTE(i) as a hint to remove entries <=i. If you would like to find how many entries will be
// removed before calling RemoveLTE, use CanLTE. for example in this example CanLTE(150) returns 121
//
// Log.Reset(i) clears all entries and resets lastIndex to i. This is used to discard complete log.
//
// Getting Entries
//
// Log.Get(i) returns i-th entry. If i>LastIndex it panics. If i<=PrevIndex it returns ErrNotFound.
//
// Log.GetN(i, n) returns n entries from i-th entry inclusive. If i+n-1>LastIndex it panics. If i<=PrevIndex it returns
// ErrNotFound. GetN returns [][]byte, one []byte per segment.
//
// The []byte returned by both Get and Getn are memory mapped. returned []byte is valid until you call one of the method:
// RemoveGTE, RemoveLTE, Close.
//
// Views
//
// Log is not thread safe for use from multiple goroutines. Instead of synchronizing at application end, use views.
// Views is a lock free data structure that provide read only view. There should be one writer goroutine which appends
// new entries to log, and multiple reader goroutines reading the log using views. A single View can be shared by
// multiple go routines or for each goroutine you can create one view. Below shows example usage:
//
//  func writer(l *log.Log, readers ...chan *log.Log) {
//      var i int
//      for cmd := range <-commandsCh {
//          entry := cmd.encode()
//          if err:= l.Append(entry); err!=nil {
//              panic(err)
//          }
//          i++
//          if i==100 { // batch size 100
//              for _, reader := range readers {
//                  reader <-l.View()
//              }
//          }
//      }
//  }
//
//  func reader(logCh chan *log.Log, conn *net.Conn) {
//      var i uint64 = 1
//      for {
//          l := <-logCh
//          entries := l.GetN(i, l.LastIndex()-i+1)
//          buffs := net.Buffers(entries)
//          if err := buffs.WriteTo(conn); err!=nil {
//              panic(err)
//          }
//      }
//  }
//
// Only Log.Append is safe to use in writer goroutine. If you have to call RemoveLTE, RemoveGTE etc, you have to synchronize
// your self. After calls to these methods the previously created views should no longer be used. You should create new views.
//
// View is actually *log.Log from implementation point of view. Thus it have Append, RemoveLTE methods on view,
// but they should not be used on view. The library has no checks to prevent that.
package log
