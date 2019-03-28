// +build !linux,!openbsd

package mmap

func (f *File) SyncData() error {
	return f.Sync()
}

func (f *File) Sync() error {
	return f.File.Sync()
}
