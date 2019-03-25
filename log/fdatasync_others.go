// +build !linux,!openbsd

package log

func (f *mmapFile) fdatasync() error {
	return f.Sync()
}
