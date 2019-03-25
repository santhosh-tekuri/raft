// +build !linux,!openbsd

package log

func (f *mmapFile) syncData() error {
	return f.Sync()
}
