package fileutil

import "os"

// Replace renames src over dst across platforms. Windows does not allow
// os.Rename to replace an existing file, so a backup is used for rollback.
func Replace(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	backup := dst + ".bak"
	_ = os.Remove(backup)
	if err := os.Rename(dst, backup); err != nil {
		if os.IsNotExist(err) {
			return os.Rename(src, dst)
		}
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		_ = os.Rename(backup, dst)
		return err
	}
	return os.Remove(backup)
}
