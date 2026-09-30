# internal/fileutil

Small filesystem helpers used by the file-backed store and cluster registry.

## Replace

`Replace(src, dst)` atomically renames `src` over `dst` across platforms. On
Unix a single rename suffices; on Windows, where `os.Rename` cannot replace an
existing file, it moves the destination aside to a `.bak` file first and rolls
back on failure. Both `FileStore` and `FileRegistry` write a temporary file and
call `Replace` to avoid leaving a partially written document.
