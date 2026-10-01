# internal/fileutil

被文件后端存储与集群注册表使用的小型文件系统辅助函数。

## Replace

`Replace(src, dst)` 跨平台地将 `src` 原子重命名覆盖到 `dst`。在 Unix 上一次重命名即可；在 Windows 上 `os.Rename` 无法覆盖已存在文件，因此先把它移到 `.bak` 备份文件，失败时回滚。`FileStore` 与 `FileRegistry` 都会先写临时文件再调用 `Replace`，以避免留下半写的文档。
