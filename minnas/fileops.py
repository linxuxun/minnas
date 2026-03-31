"""
fileops.py — Virtual File System for MiniNAS.

Implements full file semantics (open/read/write/close/seek/truncate/append)
backed by the CAS (Content-Addressable Store).

The VFS maintains a working directory view backed by snapshots. Modified files
are buffered in memory and committed to the CAS on close()/flush().
"""

import io
import os
import time
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Optional

from minnas.snapshot import SnapshotStore, ObjectNotFoundError
from minnas.namespace import NamespaceStore


# Mode interpretation:
# 'r'  — read existing (error if not exists)
# 'w'  — write (create or truncate)
# 'a'  — append (create or seek to end)
# 'r+' — read+write existing
# 'w+' — read+write (create or truncate)
# 'a+' — append+read (create, seek to end on write)

VALID_MODES = {"r", "w", "a", "r+", "w+", "a+"}


class FileModeError(Exception):
    """Raised for invalid file modes or operations."""
    pass


class FileNotFoundError_(Exception):
    """Raised when a file is not found in the VFS."""
    pass


class FileClosedError(Exception):
    """Raised when operating on a closed file."""
    pass


@dataclass
class VirtualFile:
    """
    A virtual file with buffered I/O backed by the CAS.

    Attributes:
        path: The file path within the virtual filesystem.
        mode: File mode string ('r', 'w', 'a', 'r+', 'w+', 'a+').
    """
    path: str
    mode: str

    _buffer: io.BytesIO = field(default_factory=io.BytesIO)
    _position: int = 0
    _modified: bool = False
    _blob_sha: Optional[str] = field(default=None)
    _closed: bool = False
    _created: bool = False
    _snapshots: Optional[SnapshotStore] = field(default=None, repr=False)

    def __init__(
        self,
        path: str,
        mode: str,
        snapshots: Optional[SnapshotStore] = None,
    ):
        self.path = path
        self.mode = mode
        self._buffer: io.BytesIO = io.BytesIO()
        self._position: int = 0
        self._modified: bool = False
        self._blob_sha: Optional[str] = None
        self._closed: bool = False
        self._created: bool = False
        self._snapshots: Optional[SnapshotStore] = snapshots
        if self.mode not in VALID_MODES:
            raise FileModeError(f"Invalid file mode: {self.mode}")

    def _check_closed(self) -> None:
        if self._closed:
            raise FileClosedError(f"File is closed: {self.path}")

    def read(self, n: int = -1) -> bytes:
        """
        Read up to n bytes from the current position.

        Args:
            n: Number of bytes to read. -1 means read all remaining.

        Returns:
            Bytes read.

        Raises:
            FileModeError: If file is not open for reading.
        """
        self._check_closed()
        if "r" not in self.mode and "+" not in self.mode:
            raise FileModeError(f"File not open for reading: {self.path}")

        data = self._buffer.getvalue()
        remaining = data[self._position:]
        if n == -1:
            result = remaining
        else:
            result = remaining[:n]

        self._position += len(result)
        return result

    def write(self, data: bytes) -> int:
        """
        Write data at the current position.

        Args:
            data: Bytes to write.

        Returns:
            Number of bytes written.

        Raises:
            FileModeError: If file is not open for writing.
        """
        self._check_closed()
        if "w" not in self.mode and "+" not in self.mode and "a" not in self.mode:
            raise FileModeError(f"File not open for writing: {self.path}")

        content = self._buffer.getvalue()

        if "a" in self.mode and self.mode != "r+":
            # Append mode: always seek to end first
            self._position = len(content)

        if self._position > len(content):
            # Extend with nulls if seeking past end
            content = content + b"\0" * (self._position - len(content))

        content = content[:self._position] + data
        self._buffer = io.BytesIO(content)
        self._position += len(data)
        self._modified = True
        return len(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Move the file position.

        Args:
            offset: Byte offset.
            whence: SEEK_SET (0), SEEK_CUR (1), or SEEK_END (2).

        Returns:
            New absolute position.
        """
        self._check_closed()
        content_len = len(self._buffer.getvalue())

        if whence == 0:  # SEEK_SET
            new_pos = offset
        elif whence == 1:  # SEEK_CUR
            new_pos = self._position + offset
        elif whence == 2:  # SEEK_END
            new_pos = content_len + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        if new_pos < 0:
            new_pos = 0
        self._position = new_pos
        return new_pos

    def tell(self) -> int:
        """Return current file position."""
        self._check_closed()
        return self._position

    def truncate(self, size: Optional[int] = None) -> int:
        """
        Truncate the file to at most size bytes.

        Args:
            size: New file size. Defaults to current position.

        Returns:
            New file size.
        """
        self._check_closed()
        content = self._buffer.getvalue()
        if size is None:
            size = self._position
        new_content = content[:size]
        self._buffer = io.BytesIO(new_content)
        if self._position > size:
            self._position = size
        self._modified = True
        return len(new_content)

    def append(self, data: bytes) -> int:
        """
        Append data to the end of the file.

        Args:
            data: Bytes to append.

        Returns:
            Number of bytes appended.
        """
        self._check_closed()
        current_end = len(self._buffer.getvalue())
        self._buffer = io.BytesIO(self._buffer.getvalue() + data)
        self._modified = True
        return len(data)

    def flush(self) -> str:
        """
        Flush the buffer and commit to CAS.

        Returns:
            SHA of the stored blob.

        Raises:
            FileModeError: If file is not writable.
        """
        self._check_closed()
        if "w" not in self.mode and "+" not in self.mode and "a" not in self.mode:
            raise FileModeError(f"File not open for writing: {self.path}")

        if self._snapshots is None:
            raise RuntimeError("No snapshot store attached")

        data = self._buffer.getvalue()
        sha = self._snapshots.store(data)
        self._blob_sha = sha
        self._modified = False
        return sha

    def close(self) -> str:
        """
        Close the file, committing if modified.

        Returns:
            SHA of the stored blob (or existing SHA if unchanged).

        Raises:
            FileClosedError: If file is already closed.
        """
        self._check_closed()

        if self._modified and self._snapshots is not None:
            self.flush()
        elif self._blob_sha is None and self._snapshots is not None:
            # File has content but wasn't explicitly flushed
            data = self._buffer.getvalue()
            if data:
                self._blob_sha = self._snapshots.store(data)

        self._closed = True
        return self._blob_sha or ""

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def modified(self) -> bool:
        return self._modified

    @property
    def blob_sha(self) -> Optional[str]:
        return self._blob_sha

    @property
    def size(self) -> int:
        return len(self._buffer.getvalue())


class VirtualFS:
    """
    Virtual File System backed by the CAS.

    Provides file operations (open/read/write/seek/close/truncate)
    and directory operations (ls/mkdir/rm/exists/stat) on top of
    the content-addressable store.

    The VFS maintains:
    - _cwd: Current working directory (for relative paths).
    - _open_files: Dict mapping file descriptors (int) -> VirtualFile.
    - _snapshots: SnapshotStore for reading/writing blobs.
    - _current_tree: Current snapshot tree (path -> blob_sha).
    - _next_fd: Counter for allocating file descriptors.
    """

    def __init__(self, snapshots: SnapshotStore, initial_tree: Optional[dict] = None):
        """
        Initialize the virtual filesystem.

        Args:
            snapshots: SnapshotStore instance for CAS operations.
            initial_tree: Optional initial tree (path -> blob_sha).
        """
        self._snapshots = snapshots
        self._current_tree: dict = dict(initial_tree) if initial_tree else {}
        self._cwd: str = "/"
        self._open_files: dict = {}
        self._next_fd: int = 3  # Reserve 0,1,2 for stdin/stdout/stderr concepts

    def _resolve_path(self, path: str) -> str:
        """Resolve a path (possibly relative) to an absolute VFS path."""
        if not path.startswith("/"):
            # Relative to cwd
            path = str(PurePosixPath(self._cwd) / path)
        # Normalize ..
        parts = []
        for part in PurePosixPath(path).parts:
            if part == ".." and parts:
                parts.pop()
            elif part != ".":
                parts.append(part)
        return "/" + "/".join(parts) if parts else "/"

    def _alloc_fd(self) -> int:
        """Allocate a new file descriptor."""
        fd = self._next_fd
        self._next_fd += 1
        return fd

    def open(self, path: str, mode: str = "r") -> int:
        """
        Open a virtual file.

        Args:
            path: File path within the VFS.
            mode: File mode ('r', 'w', 'a', 'r+', 'w+', 'a+').

        Returns:
            File descriptor (int).

        Raises:
            FileModeError: If mode is invalid.
            FileNotFoundError_: If mode='r' and file doesn't exist.
        """
        if mode not in VALID_MODES:
            raise FileModeError(f"Invalid file mode: {mode}")

        path = self._resolve_path(path)
        existing_sha = self._current_tree.get(path)

        vf = VirtualFile(path=path, mode=mode)
        vf._snapshots = self._snapshots

        # Load existing content for read/update modes
        if existing_sha and ("r" in mode or "+" in mode or "a" in mode):
            try:
                data = self._snapshots.load(existing_sha)
                vf._buffer = io.BytesIO(data)
                vf._blob_sha = existing_sha
            except ObjectNotFoundError:
                pass

        # Handle write/truncate mode
        if "w" in mode:
            vf._buffer = io.BytesIO(b"")
            vf._modified = True
            vf._created = not bool(existing_sha)
        elif "a" in mode and not "+" in mode:
            # Seek to end for append
            vf._position = len(vf._buffer.getvalue())

        fd = self._alloc_fd()
        self._open_files[fd] = vf
        return fd

    def read(self, fd: int, n: int = -1) -> bytes:
        """Read from an open file descriptor."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        return self._open_files[fd].read(n)

    def write(self, fd: int, data: bytes) -> int:
        """Write to an open file descriptor."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        return self._open_files[fd].write(data)

    def lseek(self, fd: int, offset: int, whence: int = 0) -> int:
        """Seek within an open file descriptor."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        return self._open_files[fd].seek(offset, whence)

    def tell(self, fd: int) -> int:
        """Return current position of an open file descriptor."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        return self._open_files[fd].tell()

    def truncate(self, path: str, size: Optional[int] = None) -> int:
        """Truncate a file to a given size."""
        path = self._resolve_path(path)
        fd = self.open(path, "r+")
        try:
            return self._open_files[fd].truncate(size)
        finally:
            self.close(fd)

    def close(self, fd: int) -> str:
        """Close a file descriptor, committing changes to the VFS tree."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")

        vf = self._open_files.pop(fd)
        sha = vf.close()

        if sha:
            self._current_tree[vf.path] = sha
        elif vf.path in self._current_tree and not sha:
            # File was truncated to empty or deleted
            pass

        return sha

    def stat(self, path: str) -> dict:
        """
        Get file metadata.

        Returns:
            Dict with keys: path, size, mtime, ctime, sha, is_dir, exists.
        """
        path = self._resolve_path(path)

        if path in self._current_tree:
            sha = self._current_tree[path]
            size = 0
            if sha:
                try:
                    data = self._snapshots.load(sha)
                    size = len(data)
                except ObjectNotFoundError:
                    pass
            return {
                "path": path,
                "size": size,
                "sha": sha,
                "is_dir": False,
                "exists": True,
            }
        else:
            return {
                "path": path,
                "size": 0,
                "sha": None,
                "is_dir": False,
                "exists": False,
            }

    def listdir(self, path: str = ".") -> list:
        """
        List directory entries at a path.

        Returns:
            List of file/directory names (basenames only).
        """
        path = self._resolve_path(path)
        names = set()

        for p in self._current_tree.keys():
            if p == path:
                continue
            if p.startswith(path):
                if path == "/":
                    remainder = p[1:]
                else:
                    remainder = p[len(path):]
                parts = remainder.split("/")
                if len(parts) >= 1 and parts[0]:
                    names.add(parts[0])

        return sorted(names)

    def mkdir(self, path: str) -> None:
        """
        Create a directory (stored as a marker in the tree).

        In MiniNAS, directories are implicit — they exist when files
        reference paths within them. This creates a .dir marker.
        """
        path = self._resolve_path(path)
        dir_marker = path.rstrip("/") + "/.minnas_dir"
        self._current_tree[dir_marker] = ""

    def rm(self, path: str) -> None:
        """Remove a file from the VFS tree."""
        path = self._resolve_path(path)
        if path in self._current_tree:
            del self._current_tree[path]
        else:
            raise FileNotFoundError_(f"File not found: {path}")

    def exists(self, path: str) -> bool:
        """Check if a path exists in the VFS."""
        path = self._resolve_path(path)
        return path in self._current_tree

    def is_dir(self, path: str) -> bool:
        """Check if a path is a directory marker."""
        path = self._resolve_path(path)
        return path.rstrip("/") + "/.minnas_dir" in self._current_tree

    def cat(self, path: str) -> bytes:
        """Read entire file contents and return as bytes."""
        path = self._resolve_path(path)
        sha = self._current_tree.get(path)
        if sha is None:
            raise FileNotFoundError_(f"File not found: {path}")
        return self._snapshots.load(sha)

    def write_file(self, path: str, content: bytes) -> str:
        """Write content to a file, returning its SHA."""
        path = self._resolve_path(path)
        sha = self._snapshots.store(content)
        self._current_tree[path] = sha
        return sha

    def commit(
        self, message: str, parent_sha: Optional[str],
        author: str = "anonymous"
    ) -> str:
        """
        Commit all current state as a new snapshot.

        Args:
            message: Commit message.
            parent_sha: SHA of the parent snapshot.
            author: Author name.

        Returns:
            SHA of the new snapshot.
        """
        # Close all open files to commit their changes
        fds = list(self._open_files.keys())
        for fd in fds:
            self.close(fd)

        snapshot_sha = self._snapshots.create_snapshot(
            tree=dict(self._current_tree),
            message=message,
            parent_sha=parent_sha,
            author=author,
        )
        return snapshot_sha

    def checkout_snapshot(self, sha: str) -> None:
        """
        Restore the VFS to a previous snapshot.

        Args:
            sha: SHA of the snapshot to checkout.
        """
        tree = self._snapshots.get_tree(sha)
        self._current_tree = dict(tree)

        # Close all open files after checkout
        for vf in list(self._open_files.values()):
            vf._closed = True
        self._open_files.clear()

    def get_tree(self) -> dict:
        """Get the current VFS tree (path -> blob_sha)."""
        return dict(self._current_tree)

    def get_status(self, parent_tree: Optional[dict] = None) -> dict:
        """
        Get status of the working tree vs a parent.

        Returns:
            Dict with keys: modified (list), added (list), deleted (list).
        """
        current = self._current_tree
        parent = parent_tree or {}

        modified = []
        added = []
        deleted = []

        all_paths = set(current.keys()) | set(parent.keys())

        for path in sorted(all_paths):
            if path.endswith("/.minnas_dir"):
                continue
            cur_sha = current.get(path)
            par_sha = parent.get(path)

            if cur_sha is None and par_sha is not None:
                deleted.append(path)
            elif cur_sha is not None and par_sha is None:
                added.append(path)
            elif cur_sha != par_sha:
                modified.append(path)

        return {
            "modified": modified,
            "added": added,
            "deleted": deleted,
            "current": current,
        }

    @property
    def cwd(self) -> str:
        return self._cwd

    def chdir(self, path: str) -> None:
        """Change the current working directory."""
        self._cwd = self._resolve_path(path)
