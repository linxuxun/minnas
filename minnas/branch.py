"""
branch.py — Branch management and reflog for MiniNAS.

Branches are stored as files under:
    .minnas/refs/heads/<branch_name>

Each file contains a single line: the SHA of the snapshot it points to.

HEAD can be:
- A symbolic ref (branch name) stored in .minnas/HEAD: "ref: refs/heads/main"
- A detached HEAD with a raw SHA in .minnas/HEAD

Reflog entries are stored in .minnas/reflogs/<branch_name>
Each entry: <sha> <old_sha> <new_sha> <action> <author> <timestamp>
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


class BranchError(Exception):
    """Base exception for branch-related errors."""
    pass


class BranchNotFoundError(BranchError):
    """Raised when a requested branch does not exist."""
    pass


class BranchExistsError(BranchError):
    """Raised when attempting to create a branch that already exists."""
    pass


class BranchManager:
    """
    Manages Git-style branches and the reflog.

    Provides:
    - create_branch(name, snapshot_sha)
    - delete_branch(name)
    - list_branches() -> [(name, sha, current?)]
    - checkout(branch_name) -> update HEAD
    - get_current_branch() -> str
    - get_reflog(branch=None) -> [(sha, old_sha, new_sha, action, author, time)]
    """

    REF_PREFIX = "refs/heads/"
    HEAD_FILE = "HEAD"
    REFLOG_DIR = "reflogs"

    def __init__(self, root: Path):
        """
        Initialize the branch manager.

        Args:
            root: Root directory of the repository (contains .minnas/).
        """
        self._root = Path(root)
        self._minnas = self._root / ".minnas"
        self._refs_dir = self._minnas / "refs" / "heads"
        self._head_file = self._minnas / self.HEAD_FILE
        self._reflog_dir = self._minnas / self.REFLOG_DIR

        self._refs_dir.mkdir(parents=True, exist_ok=True)
        self._reflog_dir.mkdir(parents=True, exist_ok=True)

        # Initialize HEAD if not exists
        if not self._head_file.exists():
            self._init_head()

    def _init_head(self) -> None:
        """Initialize HEAD with default branch 'main'."""
        self._head_file.write_text(f"ref: {self.REF_PREFIX}main\n", encoding="utf-8")
        # Create main branch pointing to nothing (initial repo)
        # Don't create empty main unless user explicitly does

    def _branch_path(self, name: str) -> Path:
        """Get the file path for a branch."""
        return self._refs_dir / name

    def _reflog_path(self, name: str) -> Path:
        """Get the reflog file path for a branch."""
        return self._reflog_dir / name

    def _read_head(self) -> tuple:
        """Read HEAD and return (is_detached, ref_or_sha)."""
        content = self._head_file.read_text(encoding="utf-8").strip()
        if content.startswith("ref: "):
            return (False, content[5:].strip())
        else:
            return (True, content.strip())

    def _write_head(self, is_detached: bool, ref_or_sha: str) -> None:
        """Write to HEAD file."""
        if is_detached:
            self._head_file.write_text(ref_or_sha + "\n", encoding="utf-8")
        else:
            self._head_file.write_text(f"ref: {ref_or_sha}\n", encoding="utf-8")

    def _add_reflog_entry(
        self, branch: str, old_sha: Optional[str], new_sha: str,
        action: str, author: str = "anonymous"
    ) -> None:
        """Append a reflog entry."""
        log_path = self._reflog_path(branch)
        timestamp = datetime.utcnow().isoformat()
        entry = f"{new_sha} {old_sha or '0000000000000000000000000000000000000000'} {new_sha} {action} {author} {timestamp}\n"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(entry)

    def create_branch(self, name: str, snapshot_sha: Optional[str] = None) -> None:
        """
        Create a new branch pointing to a snapshot (or nothing).

        Args:
            name: Branch name.
            snapshot_sha: SHA to point the branch at (optional).

        Raises:
            BranchExistsError: If branch already exists.
        """
        if self._branch_path(name).exists():
            raise BranchExistsError(f"Branch already exists: {name}")

        sha_str = snapshot_sha or ""
        self._branch_path(name).write_text(sha_str + "\n", encoding="utf-8")

    def delete_branch(self, name: str) -> None:
        """
        Delete a branch.

        Args:
            name: Branch name to delete.

        Raises:
            BranchNotFoundError: If branch does not exist.
            BranchError: If trying to delete current branch.
        """
        is_detached, current = self._read_head()
        if not is_detached and current == f"{self.REF_PREFIX}{name}":
            raise BranchError("Cannot delete the current branch")

        bp = self._branch_path(name)
        if not bp.exists():
            raise BranchNotFoundError(f"Branch not found: {name}")

        bp.unlink()

        # Remove reflog
        lp = self._reflog_path(name)
        if lp.exists():
            lp.unlink()

    def list_branches(self, all_namespaces: bool = False) -> list:
        """
        List all branches.

        Args:
            all_namespaces: If True, include remote/other branches.

        Returns:
            List of tuples: (name, sha, is_current)
        """
        branches = []
        is_detached, current_ref = self._read_head()

        if not self._refs_dir.exists():
            return branches

        for bp in sorted(self._refs_dir.iterdir()):
            if bp.is_file():
                name = bp.name
                sha = bp.read_text(encoding="utf-8").strip()
                ref_path = f"{self.REF_PREFIX}{name}"
                is_current = (not is_detached and current_ref == ref_path)
                branches.append((name, sha, is_current))

        return branches

    def get_current_branch(self) -> Optional[str]:
        """
        Get the current branch name.

        Returns:
            Branch name if on a branch, None if detached HEAD.
        """
        is_detached, ref_or_sha = self._read_head()
        if is_detached:
            return None
        prefix = self.REF_PREFIX
        if ref_or_sha.startswith(prefix):
            return ref_or_sha[len(prefix):]
        return None

    def get_current_sha(self) -> Optional[str]:
        """
        Get the SHA that HEAD currently points to.

        Returns:
            SHA string or None if no commits.
        """
        is_detached, ref_or_sha = self._read_head()
        if is_detached:
            return ref_or_sha if ref_or_sha else None
        else:
            bp = self._root / ref_or_sha.lstrip("/")
            if bp.exists():
                return bp.read_text(encoding="utf-8").strip()
            return None

    def checkout(self, name_or_sha: str) -> None:
        """
        Switch to a branch or a specific snapshot (detached HEAD).

        Args:
            name_or_sha: Branch name or snapshot SHA.

        Raises:
            BranchNotFoundError: If branch name is given but doesn't exist.
        """
        bp = self._branch_path(name_or_sha)
        if bp.exists():
            # It's a branch
            sha = bp.read_text(encoding="utf-8").strip()
            self._write_head(False, f"{self.REF_PREFIX}{name_or_sha}")
            self._add_reflog_entry(
                name_or_sha, sha, sha, "checkout", "anonymous"
            )
        else:
            # Try as SHA (detached HEAD)
            self._write_head(True, name_or_sha)

    def set_branch(self, name: str, sha: str) -> None:
        """
        Force-update a branch to point to a specific SHA.

        Args:
            name: Branch name.
            sha: New SHA to point to.

        Raises:
            BranchNotFoundError: If branch does not exist.
        """
        bp = self._branch_path(name)
        if not bp.exists():
            raise BranchNotFoundError(f"Branch not found: {name}")

        old_sha = bp.read_text(encoding="utf-8").strip()
        bp.write_text(sha + "\n", encoding="utf-8")
        self._add_reflog_entry(name, old_sha, sha, "update", "anonymous")

    def update_head(self, sha: str, message: str = "commit", author: str = "anonymous") -> None:
        """
        Update HEAD (current branch or detached) with a new SHA.

        Called automatically after a commit.

        Args:
            sha: New snapshot SHA.
            message: Commit message for reflog.
            author: Author for reflog.
        """
        is_detached, ref_or_sha = self._read_head()
        if is_detached:
            self._write_head(True, sha)
        else:
            bp = self._root / ref_or_sha.lstrip("/")
            old_sha = ""
            if bp.exists():
                old_sha = bp.read_text(encoding="utf-8").strip()
            bp.write_text(sha + "\n", encoding="utf-8")
            # Extract branch name from ref
            prefix = self.REF_PREFIX
            if ref_or_sha.startswith(prefix):
                branch = ref_or_sha[len(prefix):]
                self._add_reflog_entry(branch, old_sha, sha, message, author)

    def get_reflog(self, branch: Optional[str] = None) -> list:
        """
        Get reflog entries.

        Args:
            branch: Branch name. If None, uses current branch.

        Returns:
            List of tuples: (sha, old_sha, new_sha, action, author, time)
        """
        if branch is None:
            branch = self.get_current_branch()
            if branch is None:
                return []

        log_path = self._reflog_path(branch)
        if not log_path.exists():
            return []

        entries = []
        for line in log_path.read_text(encoding="utf-8").strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split(" ", 5)
            if len(parts) >= 6:
                entries.append(tuple(parts[:6]))
        return list(reversed(entries))

    def branch_sha(self, name: str) -> Optional[str]:
        """Get the SHA a branch points to."""
        bp = self._branch_path(name)
        if bp.exists():
            return bp.read_text(encoding="utf-8").strip() or None
        return None
