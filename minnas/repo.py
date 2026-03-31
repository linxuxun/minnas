"""
repo.py — Repository initialization and management for MiniNAS.

The Repo class is the main entry point for most MiniNAS operations.
It coordinates the SnapshotStore, BranchManager, NamespaceStore, and VirtualFS.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from minnas.backend import Backend, LocalBackend, MemoryBackend, RemoteBackend
from minnas.snapshot import SnapshotStore, Snapshot, ObjectNotFoundError
from minnas.namespace import NamespaceStore
from minnas.branch import BranchManager
from minnas.fileops import VirtualFS


class RepoNotFoundError(Exception):
    """Raised when a repository is not found at the given path."""
    pass


class Repo:
    """
    The main MiniNAS repository class.

    Coordinates:
    - SnapshotStore: CAS operations
    - BranchManager: Branch and HEAD management
    - NamespaceStore: Namespace isolation
    - VirtualFS: File operations
    """

    def __init__(
        self,
        root: Path,
        namespace: str,
        backend: Backend,
        snapshots: SnapshotStore,
        branch_mgr: BranchManager,
        namespace_mgr: NamespaceStore,
        fs: VirtualFS,
    ):
        self.root = Path(root)
        self.namespace = namespace
        self.backend = backend
        self._snapshots = snapshots
        self._branch_mgr = branch_mgr
        self._namespace_mgr = namespace_mgr
        self._fs = fs

    @staticmethod
    def init(
        path: str = ".",
        namespace: str = "default",
        backend_type: str = "local",
        remote_url: str = "",
        remote_token: str = "",
    ) -> "Repo":
        """
        Initialize or open a MiniNAS repository.

        Args:
            path: Path to the repository root.
            namespace: Namespace name to use.
            backend_type: 'local', 'memory', or 'remote'.

        Returns:
            A new Repo instance.
        """
        import zlib, json as _json

        root = Path(path).resolve()
        root.mkdir(parents=True, exist_ok=True)

        minnas_dir = root / ".minnas"
        minnas_dir.mkdir(exist_ok=True)

        # Create namespace manager first
        ns_mgr = NamespaceStore(root)
        ns_mgr.ensure_namespace(namespace)
        ns_name = namespace

        ns_path = ns_mgr.get_current_path()
        ns_objects = ns_path / "objects"
        ns_objects.mkdir(parents=True, exist_ok=True)
        ns_refs = ns_path / "refs"
        ns_refs.mkdir(parents=True, exist_ok=True)
        ns_refs_heads = ns_refs / "heads"
        ns_refs_heads.mkdir(parents=True, exist_ok=True)
        ns_refs_snapshots = ns_refs / "snapshots"
        ns_refs_snapshots.mkdir(parents=True, exist_ok=True)

        # Create backend based on type
        if backend_type == "local":
            backend: Backend = LocalBackend(ns_objects)
        elif backend_type == "memory":
            backend = MemoryBackend()
        elif backend_type == "remote":
            if not remote_url:
                raise ValueError("remote_url required for remote backend")
            backend = RemoteBackend(remote_url, remote_token)
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")

        snapshots = SnapshotStore(backend)
        branch_mgr = BranchManager(minnas_dir)
        fs = VirtualFS(snapshots, {})

        return Repo(
            root=root,
            namespace=ns_name,
            backend=backend,
            snapshots=snapshots,
            branch_mgr=branch_mgr,
            namespace_mgr=ns_mgr,
            fs=fs,
        )

    @staticmethod
    def open(path: str = ".", namespace: Optional[str] = None) -> "Repo":
        """
        Open an existing MiniNAS repository.

        Args:
            path: Path to the repository root.
            namespace: Namespace to open (defaults to current).

        Returns:
            Repo instance.

        Raises:
            RepoNotFoundError: If no .minnas directory exists.
        """
        import zlib, json as _json

        root = Path(path).resolve()
        minnas_dir = root / ".minnas"

        if not minnas_dir.exists():
            raise RepoNotFoundError(f"Not a MiniNAS repository: {root}")

        ns_mgr = NamespaceStore(root)
        ns_name = namespace or ns_mgr.get_current()

        if not ns_mgr.namespace_exists(ns_name):
            ns_mgr.ensure_namespace(ns_name)
        ns_mgr.switch_namespace(ns_name)

        ns_path = ns_mgr.get_current_path()
        ns_objects = ns_path / "objects"
        ns_objects.mkdir(parents=True, exist_ok=True)

        ns_backend = LocalBackend(ns_objects)
        ns_snapshots = SnapshotStore(ns_backend)
        branch_mgr = BranchManager(minnas_dir)

        existing_sha = branch_mgr.get_current_sha()
        initial_tree = {}
        if existing_sha:
            try:
                initial_tree = ns_snapshots.get_tree(existing_sha)
            except Exception:
                pass

        fs = VirtualFS(ns_snapshots, initial_tree)

        return Repo(
            root=root,
            namespace=ns_name,
            backend=ns_backend,
            snapshots=ns_snapshots,
            branch_mgr=branch_mgr,
            namespace_mgr=ns_mgr,
            fs=fs,
        )

    def commit(self, message: str, author: str = "anonymous") -> str:
        """Commit the current state as a new snapshot."""
        parent_sha = self._branch_mgr.get_current_sha()
        sha = self._fs.commit(message, parent_sha, author)
        self._branch_mgr.update_head(sha, "commit", author)
        return sha

    def log(self, n: int = 10) -> list:
        """Get the commit log."""
        parent_sha = self._branch_mgr.get_current_sha()
        if not parent_sha:
            return []

        log_entries = []
        current_sha = parent_sha
        visited = set()

        while current_sha and len(log_entries) < n:
            if current_sha in visited:
                break
            visited.add(current_sha)
            try:
                snap = self._snapshots.get_snapshot(current_sha)
                log_entries.append((
                    snap.sha,
                    snap.message,
                    snap.author,
                    snap.timestamp,
                    snap.parent_sha,
                ))
                current_sha = snap.parent_sha
            except (ObjectNotFoundError, Exception):
                break

        return log_entries

    def status(self) -> dict:
        """Get the current repository status."""
        parent_sha = self._branch_mgr.get_current_sha()
        parent_tree = {}
        if parent_sha:
            try:
                parent_tree = self._snapshots.get_tree(parent_sha)
            except Exception:
                pass
        return self._fs.get_status(parent_tree)

    def diff(self, sha1: str, sha2: str) -> list:
        """Diff two snapshots."""
        return self._snapshots.diff(sha1, sha2)

    def snapshot(self, sha: Optional[str] = None) -> Snapshot:
        """Get a snapshot by SHA, or the current HEAD snapshot."""
        if sha is None:
            sha = self._branch_mgr.get_current_sha()
            if sha is None:
                raise ObjectNotFoundError("No current snapshot")
        return self._snapshots.get_snapshot(sha)

    def gc(self) -> int:
        """Garbage collect unreachable objects. Returns count of deleted objects."""
        parent_sha = self._branch_mgr.get_current_sha()
        root_shas = [parent_sha] if parent_sha else []
        all_shas = set(self._snapshots.list_all())
        reachable = self._snapshots.find_reachable_shas(root_shas)

        deleted = 0
        for sha in all_shas:
            if sha not in reachable:
                try:
                    self._snapshots.delete(sha)
                    deleted += 1
                except Exception:
                    pass
        return deleted

    def debug_stats(self) -> dict:
        """Get repository statistics."""
        import zlib, json as _json

        all_shas = self._snapshots.list_all()
        snapshot_shas = []
        blob_shas = []

        for sha in all_shas:
            try:
                data = self.backend.read(sha)
                decoded = zlib.decompress(data)
                obj = _json.loads(decoded)
                if obj.get("type") == "snapshot":
                    snapshot_shas.append(sha)
                else:
                    blob_shas.append(sha)
            except Exception:
                blob_shas.append(sha)

        parent_sha = self._branch_mgr.get_current_sha()
        current_tree = {}
        if parent_sha:
            try:
                current_tree = self._snapshots.get_tree(parent_sha)
            except Exception:
                pass

        return {
            "total_objects": len(all_shas),
            "snapshot_count": len(snapshot_shas),
            "blob_count": len(blob_shas),
            "current_tree_files": len([k for k in current_tree.keys() if not k.endswith("/.minnas_dir")]),
            "current_branch": self._branch_mgr.get_current_branch(),
            "current_sha": parent_sha,
            "namespace": self.namespace,
            "backend": self.backend.path(),
        }

    def debug_verify(self) -> dict:
        """Verify repository integrity."""
        errors = []
        all_shas = self._snapshots.list_all()

        for sha in all_shas:
            try:
                self._snapshots.load(sha)
            except Exception as e:
                errors.append(f"{sha}: {e}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "total_objects_checked": len(all_shas),
        }
