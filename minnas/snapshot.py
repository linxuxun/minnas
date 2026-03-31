"""
snapshot.py — Git-style Content-Addressable Store (CAS) for MiniNAS.

Each object is stored as a zlib-compressed blob using the Git blob format:
    "blob {size}\0{data}"

Objects are stored under:
    .minnas/objects/<2-char-dir>/<38-char-sha256>

Snapshots are manifests (trees) mapping paths -> blob SHAs.
"""

import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from minnas.backend import Backend, LocalBackend, MemoryBackend


class MinNASError(Exception):
    """Base exception for MiniNAS errors."""
    pass


class ObjectNotFoundError(MinNASError):
    """Raised when a requested object does not exist in the store."""
    pass


class InvalidObjectError(MinNASError):
    """Raised when an object is malformed or corrupted."""
    pass


@dataclass
class Snapshot:
    """
    Represents a snapshot (tree manifest) at a point in time.

    Attributes:
        sha: The SHA-256 hash of this snapshot's manifest.
        tree: Dict mapping file paths (str) to blob SHAs (str).
        message: Commit/snapshot message.
        author: Author of this snapshot.
        timestamp: ISO-formatted datetime string.
        parent_sha: SHA of the parent snapshot (or None for initial).
    """
    sha: str
    tree: dict
    message: str
    author: str
    timestamp: str
    parent_sha: Optional[str] = None


def _hash_data(data: bytes) -> str:
    """Compute SHA-256 hex digest of data."""
    return hashlib.sha256(data).hexdigest()


def _encode_blob(data: bytes) -> bytes:
    """
    Encode data as a Git-style blob.
    Format: "blob {size}\0{data}"
    Compressed with zlib.
    """
    header = f"blob {len(data)}".encode("utf-8")
    raw = header + b"\0" + data
    return zlib.compress(raw)


def _decode_blob(blob_data: bytes) -> bytes:
    """
    Decode a blob from zlib-compressed Git blob format.
    Validates header before returning content.
    """
    try:
        raw = zlib.decompress(blob_data)
    except zlib.error as e:
        raise InvalidObjectError(f"Failed to decompress blob: {e}")

    if not raw.startswith(b"blob "):
        raise InvalidObjectError("Invalid blob header: does not start with 'blob '")

    null_pos = raw.find(b"\0", 5)
    if null_pos == -1:
        raise InvalidObjectError("Invalid blob: missing null separator")

    header_part = raw[:null_pos]
    try:
        size = int(header_part.split()[1])
    except (ValueError, IndexError) as e:
        raise InvalidObjectError(f"Invalid blob header: {e}")

    data = raw[null_pos + 1:]
    if len(data) != size:
        raise InvalidObjectError(
            f"Blob size mismatch: declared {size}, got {len(data)}"
        )
    return data


class SnapshotStore:
    """
    Git-style Content-Addressable Store for blobs and snapshots.

    Provides:
    - store(data) -> sha: Store data and return its SHA-256 hash.
    - load(sha) -> bytes: Load raw data by SHA.
    - exists(sha) -> bool: Check if an object exists.
    - delete(sha): Remove an object.
    - list_all() -> list[str]: List all stored SHAs.
    - create_snapshot(tree, message, parent_sha, author) -> snapshot_sha
    - get_snapshot(sha) -> Snapshot
    - get_tree(snapshot_sha) -> dict[str, str]
    - diff(sha1, sha2) -> list[dict]
    """

    SNAPSHOT_TYPE = "snapshot"
    BLOB_TYPE = "blob"

    def __init__(self, backend: Backend):
        self._backend = backend

    def store(self, data: bytes) -> str:
        """Store raw bytes and return their SHA-256 hash."""
        sha = _hash_data(data)
        if not self._backend.exists(sha):
            self._backend.write(sha, _encode_blob(data))
        return sha

    def load(self, sha: str) -> bytes:
        """Load and decode an object by SHA."""
        if not self._backend.exists(sha):
            raise ObjectNotFoundError(f"Object not found: {sha}")
        raw = self._backend.read(sha)
        return _decode_blob(raw)

    def exists(self, sha: str) -> bool:
        """Check if an object exists in the store."""
        return self._backend.exists(sha)

    def delete(self, sha: str) -> None:
        """Delete an object from the store."""
        if not self._backend.exists(sha):
            raise ObjectNotFoundError(f"Object not found: {sha}")
        self._backend.delete(sha)

    def list_all(self) -> list:
        """List all SHAs currently stored."""
        return self._backend.list_all()

    def store_blob(self, data: bytes) -> str:
        """Alias for store() — stores data as a blob and returns its SHA."""
        return self.store(data)

    def load_blob(self, sha: str) -> bytes:
        """Alias for load() — loads and decodes a blob by SHA."""
        return self.load(sha)

    def create_snapshot(
        self,
        tree: dict,
        message: str,
        parent_sha: Optional[str],
        author: str = "anonymous",
    ) -> str:
        """Create a new snapshot (tree manifest)."""
        snapshot_data = {
            "type": self.SNAPSHOT_TYPE,
            "tree": tree,
            "message": message,
            "author": author,
            "timestamp": datetime.utcnow().isoformat(),
            "parent_sha": parent_sha,
        }
        content = json.dumps(snapshot_data, sort_keys=True, ensure_ascii=False)
        sha = self.store(content.encode("utf-8"))
        return sha

    def get_snapshot(self, sha: str) -> Snapshot:
        """Retrieve a snapshot by SHA."""
        raw = self.load(sha)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise InvalidObjectError(f"Invalid snapshot JSON: {e}")

        if data.get("type") != self.SNAPSHOT_TYPE:
            raise InvalidObjectError(f"Object is not a snapshot: {sha}")

        return Snapshot(
            sha=sha,
            tree=data.get("tree", {}),
            message=data.get("message", ""),
            author=data.get("author", "anonymous"),
            timestamp=data.get("timestamp", ""),
            parent_sha=data.get("parent_sha"),
        )

    def get_tree(self, snapshot_sha: str) -> dict:
        """Get the tree (file path -> blob SHA mapping) of a snapshot."""
        snapshot = self.get_snapshot(snapshot_sha)
        return snapshot.tree

    def diff(self, sha1: str, sha2: str) -> list:
        """Compute the diff between two snapshots."""
        tree1 = self.get_tree(sha1)
        tree2 = self.get_tree(sha2)

        all_paths = set(tree1.keys()) | set(tree2.keys())
        changes = []

        for path in sorted(all_paths):
            old_sha = tree1.get(path)
            new_sha = tree2.get(path)

            if old_sha is None and new_sha is not None:
                changes.append({"action": "add", "path": path, "old_sha": None, "new_sha": new_sha})
            elif old_sha is not None and new_sha is None:
                changes.append({"action": "delete", "path": path, "old_sha": old_sha, "new_sha": None})
            elif old_sha != new_sha:
                changes.append({"action": "modify", "path": path, "old_sha": old_sha, "new_sha": new_sha})

        return changes

    def get_all_snapshots(self) -> list:
        """Retrieve all snapshots in the store."""
        snapshots = []
        for sha in self._backend.list_all():
            try:
                data_raw = self._backend.read(sha)
                data = json.loads(zlib.decompress(data_raw).decode("utf-8"))
                if data.get("type") == self.SNAPSHOT_TYPE:
                    snapshots.append(self.get_snapshot(sha))
            except Exception:
                pass
        return snapshots

    def find_reachable_shas(self, root_shas: list) -> set:
        """Find all SHAs reachable from the given root snapshots."""
        reachable = set()
        queue = list(root_shas)

        while queue:
            sha = queue.pop()
            if sha in reachable:
                continue
            if not self._backend.exists(sha):
                continue
            reachable.add(sha)

            try:
                data_raw = self._backend.read(sha)
                data = json.loads(zlib.decompress(data_raw).decode("utf-8"))
                if data.get("type") == self.SNAPSHOT_TYPE:
                    for blob_sha in data.get("tree", {}).values():
                        if blob_sha not in reachable:
                            queue.append(blob_sha)
                    parent = data.get("parent_sha")
                    if parent and parent not in reachable:
                        queue.append(parent)
            except Exception:
                pass

        return reachable
