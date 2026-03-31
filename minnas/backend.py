"""
backend.py — Pluggable storage backends for MiniNAS.

Provides three backend implementations:
- LocalBackend: Stores objects in a local directory.
- MemoryBackend: Stores objects in RAM (for testing).
- RemoteBackend: Stores objects via HTTP API.
"""

import json
import urllib.request
import urllib.error
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class Backend(ABC):
    """
    Abstract base class for MiniNAS storage backends.

    All backends must implement read, write, exists, delete, and list_all.
    """

    @abstractmethod
    def read(self, sha: str) -> bytes:
        """Read raw (compressed) object data by SHA."""
        pass

    @abstractmethod
    def write(self, sha: str, data: bytes) -> None:
        """Write raw (compressed) object data by SHA."""
        pass

    @abstractmethod
    def exists(self, sha: str) -> bool:
        """Check if an object with the given SHA exists."""
        pass

    @abstractmethod
    def delete(self, sha: str) -> None:
        """Delete the object with the given SHA."""
        pass

    @abstractmethod
    def list_all(self) -> list:
        """List all SHA keys currently stored."""
        pass

    @abstractmethod
    def path(self) -> Optional[str]:
        """Return backend-specific path/identifier (for info display)."""
        pass


class LocalBackend(Backend):
    """
    Stores objects in a local directory tree.

    Objects are stored under:
        <root>/<2-char-dir>/<38-char-sha>

    The directory structure distributes files across 256 subdirectories
    based on the first two characters of the SHA.
    """

    def __init__(self, root: Path):
        """
        Initialize the local backend.

        Args:
            root: Root directory for object storage (e.g., .minnas/objects/).
        """
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def path(self) -> str:
        return str(self._root)

    def _object_path(self, sha: str) -> Path:
        """Get the filesystem path for a given SHA object."""
        return self._root / sha[:2] / sha[2:]

    def read(self, sha: str) -> bytes:
        """Read raw object data from disk."""
        obj_path = self._object_path(sha)
        if not obj_path.exists():
            raise FileNotFoundError(f"Object not found: {sha}")
        return obj_path.read_bytes()

    def write(self, sha: str, data: bytes) -> None:
        """Write raw object data to disk."""
        obj_path = self._object_path(sha)
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        obj_path.write_bytes(data)

    def exists(self, sha: str) -> bool:
        """Check if object exists in local storage."""
        return self._object_path(sha).exists()

    def delete(self, sha: str) -> None:
        """Delete object from local storage."""
        obj_path = self._object_path(sha)
        if obj_path.exists():
            obj_path.unlink()
        # Try to remove parent dir if empty
        try:
            obj_path.parent.rmdir()
        except OSError:
            pass

    def list_all(self) -> list:
        """List all SHA objects in local storage."""
        shas = []
        if not self._root.exists():
            return shas
        for d1 in self._root.iterdir():
            if d1.is_dir() and len(d1.name) == 2:
                for obj_file in d1.iterdir():
                    if obj_file.is_file():
                        sha = d1.name + obj_file.name
                        if len(sha) == 64:
                            shas.append(sha)
        return shas


class MemoryBackend(Backend):
    """
    Stores objects in RAM.

    Useful for testing and temporary in-memory repositories.
    Objects are NOT persisted to disk.
    """

    def __init__(self):
        self._store: dict = {}

    def path(self) -> str:
        return "<memory>"

    def read(self, sha: str) -> bytes:
        """Read object from memory store."""
        if sha not in self._store:
            raise FileNotFoundError(f"Object not found: {sha}")
        return self._store[sha]

    def write(self, sha: str, data: bytes) -> None:
        """Write object to memory store."""
        self._store[sha] = data

    def exists(self, sha: str) -> bool:
        """Check if object exists in memory."""
        return sha in self._store

    def delete(self, sha: str) -> None:
        """Delete object from memory."""
        if sha in self._store:
            del self._store[sha]

    def list_all(self) -> list:
        """List all SHA keys in memory."""
        return list(self._store.keys())


class RemoteBackend(Backend):
    """
    HTTP-based remote storage backend.

    Communicates with a remote MiniNAS server via HTTP REST API.

    Endpoints expected on the remote server:
    - GET  /objects/<sha>        — read object
    - PUT  /objects/<sha>        — write object
    - HEAD /objects/<sha>        — check existence
    - DELETE /objects/<sha>     — delete object
    - GET  /objects              — list all SHAs
    """

    def __init__(self, base_url: str, token: str = ""):
        """
        Initialize the remote backend.

        Args:
            base_url: Base URL of the remote MiniNAS server.
            token: Bearer token for authentication.
        """
        self._base_url = base_url.rstrip("/")
        self._token = token

    def _headers(self) -> dict:
        """Build request headers with optional auth."""
        headers = {"Content-Type": "application/octet-stream"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _url(self, sha: str) -> str:
        return f"{self._base_url}/objects/{sha}"

    def read(self, sha: str) -> bytes:
        """Read object from remote server."""
        req = urllib.request.Request(self._url(sha), headers=self._headers())
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(f"Object not found on remote: {sha}")
            raise

    def write(self, sha: str, data: bytes) -> None:
        """Write object to remote server."""
        req = urllib.request.Request(
            self._url(sha),
            data=data,
            headers=self._headers(),
            method="PUT",
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()

    def exists(self, sha: str) -> bool:
        """Check if object exists on remote server."""
        req = urllib.request.Request(self._url(sha), headers=self._headers(), method="HEAD")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise

    def delete(self, sha: str) -> None:
        """Delete object from remote server."""
        req = urllib.request.Request(self._url(sha), headers=self._headers(), method="DELETE")
        try:
            with urllib.request.urlopen(req) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(f"Object not found on remote: {sha}")
            raise

    def list_all(self) -> list:
        """List all SHA keys on remote server."""
        req = urllib.request.Request(
            f"{self._base_url}/objects",
            headers=self._headers(),
        )
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("shas", [])
        except urllib.error.HTTPError:
            return []

    def path(self) -> str:
        return self._base_url
