"""
namespace.py — Namespace isolation layer for MiniNAS.

Each namespace is a completely isolated storage space with its own:
- .minnas/objects/     — CAS blob storage
- .minnas/refs/        — Branch refs
- .minnas/refs/snapshots/ — Snapshot manifests
- .minnas/namespaces/  — Other namespaces

Namespaces are stored as subdirectories under:
    .minnas/namespaces/<namespace_name>/
"""

import json
import shutil
from pathlib import Path
from typing import Optional

from minnas.snapshot import MinNASError, ObjectNotFoundError


class NamespaceExistsError(MinNASError):
    """Raised when attempting to create a namespace that already exists."""
    pass


class NamespaceNotFoundError(MinNASError):
    """Raised when a requested namespace does not exist."""
    pass


class NamespaceStore:
    """
    Manages isolated namespaces within a MiniNAS repository.

    Each namespace has its own storage directory, enabling complete
    isolation of files, branches, and snapshots.
    """

    def __init__(self, root: Path):
        """
        Initialize the namespace store.

        Args:
            root: Root directory of the repository (contains .minnas/).
        """
        self._root = Path(root)
        self._ns_dir = self._root / ".minnas" / "namespaces"
        self._ns_dir.mkdir(parents=True, exist_ok=True)
        self._current_file = self._root / ".minnas" / "current_namespace"
        self._init_default_namespace()

    def _init_default_namespace(self) -> None:
        """Create the 'default' namespace if no namespaces exist."""
        if not list(self._ns_dir.iterdir()):
            self._ns_dir.joinpath("default").mkdir(parents=True, exist_ok=True)
            self._ns_dir.joinpath("default", "objects").mkdir(exist_ok=True)
            self._ns_dir.joinpath("default", "refs").mkdir(parents=True, exist_ok=True)
            self._ns_dir.joinpath("default", "refs", "heads").mkdir(parents=True, exist_ok=True)
            self._ns_dir.joinpath("default", "refs", "snapshots").mkdir(exist_ok=True)
            self._set_current_ns("default")

    def _ns_path(self, name: str) -> Path:
        """Get the directory path for a namespace."""
        return self._ns_dir / name

    def _set_current_ns(self, name: str) -> None:
        """Set the current namespace name."""
        self._current_file.write_text(name, encoding="utf-8")

    def get_current(self) -> str:
        """Return the name of the currently active namespace."""
        if self._current_file.exists():
            return self._current_file.read_text(encoding="utf-8").strip()
        return "default"

    def get_current_path(self) -> Path:
        """Return the directory path of the current namespace."""
        return self._ns_path(self.get_current())

    def list_namespaces(self) -> list:
        """
        List all available namespace names.

        Returns:
            List of namespace name strings.
        """
        if not self._ns_dir.exists():
            return []
        return sorted([d.name for d in self._ns_dir.iterdir() if d.is_dir()])

    def create_namespace(self, name: str) -> None:
        """
        Create a new namespace.

        Args:
            name: Name for the new namespace.

        Raises:
            NamespaceExistsError: If namespace already exists.
        """
        ns_path = self._ns_path(name)
        if ns_path.exists():
            raise NamespaceExistsError(f"Namespace already exists: {name}")

        ns_path.mkdir(parents=True, exist_ok=True)
        (ns_path / "objects").mkdir(exist_ok=True)
        (ns_path / "refs").mkdir(parents=True, exist_ok=True)
        (ns_path / "refs" / "heads").mkdir(exist_ok=True)
        (ns_path / "refs" / "snapshots").mkdir(exist_ok=True)

    def switch_namespace(self, name: str) -> None:
        """
        Switch to a different namespace.

        Args:
            name: Name of the namespace to switch to.

        Raises:
            NamespaceNotFoundError: If namespace does not exist.
        """
        ns_path = self._ns_path(name)
        if not ns_path.exists():
            raise NamespaceNotFoundError(f"Namespace not found: {name}")
        self._set_current_ns(name)

    def delete_namespace(self, name: str) -> None:
        """
        Delete a namespace and all its contents.

        Args:
            name: Name of the namespace to delete.

        Raises:
            NamespaceNotFoundError: If namespace does not exist.
            ValueError: If attempting to delete the current namespace.
        """
        if name == self.get_current():
            raise ValueError("Cannot delete the currently active namespace")

        ns_path = self._ns_path(name)
        if not ns_path.exists():
            raise NamespaceNotFoundError(f"Namespace not found: {name}")

        shutil.rmtree(ns_path)

    def namespace_exists(self, name: str) -> bool:
        """Check if a namespace exists."""
        return self._ns_path(name).exists()

    def ensure_namespace(self, name: str) -> None:
        """Create namespace if it doesn't exist."""
        if not self.namespace_exists(name):
            self.create_namespace(name)
