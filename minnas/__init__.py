"""
MiniNAS — Mini Network Attached Storage
A lightweight file storage system with Git-style incremental snapshots.
"""

__version__ = "0.1.0"
__author__ = "linxuxun"
__license__ = "MIT"

from minnas.repo import Repo
from minnas.snapshot import Snapshot, SnapshotStore
from minnas.backend import Backend, LocalBackend, MemoryBackend, RemoteBackend
from minnas.namespace import NamespaceStore
from minnas.branch import BranchManager
from minnas.fileops import VirtualFile, VirtualFS

__all__ = [
    "Repo",
    "Snapshot",
    "SnapshotStore",
    "Backend",
    "LocalBackend",
    "MemoryBackend",
    "RemoteBackend",
    "NamespaceStore",
    "BranchManager",
    "VirtualFile",
    "VirtualFS",
]
