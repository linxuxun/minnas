# MiniNAS - 架构设计文档

> 深入解析 MiniNAS 的核心设计与实现思路

**版本：** v1.0.0
**目标读者：** 开发者 / 架构师

---

## 1. 设计原则

1. **内容寻址优于路径寻址** — 数据按内容哈希存储，相同内容自动去重
2. **快照是不可变的事实** — 历史快照一旦创建不可修改，只有 HEAD 可移动
3. **命名空间天然隔离** — 每个命名空间是完全独立的空间，共享底层对象存储
4. **零外部依赖** — 仅使用 Python 3 标准库，任何环境开箱即用

---

## 2. 系统架构

```
┌─────────────────────────────────────────────────────┐
│                    用户层                             │
│         CLI (click/argparse) / API / FUSE           │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│                   Repo (仓库主入口)                    │
│     协调 SnapshotStore · BranchManager · VirtualFS    │
└────────────────────┬────────────────────────────────┘
                     │
    ┌────────────────┼────────────────┐
    │                │                │
    ▼                ▼                ▼
┌─────────┐   ┌──────────┐   ┌─────────────────┐
│VirtualFS│   │BranchMgr │   │SnapshotStore     │
│虚拟文件系统│   │分支管理   │   │内容寻址存储      │
└────┬────┘   └─────┬────┘   └────────┬──────────┘
     │              │                 │
     │              │         ┌────────┴────────┐
     │              │         │                 │
     ▼              ▼         ▼                 ▼
 ┌───────┐   ┌──────────┐ ┌────────┐    ┌────────────┐
 │Namespace│  │ Reflog   │ │  Blob  │    │  Backend   │
 │Store   │  │ 日志记录  │ │ CAS存储│    │ (可插拔)   │
 └────┬───┘  └────┬─────┘ └───┬────┘    └──────┬─────┘
      │            │           │                │
      └────────────┴───────────┴────────────────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
              ▼            ▼                ▼
         LocalDir/      Memory/          Remote/
         .minnas/objects   RAM           HTTP API
```

---

## 3. 核心数据结构

### 3.1 Blob（数据块）

**定义：** 任意的字节序列，通过 SHA-256 哈希寻址。

**存储格式：**
```
.git/objects/36/b2/36b28c6d... → "blob 12\0Hello World!"
                                ↑ header    ↑ content
```

**Python 实现：**
```python
def store(data: bytes) -> str:
    header = f"blob {len(data)}\0".encode()
    blob_data = header + data
    sha = hashlib.sha256(blob_data).hexdigest()
    backend.write(sha, blob_data)
    return sha
```

**关键设计：**
- Header 使用 `blob {size}\0` 前缀（与 Git 兼容）
- 先压缩（zlib）后存储，节省空间
- SHA-256 哈希作为内容指纹天然去重

---

### 3.2 Snapshot（快照）

**定义：** 某一时刻文件系统中所有文件的完整状态映射。

**结构：**
```python
@dataclass
class Snapshot:
    sha: str           # 快照的 SHA
    tree: dict         # {文件路径: blob_sha}
    message: str      # 提交信息
    author: str        # 作者
    timestamp: str     # ISO 格式时间
    parent_sha: str    # 父快照 SHA（第一个快照为 None）
```

**存储方式：** Snapshot 本身也被打包成 blob：
```
blob_sha = store(json.dumps({
    "tree": {"a.txt": "sha1", "b.txt": "sha2"},
    "message": "initial",
    "author": "alice",
    "timestamp": "2026-03-31T...",
    "parent": None
}))
```

**快照链：**
```
snapshot_v0 (sha_0)
  └── parent: None
  └── tree: {"/a.txt": "sha_a0"}

snapshot_v1 (sha_1)
  └── parent: sha_0
  └── tree: {"/a.txt": "sha_a1", "/b.txt": "sha_b1"}
       ↑ unchanged      ↑ new
       直接复用 sha_a0   新建

snapshot_v2 (sha_2)
  └── parent: sha_1
  └── tree: {"/a.txt": "sha_a1", "/c.txt": "sha_c2"}
       ↑ unchanged (复用 sha_1 的映射)
```

---

### 3.3 增量 diff 算法

比较两个快照之间的差异：

```python
def diff(sha1: str, sha2: str) -> list[Change]:
    tree1 = get_tree(sha1)  # path -> blob_sha
    tree2 = get_tree(sha2)

    changes = []
    all_paths = set(tree1) | set(tree2)

    for path in all_paths:
        s1 = tree1.get(path)
        s2 = tree2.get(path)

        if s1 is None and s2 is not None:
            changes.append({"path": path, "action": "add", "old": None, "new": s2})
        elif s1 is not None and s2 is None:
            changes.append({"path": path, "action": "delete", "old": s1, "new": None})
        elif s1 != s2:  # 内容变化
            changes.append({"path": path, "action": "modify", "old": s1, "new": s2})
        # s1 == s2: 无变化，不记录

    return changes
```

---

## 4. 模块设计

### 4.1 SnapshotStore（CAS 引擎）

**职责：** 管理所有 blob 的存储和检索。

**核心 API：**
```python
class SnapshotStore:
    def store(self, data: bytes) -> str
        """存储数据，返回 SHA-256 哈希"""

    def load(self, sha: str) -> bytes
        """通过 SHA 加载数据，NotFoundError 如不存在"""

    def create_snapshot(self, tree: dict, message: str, parent: str, author: str) -> str
        """创建新快照，返回快照 SHA"""

    def get_snapshot(self, sha: str) -> Snapshot
        """获取快照元数据"""

    def get_tree(self, sha: str) -> dict
        """获取快照的文件映射表"""

    def diff(self, sha1: str, sha2: str) -> list[Change]
        """比较两个快照的差异"""
```

**内部状态：**
```python
class SnapshotStore:
    _backend: Backend        # 可插拔存储后端
    _meta_dir: Path          # 元数据目录（快照索引）
```

---

### 4.2 VirtualFS（虚拟文件系统）

**职责：** 提供 POSIX 风格的文件操作，桥接上层 API 和底层 CAS。

**文件描述符管理：**
```python
class VirtualFS:
    _open_files: dict[int, VirtualFile]  # fd → VirtualFile
    _next_fd: int                        # 下一个可用 fd
    _current_tree: dict[str, str]        # 当前快照的完整文件映射
    _snapshots: SnapshotStore             # CAS 引用
```

**文件操作状态机：**
```
open(path, 'w')
    ↓
VirtualFile(path, 'w', snapshots=store)
    ↓
write() → 写入 _buffer (BytesIO)
    ↓
close()
    ↓
content = _buffer.getvalue()
blob_sha = snapshots.store(content)   ← 写入 CAS
_update_tree(path, blob_sha)           ← 更新当前树映射
```

**工作原理：**
- 写操作先写入内存缓冲区（BytesIO）
- 关闭时将缓冲区的完整内容提交到 CAS
- 读取时从 CAS 加载到缓冲区
- 支持任意位置 seek（seek 后修改，再 close）

---

### 4.3 BranchManager（分支管理）

**Ref 存储格式：**
```
.minnas/refs/heads/main     → "abc123...\n" (快照 SHA)
.minnas/refs/heads/feature  → "def456...\n"
.minnas/HEAD                → "ref: refs/heads/main"  (符号引用)
                               或 "abc123...\n"  (detached HEAD)
```

**Reflog 格式：**
```
.minnas/logs/refs/heads/main
  abc123... def456... commit "update config" alice 2026-03-31T10:00:00
  def456... ghi789... commit "add feature"  bob   2026-03-31T11:00:00
       ↑       ↑      ↑          ↑           ↑
    旧SHA   新SHA  操作类型   消息        作者
```

**操作原子性：**
- 所有分支操作先写 reflog → 再更新 ref
- Reflog 是追加写入，保证操作可审计

---

### 4.4 NamespaceStore（命名空间隔离）

**目录结构：**
```
.minnas/
├── objects/              ← 所有命名空间共享的 CAS 存储
│   ├── 36/b2/...
│   └── ab/12/...
├── refs/                 ← 所有命名空间共享的分支
│   └── heads/
├── namespaces/
│   ├── default/
│   │   ├── HEAD
│   │   ├── snapshots/   ← default 快照索引
│   │   └── current_tree  ← 当前工作树快照 SHA
│   ├── proj1/
│   │   └── ...
│   └── proj2/
│       └── ...
└── _current_ns           ← 当前命名空间名称
```

**设计要点：**
- 所有命名空间**共享同一个对象存储**（`objects/`）
- 每个命名空间只记录自己的快照索引和当前树指针
- 删除命名空间：只删除其命名空间目录，不删除共享对象（由 GC 处理孤立对象）
- 切换命名空间：更新 `_current_ns` 文件

---

### 4.5 Backend（可插拔后端）

```python
class Backend(ABC):
    @abstractmethod
    def read(self, sha: str) -> bytes: ...

    @abstractmethod
    def write(self, sha: str, data: bytes) -> None: ...

    @abstractmethod
    def exists(self, sha: str) -> bool: ...

    @abstractmethod
    def delete(self, sha: str) -> None: ...

    @abstractmethod
    def list_all(self) -> list[str]: ...
```

**LocalBackend 实现：**
```python
class LocalBackend(Backend):
    root: Path  # e.g. /path/to/repo/.minnas/objects

    def _path(self, sha: str) -> Path:
        return self.root / sha[:2] / sha[2:]

    def write(self, sha: str, data: bytes):
        path = self._path(sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        # 先写临时文件再 rename，保证原子性
        tmp = path.with_suffix('.tmp')
        tmp.write_bytes(zlib.compress(data))
        tmp.rename(path)
```

**MemoryBackend 实现：**
```python
class MemoryBackend(Backend):
    _store: dict[str, bytes]  # sha → compressed_data

    def write(self, sha: str, data: bytes):
        self._store[sha] = zlib.compress(data)
```

**RemoteBackend 实现（规划中）：**
```python
class RemoteBackend(Backend):
    base_url: str  # e.g. https://api.minnas.example.com
    token: str     # 认证 Token

    def read(self, sha: str) -> bytes:
        resp = requests.get(f"{self.base_url}/objects/{sha}")
        return zlib.decompress(resp.content)
```

---

## 5. 存储布局详解

### 5.1 完整目录结构（带数据）

```
~/projects/test/.minnas/
├── _current_ns                    → "default"
│
├── objects/                       ← 内容寻址存储
│   ├── 00/1a2b3c...              → blob 12\0Hello!
│   ├── 7f/8e9d0c...              → blob 45\0World...
│   └── ... (所有 blob，相同内容哈希相同路径)
│
├── refs/
│   └── heads/
│       ├── main                   → "abc123...\n"
│       └── feature               → "def456...\n"
│
├── HEAD                          → "ref: refs/heads/main"
│
├── logs/
│   └── refs/heads/main
│       ├── (reflog entries...)
│
└── namespaces/
    ├── default/
    │   ├── HEAD                  → "ref: refs/heads/main"
    │   ├── snapshots/
    │   │   └── abc123...         → Snapshot blob data
    │   └── current_tree          → "abc123...\n"
    │
    └── test_ns/
        ├── HEAD
        ├── snapshots/
        └── current_tree
```

---

## 6. CLI 命令路由

```
minnas <cmd>
    ├── init [--path] [--namespace] [--backend]
    ├── status [--repo]
    ├── commit <message> [--author] [--repo]
    ├── log [-n] [--repo]
    ├── diff <sha1> <sha2> [--repo]
    │
    ├── snapshot
    │   ├── list [--repo]
    │   ├── show <sha> [--repo]
    │   ├── checkout <sha> [--repo]
    │   └── delete <sha> [--repo]
    │
    ├── branch [name] [--create|--delete|--checkout] [--repo]
    │
    ├── namespace
    │   ├── list [--repo]
    │   ├── create <name> [--repo]
    │   ├── switch <name> [--repo]
    │   └── delete <name> [--repo]
    │
    ├── fs
    │   ├── ls [path] [--repo]
    │   ├── cat <path> [--repo]
    │   ├── write <path> <content> [--repo]
    │   └── rm <path> [--repo]
    │
    ├── gc [--repo]
    ├── stats [--repo]
    └── debug [--repo]
```

---

## 7. 关键设计决策

### Q1: 为什么用 SHA-256 而不是 UUID？

SHA-256 是内容指纹，**天然去重**——相同内容必然产生相同哈希，无需额外的去重逻辑。
UUID 则是随机生成的，无法判断内容是否重复。

### Q2: 为什么 Snapshot 也存成 blob？

快照本身也是数据（JSON 序列化的结构）。如果把快照元数据单独存储，就破坏了一致性——需要两套存储管理。

统一用 blob 存储后，所有数据（文件内容 + 快照元数据）都在同一个 CAS 中，GC 逻辑可以统一处理。

### Q3: 为什么共享 objects/ 而不隔离命名空间？

如果每个命名空间有独立的 objects/，则相同内容会重复存储，浪费空间。

共享 objects/ 后：
- 内容只存储一次
- GC 只需扫描一次
- 快照可以跨命名空间引用（未来功能）

### Q4: 为什么用追加写 Reflog？

Reflog 是审计日志，需要记录每次 HEAD 变动的完整历史。追加写入保证：
- 每次操作都有记录
- 即使崩溃也能恢复（至少记录了旧状态）
- 便于分析操作历史和调试

---

## 8. 扩展计划

### 8.1 FUSE 挂载（v1.1.0）

```python
# 伪代码示例
import fuse

class MiniNASFuse(fuse.Fuse):
    def __init__(self, repo_path, mount_point):
        self.repo = Repo(repo_path)
        self.mount = mount_point

    def read(self, path, size, offset):
        # 从 VirtualFS 读取
        fd = self.repo.fs.open(path, 'r')
        self.repo.fs.lseek(fd, offset)
        data = self.repo.fs.read(fd, size)
        self.repo.fs.close(fd)
        return data
```

### 8.2 并发写入（v2.0.0）

- 文件锁机制（fcntl.flock）
- 操作日志重放（Write-Ahead Log）
- 乐观并发控制（版本号）

### 8.3 GC 优化

当前 GC 策略：标记-清除（Mark-Sweep）
- 从所有分支的 HEAD 出发，递归标记可达的 blob
- 清除所有未标记的 blob

未来：引用计数 + GC 混合策略，减少全量扫描开销。
