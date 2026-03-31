# MiniNAS - 产品需求文档 (PRD)

> Git 风格增量快照，轻量级文件存储系统

**版本：** v1.0.0
**作者：** OpenClaw Agent
**日期：** 2026-03-31
**状态：** 开发完成

---

## 1. 产品概述

### 1.1 背景与动机

在软件开发、测试、数据分析等场景中，经常需要：

- 对文件系统状态进行**版本快照**，便于回滚和比对
- **隔离存储实例**，在同一目录下管理多个独立项目
- 在测试环境中**模拟真实文件操作**，而不污染真实文件系统
- **增量存储**，避免重复数据占用过多空间

现有方案的问题：
- Git：面向代码版本管理，语义复杂，学习成本高
- 虚拟化（Docker/VM）：重量级，资源占用大
- 云存储 SDK：需要网络和账号，不适合本地测试

### 1.2 核心定位

MiniNAS = **Mini Network Attached Storage**，一个专为**测试/模拟/调试**场景设计的轻量级版本化文件系统。

**关键词：** 纯 Python、零外部依赖、Git 风格快照、命名空间隔离、可插拔后端

---

## 2. 功能需求

### 2.1 存储实例隔离（Namespace）

每个命名空间是独立的存储空间，数据完全隔离。

**用户故事：**
> 作为用户，我希望在同一目录下管理多个独立的项目存储实例，每个实例有独立的快照和分支，互不干扰。

**功能点：**
- 创建命名空间 `minnas ns create <name>`
- 列出所有命名空间 `minnas ns list`
- 切换命名空间 `minnas ns switch <name>`
- 删除命名空间 `minnas ns delete <name>`

**验收标准：**
- [x] 同一文件在不同命名空间中可共存
- [x] 删除命名空间不影响其他命名空间
- [x] 默认命名空间名为 `default`

---

### 2.2 Git 风格增量快照

使用内容寻址存储（CAS），只存储内容的 SHA-256 哈希，只在内容真正变化时才创建新 blob。

**用户故事：**
> 作为用户，我希望每次提交只记录文件的增量变化，不重复存储未修改的文件内容。

**功能点：**
- `minnas commit <message>` — 创建快照（提交）
- `minnas snapshot list` — 列出所有快照
- `minnas snapshot show <sha>` — 查看快照详情
- `minnas snapshot checkout <sha>` — 切换到指定快照
- `minnas log [-n N]` — 查看提交历史

**增量快照原理：**

```
Blob 存储（.minnas/objects/）：
  36/b2/...  →  "Hello World!" (SHA-256 = 36b2...)
  7d/8f/...  →  "Hello World!" (相同内容，复用 SHA-36b2...)
  ab/12/...  →  "Hello Again!" (SHA-256 = ab12...)

Snapshot 快照 = 文件路径 → Blob SHA 的映射表：
  snapshot_v1: {"readme.txt": "36b2...", "config.py": "ab12..."}
  snapshot_v2: {"readme.txt": "36b2...", "config.py": "xy90...", "data.json": "7d8f..."}
               ↑ 未修改的文件直接复用
```

**验收标准：**
- [x] 相同内容只存储一份（内容哈希去重）
- [x] 新增文件只存储新增 blob
- [x] 删除文件不删除 blob（直到 GC）
- [x] 支持快照历史查询

---

### 2.3 分支管理与 Reflog

支持多分支开发，自动记录操作历史。

**用户故事：**
> 作为用户，我希望创建实验性分支，实验完成后合并或丢弃，不污染主分支。

**功能点：**
- `minnas branch [-a]` — 列出所有分支
- `minnas branch create <name> [sha]` — 创建分支
- `minnas branch checkout <name>` — 切换分支
- `minnas branch delete <name>` — 删除分支
- Reflog 自动记录每次 HEAD 变动（commit/checkout/switch）

**验收标准：**
- [x] 分支间数据完全隔离
- [x] Reflog 记录每次操作的原因、时间、SHA 变化
- [x] 删除分支不影响其他分支
- [x] 支持 detached HEAD 状态

---

### 2.4 完整文件语义

提供标准文件操作接口，与 POSIX 语义一致。

**功能点：**

| 操作 | 语义 | 说明 |
|------|------|------|
| `open(path, mode)` | 打开文件 | mode: r/w/a/r+/w+/a+ |
| `read(fd, n)` | 读 n 字节 | 从当前位置读取 |
| `write(fd, data)` | 写数据 | 缓冲到内存 |
| `seek(fd, offset, whence)` | 移动指针 | SEEK_SET/SEEK_CUR/SEEK_END |
| `tell(fd)` | 当前位置 | 返回字节偏移 |
| `truncate(path, size)` | 截断文件 | 保留前 size 字节 |
| `append(fd, data)` | 追加写入 | 移动到末尾后写入 |
| `close(fd)` | 关闭文件 | 触发 CAS 提交 |

**验收标准：**
- [x] 所有模式（r/w/a/r+/w+/a+）正常工作
- [x] seek/tell 精确定位
- [x] truncate 正确截断
- [x] 关闭时自动将内容提交到 CAS

---

### 2.5 可插拔后端（Pluggable Backend）

存储后端可替换，适应不同场景。

**内置后端：**

| 后端 | 驱动类 | 适用场景 |
|------|--------|---------|
| 本地目录 | `LocalBackend` | 持久化存储、团队共享 |
| 内存 | `MemoryBackend` | 测试环境、快速原型 |
| 远程 HTTP | `RemoteBackend` | 网络存储、多端同步 |

**接口定义：**
```python
class Backend(ABC):
    def read(sha: str) -> bytes
    def write(sha: str, data: bytes) -> None
    def exists(sha: str) -> bool
    def delete(sha: str) -> None
    def list_all() -> list[str]
```

**验收标准：**
- [x] 后端可在 `minnas init --backend local|memory` 时指定
- [x] 切换后端不影响已存储数据（除非数据本身在后端）

---

### 2.6 多平台客户端（规划中）

| 平台 | 技术方案 | 状态 |
|------|---------|------|
| Linux | FUSE (fuse-python) | 规划中 |
| Windows | WinFsp | 规划中 |
| macOS | FUSE for macOS | 规划中 |
| Web | Browser-FS + IndexedDB | 规划中 |

---

## 3. 非功能需求

### 3.1 性能目标

- 单文件写入延迟：< 1ms（MemoryBackend）
- 单文件写入延迟：< 5ms（LocalBackend）
- 快照创建：O(变更文件数)
- 存储去重率：相同内容 ≥ 1次去重

### 3.2 可维护性

- **零外部依赖**：仅使用 Python 3 标准库
- **代码风格**：PEP 8，类型提示全覆盖
- **文档覆盖**：所有公共方法含 docstring
- **测试覆盖**：核心模块单元测试

### 3.3 限制与约束

- 不支持超大文件（单文件建议 < 100MB）
- 不支持并发写入（同一 Repo 不支持多进程同时写）
- RemoteBackend 需要网络连接

---

## 4. 使用场景

### 场景一：配置文件版本管理
```
minnas init --path ~/.config/minnas
# 编辑配置
minnas commit "update db config"
# 实验性修改
minnas branch create experiment
# 实验失败，回滚
minnas branch checkout main
```

### 场景二：测试数据快照
```
minnas init --path ./testdata --backend memory
# 准备测试数据
minnas commit "clean baseline"
# 运行测试，修改数据
minnas commit "test run 1"
minnas snapshot checkout baseline  # 恢复干净状态
```

### 场景三：模拟文件系统
```
# 不污染真实文件系统的情况下测试文件操作
minnas init --path /tmp/vfs
minnas fs write /data/input.csv "..."
minnas fs write /config.yaml "..."
# 测试代码读取这些文件
```

---

## 5. 竞品对比

| 特性 | MiniNAS | Git | Docker Volumes | mock.patch |
|------|---------|-----|----------------|------------|
| 增量快照 | ✅ | ✅ | ❌ | ❌ |
| 命名空间隔离 | ✅ | ❌ | ✅ | ❌ |
| 零依赖 | ✅ | ❌ | ❌ | ❌ |
| 完整文件语义 | ✅ | ❌ | ✅ | ❌ |
| 可插拔后端 | ✅ | ❌ | ❌ | ❌ |
| 轻量级 | ✅ | 中 | 重 | ✅ |

---

## 6. 版本规划

| 版本 | 目标 | 状态 |
|------|------|------|
| v1.0.0 | 核心功能完成（CAS + 快照 + 分支 + NS + VFS） | ✅ |
| v1.1.0 | FUSE 挂载支持 | 规划中 |
| v1.2.0 | RemoteBackend + 多端同步 | 规划中 |
| v2.0.0 | 并发写入支持 | 规划中 |
| v2.1.0 | 性能优化（大仓库支持） | 规划中 |
