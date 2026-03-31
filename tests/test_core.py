"""Unit tests for MiniNAS core."""
import unittest, tempfile, shutil
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from minnas.snapshot import SnapshotStore, ObjectNotFoundError
from minnas.namespace import NamespaceStore
from minnas.branch import BranchManager
from minnas.backend import LocalBackend, MemoryBackend
from minnas.repo import Repo


class TestCAS(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = SnapshotStore(LocalBackend(Path(self.tmp)))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_store_load(self):
        sha = self.s.store(b"hello")
        self.assertEqual(self.s.load(sha), b"hello")
        self.assertEqual(len(sha), 64)

    def test_dedup(self):
        a = self.s.store(b"x"); b = self.s.store(b"x")
        self.assertEqual(a, b)

    def test_missing(self):
        with self.assertRaises(ObjectNotFoundError):
            self.s.load("0"*64)

    def test_snapshot(self):
        sha = self.s.create_snapshot({"f.txt": "abc"}, "msg", None, "alice")
        snap = self.s.get_snapshot(sha)
        self.assertEqual(snap.message, "msg")
        self.assertEqual(snap.author, "alice")
        self.assertEqual(snap.tree, {"f.txt": "abc"})
        self.assertIsNone(snap.parent_sha)

    def test_diff_add(self):
        a = self.s.create_snapshot({}, "v1", None, "t")
        b = self.s.create_snapshot({"new.txt": "abc"}, "v2", a, "t")
        diff = {d["path"]: d["action"] for d in self.s.diff(a, b)}
        self.assertEqual(diff.get("new.txt"), "add")

    def test_diff_modify(self):
        a = self.s.create_snapshot({"f.txt": "v1"}, "v1", None, "t")
        b = self.s.create_snapshot({"f.txt": "v2"}, "v2", a, "t")
        diff = {d["path"]: d["action"] for d in self.s.diff(a, b)}
        self.assertEqual(diff.get("f.txt"), "modify")


class TestBackend(unittest.TestCase):
    def test_memory(self):
        b = MemoryBackend()
        b.write("k", b"v")
        self.assertEqual(b.read("k"), b"v")
        b.delete("k")
        self.assertFalse(b.exists("k"))


class TestNamespace(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ns = NamespaceStore(Path(self.tmp))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_create_list_delete(self):
        self.ns.create_namespace("proj1")
        self.ns.create_namespace("proj2")
        nss = list(self.ns.list_namespaces())
        self.assertIn("proj1", nss)
        self.assertIn("proj2", nss)
        self.ns.delete_namespace("proj1")
        self.assertNotIn("proj1", list(self.ns.list_namespaces()))

    def test_switch(self):
        self.ns.create_namespace("a")
        self.ns.create_namespace("b")
        self.ns.switch_namespace("b")
        self.assertIn("b", self.ns.get_current())


class TestBranch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.s = SnapshotStore(LocalBackend(Path(self.tmp)))
        self.bm = BranchManager(Path(self.tmp))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_create_list(self):
        sha = self.s.create_snapshot({}, "init", None, "t")
        self.bm.create_branch("devel", sha)
        names = [n for n, *_ in self.bm.list_branches()]
        self.assertIn("devel", names)

    def test_delete(self):
        sha = self.s.create_snapshot({}, "init", None, "t")
        self.bm.create_branch("td", sha)
        self.bm.delete_branch("td")
        names = [n for n, *_ in self.bm.list_branches()]
        self.assertNotIn("td", names)


class TestRepo(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.repo = Repo.init(str(Path(self.tmp)), backend_type="local")

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_init(self):
        self.assertTrue((Path(self.tmp) / ".minnas").exists())

    def test_gc(self):
        self.repo.gc()


if __name__ == "__main__":
    unittest.main(verbosity=2)
