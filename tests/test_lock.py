# -*- coding:utf-8 -*-

import unittest
import os
import sys
import time
import subprocess
import threading

# Add the parent directory to Python path to import yyds_lock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from yyds_lock import force_single, release_single, single_decorator, AlreadyLockedError
from yyds_lock.core import _resolve_lock_path


class TestYYDSLock(unittest.TestCase):

    def setUp(self):
        # Cleanup test locks in home directory if any exist
        self.test_lock_name = "yyds_test_instance.lock"
        self.lock_path = _resolve_lock_path(self.test_lock_name)
        if os.path.exists(self.lock_path):
            try:
                os.remove(self.lock_path)
            except OSError:
                pass

    def tearDown(self):
        # Release and cleanup test lock
        release_single(self.test_lock_name)
        if os.path.exists(self.lock_path):
            try:
                os.remove(self.lock_path)
            except OSError:
                pass

    def test_basic_acquire_and_release(self):
        # 1. Acquire first time (should succeed)
        try:
            force_single(self.test_lock_name, block=False)
        except SystemExit:
            self.fail("force_single exited unexpectedly on first acquire")
            
        # 2. Release lock
        release_single(self.test_lock_name)

    def test_reentrant_same_process(self):
        # Re-acquiring the same lock in the same process should be a safe no-op
        try:
            force_single(self.test_lock_name, block=False)
            force_single(self.test_lock_name, block=False)
        except SystemExit:
            self.fail("force_single exited on reentrant acquire in the same process")
        finally:
            release_single(self.test_lock_name)

    def test_decorator_success(self):
        calls = []

        @single_decorator(lock_name=self.test_lock_name, block=False)
        def my_function(x):
            calls.append(x)
            return x * 2

        res = my_function(10)
        self.assertEqual(res, 20)
        self.assertEqual(calls, [10])

    def test_nested_decorators_reentrancy(self):
        # Test that nested decorators do not release the lock prematurely for the caller
        runs = []

        @single_decorator(lock_name=self.test_lock_name, block=False)
        def inner():
            runs.append("inner")

        @single_decorator(lock_name=self.test_lock_name, block=False)
        def outer():
            runs.append("outer_start")
            inner()
            # After inner() exits, the lock should STILL be held by outer!
            from yyds_lock.core import _lock_file_handles, _resolve_lock_path
            lock_path = _resolve_lock_path(self.test_lock_name)
            self.assertIn(lock_path, _lock_file_handles)
            self.assertEqual(_lock_file_handles[lock_path]["ref_count"], 1)
            runs.append("outer_end")

        outer()
        self.assertEqual(runs, ["outer_start", "inner", "outer_end"])

        # After outer exits, the lock must be completely released
        from yyds_lock.core import _lock_file_handles, _resolve_lock_path
        lock_path = _resolve_lock_path(self.test_lock_name)
        self.assertNotIn(lock_path, _lock_file_handles)

    def test_subprocess_conflict_non_blocking(self):
        # Start a background process that holds the lock
        code_hold = (
            "import time, sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            f"force_single('{self.test_lock_name}', block=False)\n"
            "print('LOCKED', flush=True)\n"
            "time.sleep(2)\n"
        )
        
        proc_hold = subprocess.Popen(
            [sys.executable, "-c", code_hold],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for background process to output "LOCKED"
        output = proc_hold.stdout.readline().strip()
        self.assertEqual(output, "LOCKED")
        
        # Now try to acquire the same lock in a second process with block=False
        code_try = (
            "import sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            f"force_single('{self.test_lock_name}', block=False)\n"
            "print('ACQUIRED', flush=True)\n"
        )
        
        proc_try = subprocess.run(
            [sys.executable, "-c", code_try],
            capture_output=True,
            text=True
        )
        
        # The second process must exit with code 1 (since it is already locked)
        self.assertEqual(proc_try.returncode, 1)
        self.assertIn("[yyds-lock] 错误: 脚本进程已在运行中", proc_try.stderr)
        self.assertNotIn("ACQUIRED", proc_try.stdout)
        
        # Cleanup
        proc_hold.stdout.close()
        proc_hold.stderr.close()
        proc_hold.wait()

    def test_subprocess_blocking_wait(self):
        # Start a background process that holds the lock for 1.5 seconds
        code_hold = (
            "import time, sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            f"force_single('{self.test_lock_name}', block=False)\n"
            "print('LOCKED', flush=True)\n"
            "time.sleep(1.5)\n"
            "print('RELEASED', flush=True)\n"
        )
        
        proc_hold = subprocess.Popen(
            [sys.executable, "-c", code_hold],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for background process to output "LOCKED"
        output = proc_hold.stdout.readline().strip()
        self.assertEqual(output, "LOCKED")
        
        # Try to acquire with block=True in a second process. It should block and then acquire.
        code_try_block = (
            "import sys, os, time\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            "start = time.time()\n"
            f"force_single('{self.test_lock_name}', block=True)\n"
            "print('ACQUIRED_BLOCKED', round(time.time() - start, 1), flush=True)\n"
        )
        
        start_time = time.time()
        proc_try = subprocess.run(
            [sys.executable, "-c", code_try_block],
            capture_output=True,
            text=True
        )
        elapsed = time.time() - start_time
        
        # The second process should succeed (exit code 0) after about 1.5 seconds
        self.assertEqual(proc_try.returncode, 0)
        self.assertIn("ACQUIRED_BLOCKED", proc_try.stdout)
        # Should have waited at least 1.0 seconds
        self.assertGreaterEqual(elapsed, 1.0)
        
        # Cleanup
        proc_hold.stdout.close()
        proc_hold.stderr.close()
        proc_hold.wait()

    def test_concurrent_threads_lock(self):
        import threading
        errors = []
        successes = []
        
        def run_thread():
            try:
                # Both threads try to acquire the lock
                force_single(self.test_lock_name, block=False)
                successes.append(threading.get_ident())
                time.sleep(0.2)
                release_single(self.test_lock_name)
            except SystemExit as e:
                errors.append(SystemExit(e.code))
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=run_thread)
        t2 = threading.Thread(target=run_thread)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # With thread isolation:
        # Exactly one thread should succeed, and one should raise SystemExit(1)
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, 1)

    def test_raise_on_conflict(self):
        # 1. Start a subprocess that holds the lock
        code_hold = (
            "import time, sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            f"force_single('{self.test_lock_name}', block=False)\n"
            "print('LOCKED', flush=True)\n"
            "time.sleep(2)\n"
        )
        
        proc_hold = subprocess.Popen(
            [sys.executable, "-c", code_hold],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for background process to output "LOCKED"
        output = proc_hold.stdout.readline().strip()
        self.assertEqual(output, "LOCKED")
        
        # 2. Now try to acquire the same lock in this process with raise_on_conflict=True
        with self.assertRaises(AlreadyLockedError):
            force_single(self.test_lock_name, block=False, raise_on_conflict=True)
            
        # Cleanup
        proc_hold.stdout.close()
        proc_hold.stderr.close()
        proc_hold.wait()

    def test_decorator_raise_on_conflict(self):
        # 1. Start a subprocess that holds the lock
        code_hold = (
            "import time, sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            f"from yyds_lock import force_single\n"
            f"force_single('{self.test_lock_name}', block=False)\n"
            "print('LOCKED', flush=True)\n"
            "time.sleep(2)\n"
        )
        
        proc_hold = subprocess.Popen(
            [sys.executable, "-c", code_hold],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for background process to output "LOCKED"
        output = proc_hold.stdout.readline().strip()
        self.assertEqual(output, "LOCKED")
        
        @single_decorator(lock_name=self.test_lock_name, block=False, raise_on_conflict=True)
        def my_decorated_func():
            pass
            
        # 2. Try running decorated function - it should raise AlreadyLockedError
        with self.assertRaises(AlreadyLockedError):
            my_decorated_func()
            
        # Cleanup
        proc_hold.stdout.close()
        proc_hold.stderr.close()
        proc_hold.wait()

    def test_canonical_path_resolution(self):
        # Create a symlink to test canonical path resolution
        # Let's create two different paths that point to the same physical file
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            target_file = os.path.join(tmpdir, "real_file.lock")
            symlink_file = os.path.join(tmpdir, "symlink_file.lock")
            
            # Create target file first so symlink works
            with open(target_file, "w") as f:
                f.write("")
                
            os.symlink(target_file, symlink_file)
            
            # Now, acquiring target_file should be reentrant with acquiring symlink_file
            try:
                force_single(target_file, block=False)
                # Since they resolve to the same canonical path, the second acquisition
                # should just increment the reference count instead of attempting a new file lock
                force_single(symlink_file, block=False)
                
                # Check ref count
                from yyds_lock.core import _lock_file_handles, _resolve_lock_path
                canonical_path = _resolve_lock_path(target_file)
                self.assertEqual(_lock_file_handles[canonical_path]["ref_count"], 2)
                
            finally:
                release_single(symlink_file)
                release_single(target_file)

    def test_path_resolution_fallback(self):
        from unittest.mock import patch
        original_access = os.access
        def mock_access(path, mode):
            if ".yyds_lock" in path or "home" in path or "~" in path:
                return False
            return original_access(path, mode)
            
        with patch("os.access", side_effect=mock_access):
            resolved = _resolve_lock_path("test_fallback.lock")
            import tempfile
            temp_dir = tempfile.gettempdir()
            self.assertTrue(resolved.startswith(os.path.realpath(temp_dir)))

    def test_custom_base_dir(self):
        import tempfile
        import shutil
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_name = "custom.lock"
            force_single(lock_name, base_dir=tmpdir)
            expected_file = os.path.join(tmpdir, lock_name)
            self.assertTrue(os.path.exists(expected_file))
            
            errors = []
            def worker():
                try:
                    force_single(lock_name, block=False, raise_on_conflict=True, base_dir=tmpdir)
                except AlreadyLockedError as e:
                    errors.append(e)
            t = threading.Thread(target=worker)
            t.start()
            t.join()
            self.assertEqual(len(errors), 1)
            
            release_single(lock_name, base_dir=tmpdir)

    def test_dynamic_lock_name_decorator(self):
        import threading
        calls = []
        @single_decorator(lock_name=lambda x: f"dynamic_{x}.lock", block=False, raise_on_conflict=True)
        def my_func(x):
            calls.append(x)
            errors = []
            def worker():
                try:
                    force_single(f"dynamic_{x}.lock", block=False, raise_on_conflict=True)
                except AlreadyLockedError as e:
                    errors.append(e)
            t = threading.Thread(target=worker)
            t.start()
            t.join()
            self.assertEqual(len(errors), 1)

        my_func("hello")
        self.assertEqual(calls, ["hello"])

    def test_thread_exclusive_isolation_blocking(self):
        import threading
        lock_name = "thread_ex_block.lock"
        
        acquired_t1 = threading.Event()
        release_t1 = threading.Event()
        released_t1 = threading.Event()
        
        def t1_worker():
            force_single(lock_name, block=False)
            acquired_t1.set()
            release_t1.wait()
            release_single(lock_name)
            released_t1.set()
            
        t1 = threading.Thread(target=t1_worker)
        t1.start()
        
        acquired_t1.wait()
        
        acquired_t2 = threading.Event()
        t2_blocked = threading.Event()
        
        def t2_worker():
            t2_blocked.set()
            force_single(lock_name, block=True)
            acquired_t2.set()
            release_single(lock_name)
            
        t2 = threading.Thread(target=t2_worker)
        t2.start()
        
        t2_blocked.wait()
        time.sleep(0.5)  # Allow t2 to attempt acquisition and block
        
        self.assertFalse(acquired_t2.is_set())
        
        release_t1.set()
        released_t1.wait()
        
        acquired_t2.wait(timeout=2)
        self.assertTrue(acquired_t2.is_set())
        
        t1.join()
        t2.join()

    def test_fork_safety(self):
        if not hasattr(os, "register_at_fork"):
            self.skipTest("os.register_at_fork is not supported on this platform")
            
        import multiprocessing
        lock_name = "fork_safety_test.lock"
        
        force_single(lock_name, block=False)
        
        def child_process(conn):
            from yyds_lock.core import _lock_file_handles
            registry_empty = len(_lock_file_handles) == 0
            
            try:
                force_single(lock_name, block=False, raise_on_conflict=True)
                child_can_acquire = True
            except AlreadyLockedError:
                child_can_acquire = False
                
            conn.send((registry_empty, child_can_acquire))
            conn.close()
            
        parent_conn, child_conn = multiprocessing.Pipe()
        p = multiprocessing.Process(target=child_process, args=(child_conn,))
        p.start()
        
        registry_empty, child_can_acquire = parent_conn.recv()
        p.join()
        
        release_single(lock_name)
        
        self.assertTrue(registry_empty)
        self.assertFalse(child_can_acquire)


if __name__ == "__main__":
    unittest.main()
