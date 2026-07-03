# -*- coding:utf-8 -*-

import unittest
import os
import sys
import time
import subprocess

# Add the parent directory to Python path to import yyds_lock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from yyds_lock import force_single, release_single, single_decorator, AlreadyLockedError


class TestYYDSLock(unittest.TestCase):

    def setUp(self):
        # Cleanup test locks in home directory if any exist
        self.test_lock_name = "yyds_test_instance.lock"
        self.lock_path = os.path.join(os.path.expanduser("~"), self.test_lock_name)
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
        
        def run_thread():
            try:
                # Both threads try to acquire the lock
                force_single(self.test_lock_name, block=False)
                time.sleep(0.2)
                release_single(self.test_lock_name)
            except Exception as e:
                errors.append(e)
            except SystemExit as e:
                errors.append(SystemExit(e.code))

        t1 = threading.Thread(target=run_thread)
        t2 = threading.Thread(target=run_thread)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        self.assertEqual(len(errors), 0, f"Expected no errors, but got: {errors}")

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


if __name__ == "__main__":
    unittest.main()
