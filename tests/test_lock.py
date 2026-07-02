# -*- coding:utf-8 -*-

import unittest
import os
import sys
import time
import subprocess

# Add the parent directory to Python path to import yyds_lock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from yyds_lock import force_single, release_single, single_decorator


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


if __name__ == "__main__":
    unittest.main()
