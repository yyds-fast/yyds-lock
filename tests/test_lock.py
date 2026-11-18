# -*- coding:utf-8 -*-

import unittest
import os
import sys
import time
import subprocess
import threading

# Add the parent directory to Python path to import yyds_lock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from yyds_lock import (
    AlreadyLockedError,
    LockTimeoutError,
    SingleInstanceLock,
    force_single,
    force_single_async,
    release_single,
    single_decorator,
)
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
            text=True,
        )

        try:
            # Wait for background process to output "LOCKED"
            output = proc_hold.stdout.readline().strip()
            self.assertEqual(output, "LOCKED")

            # Try to acquire the same lock in a second non-blocking process.
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
                text=True,
                timeout=3,
            )

            self.assertEqual(proc_try.returncode, 1)
            # A non-UTF-8 Windows pipe may backslash-escape the Chinese text.
            self.assertIn("[yyds-lock]", proc_try.stderr)
            self.assertIn(f"PID: {proc_hold.pid}", proc_try.stderr)
            self.assertNotIn("ACQUIRED", proc_try.stdout)
        finally:
            if proc_hold.poll() is None:
                proc_hold.terminate()
            try:
                proc_hold.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc_hold.kill()
                proc_hold.wait(timeout=3)
            proc_hold.stdout.close()
            proc_hold.stderr.close()

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
            text=True,
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
            [sys.executable, "-c", code_try_block], capture_output=True, text=True
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
            text=True,
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
            text=True,
        )

        # Wait for background process to output "LOCKED"
        output = proc_hold.stdout.readline().strip()
        self.assertEqual(output, "LOCKED")

        @single_decorator(
            lock_name=self.test_lock_name, block=False, raise_on_conflict=True
        )
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

            try:
                os.symlink(target_file, symlink_file)
            except (OSError, NotImplementedError) as exc:
                self.skipTest("Symbolic links are unavailable: {}".format(exc))

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
        from yyds_lock import core

        with patch.object(core.os, "access", return_value=False):
            resolved = _resolve_lock_path("test_fallback.lock")

        expected_dir = os.path.realpath(core._temp_lock_dir())
        self.assertEqual(os.path.dirname(resolved), expected_dir)

    def test_private_directory_permissions_are_hardened(self):
        import stat
        import tempfile
        from yyds_lock.core import _prepare_private_directory

        if os.name == "nt" or not hasattr(os, "getuid"):
            self.skipTest("POSIX directory permissions are unavailable")

        with tempfile.TemporaryDirectory() as tmpdir:
            private_dir = os.path.join(tmpdir, "private")
            os.mkdir(private_dir, mode=0o700)
            os.chmod(private_dir, 0o777)

            _prepare_private_directory(private_dir)

            mode = stat.S_IMODE(os.stat(private_dir).st_mode)
            self.assertEqual(mode, 0o700)

    def test_custom_base_dir(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_name = "custom.lock"
            force_single(lock_name, base_dir=tmpdir)
            expected_file = os.path.join(tmpdir, lock_name)
            self.assertTrue(os.path.exists(expected_file))

            errors = []

            def worker():
                try:
                    force_single(
                        lock_name, block=False, raise_on_conflict=True, base_dir=tmpdir
                    )
                except AlreadyLockedError as e:
                    errors.append(e)

            t = threading.Thread(target=worker)
            t.start()
            t.join()
            self.assertEqual(len(errors), 1)

            release_single(lock_name, base_dir=tmpdir)

    def test_relative_base_dir_release_survives_working_directory_change(self):
        import tempfile
        from yyds_lock import core

        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "locks", "cwd.lock")
            try:
                os.chdir(tmpdir)
                os.mkdir("locks")
                force_single("cwd.lock", base_dir="locks", raise_on_conflict=True)
                self.assertIn(os.path.realpath(lock_path), core._lock_file_handles)

                os.mkdir("next")
                os.chdir("next")
                release_single("cwd.lock", base_dir="locks")

                self.assertNotIn(os.path.realpath(lock_path), core._lock_file_handles)
                self.assertFalse(core._thread_acquisitions)
            finally:
                os.chdir(original_cwd)
                release_single(lock_path)

    def test_dynamic_lock_name_decorator(self):
        import threading

        calls = []

        @single_decorator(
            lock_name=lambda x: f"dynamic_{x}.lock", block=False, raise_on_conflict=True
        )
        def my_func(x):
            calls.append(x)
            errors = []

            def worker():
                try:
                    force_single(
                        f"dynamic_{x}.lock", block=False, raise_on_conflict=True
                    )
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
        import multiprocessing

        if (
            not hasattr(os, "register_at_fork")
            or "fork" not in multiprocessing.get_all_start_methods()
        ):
            self.skipTest("fork start method is not supported on this platform")

        context = multiprocessing.get_context("fork")
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

        parent_conn, child_conn = context.Pipe()
        p = context.Process(target=child_process, args=(child_conn,))
        p.start()

        try:
            self.assertTrue(parent_conn.poll(3), "forked child did not respond")
            registry_empty, child_can_acquire = parent_conn.recv()
            p.join(timeout=3)
            self.assertFalse(p.is_alive(), "forked child did not exit")
        finally:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
            parent_conn.close()
            child_conn.close()
            release_single(lock_name)

        self.assertTrue(registry_empty)
        self.assertFalse(child_can_acquire)

    def test_fork_does_not_deadlock_on_inherited_registry_lock(self):
        if not hasattr(os, "fork"):
            self.skipTest("os.fork is not supported on this platform")

        code = r"""
import os
import signal
import threading
import time
from yyds_lock import core

locked = threading.Event()

def holder():
    core._registry_lock.acquire()
    locked.set()
    time.sleep(0.3)
    core._registry_lock.release()

thread = threading.Thread(target=holder)
thread.start()
locked.wait()
pid = os.fork()
if pid == 0:
    os._exit(0)

thread.join()
deadline = time.monotonic() + 2
while time.monotonic() < deadline:
    completed, _ = os.waitpid(pid, os.WNOHANG)
    if completed:
        raise SystemExit(0)
    time.sleep(0.02)

os.kill(pid, signal.SIGKILL)
os.waitpid(pid, 0)
raise SystemExit(2)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fork_closes_in_flight_handles_in_child(self):
        if not hasattr(os, "fork"):
            self.skipTest("os.fork is not supported on this platform")

        code = r"""
import os
import subprocess
import sys
import tempfile
import threading
import time
from yyds_lock import AlreadyLockedError, force_single, release_single
from yyds_lock import core

with tempfile.TemporaryDirectory() as directory:
    path = os.path.join(directory, "pending.lock")
    holder_code = (
        "import sys; from yyds_lock import force_single; "
        "force_single(sys.argv[1], raise_on_conflict=True); "
        "print('LOCKED', flush=True); sys.stdin.readline()"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if holder.stdout.readline().strip() != "LOCKED":
        raise SystemExit(10)

    acquired = threading.Event()
    def waiter():
        force_single(path, block=True, raise_on_conflict=True)
        acquired.set()
        release_single(path)

    thread = threading.Thread(target=waiter)
    thread.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with core._registry_guard():
            if core._pending_handles:
                break
        time.sleep(0.01)
    else:
        raise SystemExit(11)

    pid = os.fork()
    if pid == 0:
        clean = not core._pending_handles and not core._lock_file_handles
        try:
            force_single(path, block=False, raise_on_conflict=True)
            conflicted = False
        except AlreadyLockedError:
            conflicted = True
        os._exit(0 if clean and conflicted else 12)

    _, child_status = os.waitpid(pid, 0)
    holder.stdin.write("release\n")
    holder.stdin.flush()
    holder.wait(timeout=2)
    thread.join(timeout=2)
    child_exited_cleanly = (
        os.WIFEXITED(child_status) and os.WEXITSTATUS(child_status) == 0
    )
    clean_parent = (
        acquired.is_set()
        and not thread.is_alive()
        and not core._pending_handles
        and not core._lock_file_handles
    )
    raise SystemExit(0 if child_exited_cleanly and clean_parent else 13)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            capture_output=True,
            text=True,
            timeout=7,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_default_conflict_message_is_emitted_once(self):
        import io
        import tempfile
        from contextlib import redirect_stderr
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "single-message.lock")
            force_single(lock_path, raise_on_conflict=True)
            errors = []
            stderr = io.StringIO()

            def contender():
                try:
                    force_single(lock_path, block=False)
                except SystemExit as exc:
                    errors.append(exc.code)

            with patch.object(core.logger, "hasHandlers", return_value=False):
                with redirect_stderr(stderr):
                    thread = threading.Thread(target=contender)
                    thread.start()
                    thread.join()

            release_single(lock_path)

        self.assertEqual(errors, [1])
        self.assertEqual(
            stderr.getvalue().count("[yyds-lock] Failed to acquire lock"), 1
        )
        self.assertNotIn("\033", stderr.getvalue())

    def test_non_conflict_os_error_is_not_reported_as_locked(self):
        import errno
        import tempfile
        from unittest.mock import patch
        from yyds_lock import LockOperationError

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "io-error.lock")
            with patch(
                "yyds_lock.core._acquire_os_lock",
                side_effect=OSError(errno.EIO, "I/O failure"),
            ):
                with self.assertRaises(LockOperationError):
                    force_single(lock_path, block=False, raise_on_conflict=True)

    def test_failed_registry_commit_rolls_back_os_lock(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "commit-error.lock")

            with patch.object(
                core,
                "_record_acquisition",
                side_effect=RuntimeError("commit failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "commit failed"):
                    force_single(lock_path, raise_on_conflict=True)

            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._pending_handles)
            self.assertFalse(core._path_activity)
            self.assertFalse(core._thread_acquisitions)

            force_single(lock_path, raise_on_conflict=True)
            release_single(lock_path)

    def test_release_uses_path_recorded_at_acquisition(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "stable-release.lock")
            with patch.object(
                core, "_resolve_lock_path", return_value=lock_path
            ) as resolver:
                force_single("logical-name.lock", raise_on_conflict=True)
                release_single("logical-name.lock")

            self.assertEqual(resolver.call_count, 1)
            self.assertNotIn(lock_path, core._lock_file_handles)

    def test_async_decorator_holds_lock_across_await(self):
        import asyncio
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async.lock")

            async def scenario():
                entered = asyncio.Event()
                finish = asyncio.Event()

                @single_decorator(
                    lock_name=lock_path,
                    block=False,
                    raise_on_conflict=True,
                )
                async def protected():
                    entered.set()
                    await finish.wait()
                    return "done"

                task = asyncio.create_task(protected())
                await entered.wait()
                with self.assertRaises(AlreadyLockedError):
                    force_single(lock_path, block=False, raise_on_conflict=True)
                finish.set()
                self.assertEqual(await task, "done")

            asyncio.run(scenario())

    def test_async_acquire_waits_without_blocking_event_loop(self):
        import asyncio
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async-wait.lock")

            async def scenario():
                holder_entered = asyncio.Event()
                release_holder = asyncio.Event()

                async def holder():
                    force_single(lock_path, raise_on_conflict=True)
                    holder_entered.set()
                    try:
                        await release_holder.wait()
                    finally:
                        release_single(lock_path)

                waiter_entered = asyncio.Event()

                async def waiter():
                    await force_single_async(
                        lock_path,
                        timeout=1,
                        poll_interval=0.01,
                    )
                    try:
                        waiter_entered.set()
                    finally:
                        release_single(lock_path)

                holder_task = asyncio.create_task(holder())
                await holder_entered.wait()
                waiter_task = asyncio.create_task(waiter())
                await asyncio.sleep(0)
                self.assertFalse(waiter_task.done())
                self.assertFalse(waiter_entered.is_set())

                release_holder.set()
                await asyncio.wait_for(holder_task, timeout=1)
                await asyncio.wait_for(waiter_task, timeout=1)
                self.assertTrue(waiter_entered.is_set())

            asyncio.run(scenario())

    def test_async_acquire_timeout(self):
        import asyncio
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async-timeout.lock")

            async def scenario():
                holder_entered = asyncio.Event()
                release_holder = asyncio.Event()

                async def holder():
                    force_single(lock_path, raise_on_conflict=True)
                    holder_entered.set()
                    try:
                        await release_holder.wait()
                    finally:
                        release_single(lock_path)

                holder_task = asyncio.create_task(holder())
                await holder_entered.wait()
                try:
                    with self.assertRaises(LockTimeoutError):
                        await force_single_async(
                            lock_path,
                            timeout=0.02,
                            poll_interval=0.005,
                        )
                finally:
                    release_holder.set()
                    await asyncio.wait_for(holder_task, timeout=1)

            asyncio.run(scenario())

    def test_async_blocking_decorator_waits_safely(self):
        import asyncio
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async-decorator-wait.lock")

            async def scenario():
                holder_entered = asyncio.Event()
                release_holder = asyncio.Event()
                protected_entered = asyncio.Event()

                async def holder():
                    force_single(lock_path, raise_on_conflict=True)
                    holder_entered.set()
                    try:
                        await release_holder.wait()
                    finally:
                        release_single(lock_path)

                @single_decorator(
                    lock_name=lock_path,
                    block=True,
                    timeout=1,
                    poll_interval=0.01,
                )
                async def protected():
                    protected_entered.set()
                    return "done"

                holder_task = asyncio.create_task(holder())
                await holder_entered.wait()
                protected_task = asyncio.create_task(protected())
                await asyncio.sleep(0)
                self.assertFalse(protected_entered.is_set())

                release_holder.set()
                await asyncio.wait_for(holder_task, timeout=1)
                self.assertEqual(
                    await asyncio.wait_for(protected_task, timeout=1), "done"
                )

            asyncio.run(scenario())

    def test_async_wait_option_validation(self):
        import asyncio

        async def scenario():
            with self.assertRaises(ValueError):
                await force_single_async("invalid-timeout.lock", timeout=-1)
            with self.assertRaises(ValueError):
                await force_single_async("invalid-poll.lock", poll_interval=0)
            with self.assertRaises(TypeError):
                await force_single_async("invalid-bool.lock", timeout=True)

        asyncio.run(scenario())

    def test_async_generator_decorator_holds_lock_until_closed(self):
        import asyncio
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async-generator.lock")

            async def scenario():
                @single_decorator(
                    lock_name=lock_path,
                    raise_on_conflict=True,
                )
                async def protected_generator():
                    yield "first"
                    yield "second"

                generator = protected_generator()
                self.assertEqual(await generator.__anext__(), "first")

                async def contender():
                    with self.assertRaises(AlreadyLockedError):
                        force_single(
                            lock_path,
                            block=False,
                            raise_on_conflict=True,
                        )

                await asyncio.create_task(contender())
                await generator.aclose()

                force_single(lock_path, raise_on_conflict=True)
                release_single(lock_path)

            asyncio.run(scenario())

    def test_decorator_timeout_requires_async_blocking_mode(self):
        with self.assertRaises(ValueError):

            @single_decorator(timeout=1)
            def protected():
                pass

    def test_direct_async_blocking_call_is_rejected(self):
        import asyncio

        async def scenario():
            with self.assertRaises(ValueError):
                force_single("async-direct-block.lock", block=True)

        asyncio.run(scenario())

    def test_generator_decorator_holds_lock_until_closed(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "generator.lock")

            @single_decorator(
                lock_name=lock_path,
                block=False,
                raise_on_conflict=True,
            )
            def protected_generator():
                yield "first"
                yield "second"

            generator = protected_generator()
            self.assertEqual(next(generator), "first")

            errors = []

            def contender():
                try:
                    force_single(lock_path, block=False, raise_on_conflict=True)
                except AlreadyLockedError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join()
            self.assertEqual(len(errors), 1)

            generator.close()
            force_single(lock_path, block=False, raise_on_conflict=True)
            release_single(lock_path)

    def test_context_manager_releases_after_exception(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "context.lock")
            lock = SingleInstanceLock(lock_path, raise_on_conflict=True)

            with self.assertRaisesRegex(RuntimeError, "business failure"):
                with lock:
                    self.assertTrue(lock.acquired)
                    raise RuntimeError("business failure")

            self.assertFalse(lock.acquired)
            force_single(lock_path, raise_on_conflict=True)
            release_single(lock_path)

    def test_windows_block_mode_retries_nonblocking_lock(self):
        import errno
        import tempfile
        import types
        from unittest.mock import patch
        from yyds_lock import core

        calls = []
        fake_msvcrt = types.SimpleNamespace(
            LK_NBLCK=1,
            LK_UNLCK=2,
        )

        def locking(fd, mode, size):
            calls.append((mode, size))
            if mode == fake_msvcrt.LK_NBLCK and len(calls) < 3:
                raise OSError(errno.EACCES, "locked")

        fake_msvcrt.locking = locking

        with tempfile.TemporaryFile(mode="w+b") as handle:
            with patch.object(core.os, "name", "nt"):
                with patch.dict(sys.modules, {"msvcrt": fake_msvcrt}):
                    with patch.object(core.time, "sleep") as sleeper:
                        core._acquire_os_lock(handle, block=True)

            self.assertEqual(calls, [(1, 1), (1, 1), (1, 1)])
            self.assertEqual(sleeper.call_count, 2)
            handle.seek(0, os.SEEK_END)
            self.assertEqual(handle.tell(), 1)

    def test_custom_standard_logger_receives_conflict_log(self):
        import logging
        import tempfile
        from unittest.mock import MagicMock

        custom_logger = logging.getLogger("test_custom_logger")
        mock_handler = MagicMock()
        mock_handler.level = logging.INFO
        custom_logger.addHandler(mock_handler)
        custom_logger.setLevel(logging.INFO)

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "custom_logger.lock")
            force_single(lock_path, raise_on_conflict=True)
            errors = []

            def contender():
                try:
                    force_single(lock_path, block=False, logger=custom_logger)
                except SystemExit as exc:
                    errors.append(exc.code)

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join()

            release_single(lock_path)

        self.assertEqual(errors, [1])
        self.assertTrue(mock_handler.handle.called)
        record = mock_handler.handle.call_args[0][0]
        self.assertIn(
            "custom_logger.lock' is already held by execution owner",
            record.getMessage(),
        )
        self.assertIn("[yyds-lock] Failed to acquire lock", record.getMessage())
        self.assertEqual(record.levelno, logging.ERROR)

    def test_custom_duck_typed_logger_receives_conflict_log(self):
        import tempfile

        class CustomYydsLogger:
            def __init__(self):
                self.messages = []

            def error(self, msg, *args, **kwargs):
                self.messages.append(msg)

        custom_logger = CustomYydsLogger()

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "duck_logger.lock")
            force_single(lock_path, raise_on_conflict=True)
            errors = []

            def contender():
                try:
                    force_single(lock_path, block=False, logger=custom_logger)
                except SystemExit as exc:
                    errors.append(exc.code)

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join()

            release_single(lock_path)

        self.assertEqual(errors, [1])
        self.assertEqual(len(custom_logger.messages), 1)
        self.assertIn("[yyds-lock] Failed to acquire lock", custom_logger.messages[0])

    def test_custom_callable_logger_receives_conflict_log(self):
        import tempfile

        logs = []

        def callable_logger(msg):
            logs.append(msg)

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "callable_logger.lock")
            force_single(lock_path, raise_on_conflict=True)
            errors = []

            def contender():
                try:
                    force_single(lock_path, block=False, logger=callable_logger)
                except SystemExit as exc:
                    errors.append(exc.code)

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join()

            release_single(lock_path)

        self.assertEqual(errors, [1])
        self.assertEqual(len(logs), 1)
        self.assertIn("[yyds-lock] Failed to acquire lock", logs[0])

    def test_single_instance_lock_and_decorator_with_logger(self):
        import tempfile

        logs = []

        class MockLogger:
            def error(self, msg):
                logs.append(msg)

        logger_inst = MockLogger()

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "ctx_logger.lock")
            force_single(lock_path, raise_on_conflict=True)

            errors = []

            def contender_ctx():
                try:
                    with SingleInstanceLock(lock_path, block=False, logger=logger_inst):
                        pass
                except SystemExit as exc:
                    errors.append(exc.code)

            thread = threading.Thread(target=contender_ctx)
            thread.start()
            thread.join()

            self.assertEqual(errors, [1])
            self.assertEqual(len(logs), 1)
            self.assertIn("[yyds-lock] Failed to acquire lock", logs[0])

            @single_decorator(lock_name=lock_path, block=False, logger=logger_inst)
            def protected_func():
                return "ok"

            errors.clear()
            logs.clear()

            def contender_dec():
                try:
                    protected_func()
                except SystemExit as exc:
                    errors.append(exc.code)

            thread2 = threading.Thread(target=contender_dec)
            thread2.start()
            thread2.join()

            self.assertEqual(errors, [1])
            self.assertEqual(len(logs), 1)
            self.assertIn("[yyds-lock] Failed to acquire lock", logs[0])

            release_single(lock_path)

    def test_lock_metadata_windows_and_posix(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.NamedTemporaryFile(delete=False) as f:
            lock_path = f.name

        try:
            # Test Windows metadata write and read (offset 1)
            with patch.object(core.os, "name", "nt"):
                with open(lock_path, "w+b") as handle:
                    core._prepare_windows_lock_byte(handle)
                    core._write_lock_metadata(handle)

                with open(lock_path, "rb") as handle:
                    data = handle.read()
                    self.assertTrue(data.startswith(b"\x00"))
                    self.assertIn(str(os.getpid()).encode("utf-8"), data)

                read_pid = core._read_lock_pid(lock_path)
                self.assertEqual(read_pid, os.getpid())

            # Test POSIX metadata write and read (offset 0), including overwriting
            # stale PID when opened with "a+b" (O_APPEND) as in _open_lock_handle.
            with patch.object(core.os, "name", "posix"):
                with open(lock_path, "wb") as handle:
                    handle.write(b"99999999\n")

                with open(lock_path, "a+b", buffering=0) as handle:
                    core._write_lock_metadata(handle)

                with open(lock_path, "rb") as handle:
                    data = handle.read()
                    self.assertFalse(data.startswith(b"\x00"))
                    self.assertEqual(data, f"{os.getpid()}\n".encode("utf-8"))

                read_pid = core._read_lock_pid(lock_path)
                self.assertEqual(read_pid, os.getpid())
        finally:
            if os.path.exists(lock_path):
                os.remove(lock_path)

    def test_dead_thread_owner_is_reclaimed_without_deadlock(self):
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "dead_thread.lock")

            def worker():
                force_single(lock_path, raise_on_conflict=True)

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join()
            self.assertFalse(thread.is_alive())

            # Blocking acquire must reclaim the dead thread's lock rather than hang.
            force_single(lock_path, block=True, raise_on_conflict=True)
            release_single(lock_path)
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._thread_acquisitions)

    def test_dead_async_task_owner_is_reclaimed(self):
        import asyncio
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "dead_task.lock")

            async def scenario():
                entered = asyncio.Event()

                async def crashing_holder():
                    force_single(lock_path, raise_on_conflict=True)
                    entered.set()
                    await asyncio.sleep(5)

                task1 = asyncio.create_task(crashing_holder())
                await entered.wait()

                async def waiter():
                    await force_single_async(lock_path, timeout=1.0, poll_interval=0.5)
                    release_single(lock_path)
                    return "acquired"

                task2 = asyncio.create_task(waiter())
                await asyncio.sleep(0)
                task1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task1

                result = await asyncio.wait_for(task2, timeout=0.5)
                self.assertEqual(result, "acquired")

            asyncio.run(scenario())
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._async_waiters)
            self.assertFalse(core._thread_acquisitions)

    def test_sync_generator_decorator_releases_when_closed_from_another_thread(self):
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "gen_cross_thread.lock")

            @single_decorator(lock_name=lock_path, raise_on_conflict=True)
            def gen():
                yield "one"
                yield "two"

            holder = []

            def start_gen():
                g = gen()
                self.assertEqual(next(g), "one")
                holder.append(g)

            thread = threading.Thread(target=start_gen)
            thread.start()
            thread.join()

            holder[0].close()
            self.assertFalse(core._lock_file_handles)
            force_single(lock_path, raise_on_conflict=True)
            release_single(lock_path)

    def test_async_generator_decorator_supports_asend_athrow_and_cross_task_aclose(
        self,
    ):
        import asyncio
        import gc
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "agen_full.lock")

            @single_decorator(lock_name=lock_path, raise_on_conflict=True)
            async def agen():
                try:
                    val = yield "start"
                    yield f"sent:{val}"
                except ValueError as exc:
                    yield f"caught:{exc}"

            async def scenario():
                canonical_path = os.path.realpath(lock_path)
                g = agen()
                self.assertEqual(await g.__anext__(), "start")
                self.assertEqual(await g.asend("payload"), "sent:payload")
                self.assertEqual(await g.athrow(ValueError("oops")), "caught:oops")
                with self.assertRaises(StopAsyncIteration):
                    await g.__anext__()
                self.assertNotIn(canonical_path, core._lock_file_handles)

                # Cross-task aclose must release cleanly without RuntimeError.
                g2 = agen()
                self.assertEqual(await g2.__anext__(), "start")

                async def close_in_other_task():
                    await g2.aclose()

                await asyncio.create_task(close_in_other_task())
                self.assertNotIn(canonical_path, core._lock_file_handles)

                # Abandoned unclosed async generator releases on GC finalization.
                g3 = agen()
                self.assertEqual(await g3.__anext__(), "start")
                del g3
                gc.collect()

            asyncio.run(scenario())
            self.assertFalse(core._lock_file_handles)

    def test_open_base_exception_cleans_up_path_activity(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "interrupt.lock")

            with patch.object(
                core, "_open_lock_handle", side_effect=KeyboardInterrupt()
            ):
                with self.assertRaises(KeyboardInterrupt):
                    force_single(lock_path, raise_on_conflict=True)

            self.assertFalse(core._path_activity)
            self.assertFalse(core._pending_handles)
            force_single(lock_path, raise_on_conflict=True)
            release_single(lock_path)

    def test_relative_lock_path_respects_working_directory_change(self):
        import tempfile
        from yyds_lock import core

        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            dir1 = os.path.join(tmpdir, "dir1")
            dir2 = os.path.join(tmpdir, "dir2")
            os.makedirs(dir1)
            os.makedirs(dir2)
            try:
                os.chdir(dir1)
                force_single("sub/rel.lock", raise_on_conflict=True)
                os.chdir(dir2)
                force_single("sub/rel.lock", raise_on_conflict=True)

                path1 = os.path.realpath(os.path.join(dir1, "sub/rel.lock"))
                path2 = os.path.realpath(os.path.join(dir2, "sub/rel.lock"))
                self.assertTrue(os.path.exists(path1))
                self.assertTrue(os.path.exists(path2))
                self.assertIn(path1, core._lock_file_handles)
                self.assertIn(path2, core._lock_file_handles)

                os.chdir(dir1)
                release_single("sub/rel.lock")
                self.assertNotIn(path1, core._lock_file_handles)
                self.assertIn(path2, core._lock_file_handles)

                os.chdir(dir2)
                release_single("sub/rel.lock")
                self.assertFalse(core._lock_file_handles)
            finally:
                os.chdir(orig_cwd)

    def test_base_dir_with_relative_subpath_and_invalid_inputs(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            force_single("team/job.lock", base_dir=tmpdir, raise_on_conflict=True)
            expected = os.path.join(tmpdir, "team", "job.lock")
            self.assertTrue(os.path.exists(expected))
            release_single("team/job.lock", base_dir=tmpdir)

            with self.assertRaises(ValueError):
                force_single(os.path.abspath(expected), base_dir=tmpdir)

            for invalid_name in [".", "..", "dir/", "sub/.", "sub/.."]:
                with self.assertRaises(ValueError):
                    force_single(invalid_name)

    def test_callable_logger_formats_unlock_warning_arguments(self):
        import io
        import tempfile
        from contextlib import redirect_stderr
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "warn_fmt.lock")
            logs = []

            force_single(lock_path, raise_on_conflict=True)
            with patch.object(core, "_unlock_os_lock", side_effect=OSError("boom")):
                release_single(lock_path, logger=lambda msg: logs.append(msg))

            self.assertEqual(len(logs), 1)
            self.assertIn(os.path.realpath(lock_path), logs[0])
            self.assertIn("boom", logs[0])
            self.assertNotIn("%s", logs[0])

            # Invalid non-callable logger falls back to default logging/stderr.
            force_single(lock_path, raise_on_conflict=True)
            stderr = io.StringIO()
            errors = []

            def contender():
                try:
                    force_single(lock_path, block=False, logger=123)
                except SystemExit as exc:
                    errors.append(exc.code)

            with patch.object(core.logger, "hasHandlers", return_value=False):
                with redirect_stderr(stderr):
                    thread = threading.Thread(target=contender)
                    thread.start()
                    thread.join()

            release_single(lock_path)
            self.assertEqual(errors, [1])
            self.assertIn("[yyds-lock] Failed to acquire lock", stderr.getvalue())

    def test_single_instance_lock_async_context_manager(self):
        import asyncio
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "async_ctx.lock")

            async def scenario():
                entered = asyncio.Event()
                allow_exit = asyncio.Event()

                async def holder():
                    async with SingleInstanceLock(
                        lock_path, block=True, timeout=1.0
                    ) as lock:
                        self.assertTrue(lock.acquired)
                        entered.set()
                        await allow_exit.wait()
                    self.assertFalse(lock.acquired)

                t1 = asyncio.create_task(holder())
                await entered.wait()

                with self.assertRaises(AlreadyLockedError):
                    async with SingleInstanceLock(
                        lock_path, block=False, raise_on_conflict=True
                    ):
                        pass

                async def waiter():
                    async with SingleInstanceLock(
                        lock_path, block=True, timeout=1.0, poll_interval=0.5
                    ) as lock2:
                        self.assertTrue(lock2.acquired)
                    self.assertFalse(lock2.acquired)
                    return True

                t2 = asyncio.create_task(waiter())
                await asyncio.sleep(0)
                allow_exit.set()
                await t1
                self.assertTrue(await asyncio.wait_for(t2, timeout=0.5))

            asyncio.run(scenario())
            self.assertFalse(core._lock_file_handles)

    def test_async_wait_wakes_immediately_on_local_release_without_reresolving(self):
        import asyncio
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            resolve_calls = []
            orig_resolve = core._resolve_lock_path

            def spy_resolve(*args, **kwargs):
                resolve_calls.append((args, kwargs))
                return orig_resolve(*args, **kwargs)

            async def scenario():
                holder_ready = asyncio.Event()

                async def holder():
                    force_single("wake.lock", base_dir=tmpdir, raise_on_conflict=True)
                    holder_ready.set()
                    await asyncio.sleep(0.02)
                    release_single("wake.lock", base_dir=tmpdir)

                with patch.object(core, "_resolve_lock_path", side_effect=spy_resolve):
                    t1 = asyncio.create_task(holder())
                    await holder_ready.wait()
                    # Huge poll_interval proves wakeup uses the event, not polling.
                    t0 = time.monotonic()
                    await force_single_async(
                        "wake.lock",
                        base_dir=tmpdir,
                        timeout=0.5,
                        poll_interval=5.0,
                    )
                    elapsed = time.monotonic() - t0
                    release_single("wake.lock", base_dir=tmpdir)
                    await t1
                    self.assertLess(elapsed, 0.25)

            asyncio.run(scenario())
            self.assertEqual(len(resolve_calls), 2)

    def test_cleanup_all_wakes_released_events_and_handles(self):
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "cleanup_events.lock")
            canonical_path = os.path.realpath(lock_path)
            force_single(lock_path, raise_on_conflict=True)
            released_event = core._lock_file_handles[canonical_path]["released_event"]
            self.assertFalse(released_event.is_set())

            core._cleanup_all()
            self.assertTrue(released_event.is_set())
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._pending_handles)
            self.assertFalse(core._path_activity)
            self.assertFalse(core._thread_acquisitions)

    def test_windows_prepare_lock_byte_retries_inside_block_loop(self):
        import tempfile
        import types
        from unittest.mock import patch
        from yyds_lock import core

        attempts = {"tell": 0, "lock": 0}
        fake_msvcrt = types.SimpleNamespace(LK_NBLCK=1, LK_UNLCK=2)

        def locking(fd, mode, size):
            attempts["lock"] += 1

        fake_msvcrt.locking = locking

        class FakeHandle:
            def __init__(self, real_handle):
                self._real = real_handle
                self.writes = 0

            def fileno(self):
                return self._real.fileno()

            def seek(self, offset, whence=0):
                return self._real.seek(offset, whence)

            def tell(self):
                attempts["tell"] += 1
                return self._real.tell()

            def write(self, data):
                if self.writes == 0:
                    self.writes += 1
                    err = OSError(13, "locked")
                    err.winerror = 33
                    raise err
                return self._real.write(data)

            def flush(self):
                return self._real.flush()

        with tempfile.TemporaryFile(mode="w+b") as raw_handle:
            fake_handle = FakeHandle(raw_handle)
            with patch.object(core.os, "name", "nt"):
                with patch.dict(sys.modules, {"msvcrt": fake_msvcrt}):
                    with patch.object(core.time, "sleep") as sleeper:
                        core._acquire_os_lock(fake_handle, block=True)

            self.assertEqual(sleeper.call_count, 1)
            self.assertEqual(attempts["lock"], 1)
            raw_handle.seek(0, os.SEEK_END)
            self.assertEqual(raw_handle.tell(), 1)

    def test_cross_thread_async_loops_do_not_attach_foreign_task_callbacks(self):
        import asyncio
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "cross_loop.lock")
            cb_threads = []
            holder_acquired = threading.Event()
            release_holder = threading.Event()

            class TrackedTask(asyncio.Task):
                def add_done_callback(self, *args, **kwargs):
                    cb_threads.append(threading.current_thread())
                    return super().add_done_callback(*args, **kwargs)

            def run_holder(drop_without_release):
                loop = asyncio.new_event_loop()
                loop.set_task_factory(
                    lambda ev_loop, coro: TrackedTask(coro, loop=ev_loop)
                )

                async def holder():
                    force_single(lock_path, raise_on_conflict=True)
                    holder_acquired.set()
                    while not release_holder.is_set():
                        await asyncio.sleep(0.01)
                    if not drop_without_release:
                        release_single(lock_path)

                try:
                    loop.run_until_complete(holder())
                finally:
                    loop.close()

            # Case 1: normal cross-loop release wakes foreign waiter immediately.
            t1 = threading.Thread(target=run_holder, args=(False,))
            t1.start()
            self.assertTrue(holder_acquired.wait(timeout=2))

            async def waiter():
                await force_single_async(lock_path, timeout=1.0, poll_interval=5.0)
                release_single(lock_path)

            waiter_errors = []

            def run_waiter():
                try:
                    asyncio.run(waiter())
                except Exception as exc:
                    waiter_errors.append(exc)

            t2 = threading.Thread(target=run_waiter)
            t2.start()
            time.sleep(0.05)
            release_holder.set()
            t1.join(timeout=2)
            t2.join(timeout=2)
            self.assertFalse(waiter_errors)
            self.assertTrue(all(th is t1 for th in cb_threads))

            # Case 2: foreign-loop task exits without releasing; waiter still reclaims.
            cb_threads.clear()
            holder_acquired.clear()
            release_holder.clear()
            t3 = threading.Thread(target=run_holder, args=(True,))
            t3.start()
            self.assertTrue(holder_acquired.wait(timeout=2))

            t4 = threading.Thread(target=run_waiter)
            t4.start()
            time.sleep(0.05)
            release_holder.set()
            t3.join(timeout=2)
            t4.join(timeout=2)
            self.assertFalse(waiter_errors)
            self.assertTrue(all(th is t3 for th in cb_threads))
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._async_waiters)

    def test_base_dir_rejects_leading_separator_paths(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            for bad_name in ["/leading.lock", "\\leading.lock"]:
                with self.assertRaises(ValueError):
                    force_single(bad_name, base_dir=tmpdir, raise_on_conflict=True)

    def test_release_unheld_lock_does_not_create_lock_directory(self):
        import tempfile
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmpdir:
            missing_base = os.path.join(tmpdir, "should_not_exist")
            release_single("never_held.lock", base_dir=missing_base)
            self.assertFalse(os.path.exists(missing_base))

            fake_home = os.path.join(tmpdir, "fake_home")
            os.makedirs(fake_home)
            with patch.dict(os.environ, {"HOME": fake_home, "USERPROFILE": fake_home}):
                release_single("never_held.lock")
            self.assertFalse(os.path.exists(os.path.join(fake_home, ".yyds_lock")))

    def test_nested_chdir_context_manager_and_decorator_release_exact_lock(self):
        import tempfile
        from yyds_lock import core

        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            dir1 = os.path.join(tmpdir, "dir1")
            dir2 = os.path.join(tmpdir, "dir2")
            os.makedirs(dir1)
            os.makedirs(dir2)
            p1 = os.path.realpath(os.path.join(dir1, "sub/ctx.lock"))
            p2 = os.path.realpath(os.path.join(dir2, "sub/ctx.lock"))
            try:
                os.chdir(dir1)
                with SingleInstanceLock("sub/ctx.lock", raise_on_conflict=True) as l1:
                    os.chdir(dir2)
                    with SingleInstanceLock(
                        "sub/ctx.lock", raise_on_conflict=True
                    ) as l2:
                        l2.lock_name = "mutated_name.lock"
                        os.chdir(dir1)
                    self.assertIn(p1, core._lock_file_handles)
                    self.assertNotIn(p2, core._lock_file_handles)
                    self.assertTrue(l1.acquired)
                    self.assertFalse(l2.acquired)
                self.assertFalse(core._lock_file_handles)

                @single_decorator("sub/dec.lock", raise_on_conflict=True)
                def inner():
                    os.chdir(dir1)

                @single_decorator("sub/dec.lock", raise_on_conflict=True)
                def outer():
                    os.chdir(dir2)
                    inner()
                    d1 = os.path.realpath(os.path.join(dir1, "sub/dec.lock"))
                    d2 = os.path.realpath(os.path.join(dir2, "sub/dec.lock"))
                    self.assertIn(d1, core._lock_file_handles)
                    self.assertNotIn(d2, core._lock_file_handles)

                os.chdir(dir1)
                outer()
                self.assertFalse(core._lock_file_handles)
            finally:
                os.chdir(orig_cwd)

    def test_loguru_style_unlock_warning_and_no_duplicate_typeerror_log(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        class LoguruLike:
            def __init__(self):
                self.warnings = []
                self.error_calls = 0

            def warning(self, msg, *args, **kwargs):
                self.warnings.append(
                    msg.format(*args, **kwargs) if (args or kwargs) else msg
                )

            def error(self, msg):
                self.error_calls += 1
                raise TypeError("internal logger bug")

        log_inst = LoguruLike()
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "loguru.lock")
            canonical = os.path.realpath(lock_path)
            force_single(lock_path, raise_on_conflict=True)
            with patch.object(core, "_unlock_os_lock", side_effect=OSError("io fail")):
                release_single(lock_path, logger=log_inst)

            self.assertEqual(len(log_inst.warnings), 1)
            self.assertIn(canonical, log_inst.warnings[0])
            self.assertIn("io fail", log_inst.warnings[0])
            self.assertNotIn("%s", log_inst.warnings[0])

            force_single(lock_path, raise_on_conflict=True)

            def bad_contender():
                with self.assertRaisesRegex(TypeError, "internal logger bug"):
                    force_single(lock_path, block=False, logger=log_inst)

            th = threading.Thread(target=bad_contender)
            th.start()
            th.join()
            release_single(lock_path)
            self.assertEqual(log_inst.error_calls, 1)

    def test_in_flight_path_activity_reports_real_owner_not_unknown(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "inflight.lock")
            in_open = threading.Event()
            allow_open = threading.Event()
            orig_open = core._open_lock_handle

            def slow_open(*args, **kwargs):
                in_open.set()
                allow_open.wait(timeout=2)
                return orig_open(*args, **kwargs)

            def holder():
                force_single(lock_path, raise_on_conflict=True)
                release_single(lock_path)

            logs = []
            t = threading.Thread(target=holder)
            with patch.object(core, "_open_lock_handle", side_effect=slow_open):
                t.start()
                self.assertTrue(in_open.wait(timeout=2))
                try:
                    force_single(
                        lock_path, block=False, logger=lambda m: logs.append(m)
                    )
                except SystemExit:
                    pass
                allow_open.set()
                t.join(timeout=2)

            self.assertEqual(len(logs), 1)
            self.assertNotIn("[unknown]", logs[0])
            self.assertIn(str(t.ident), logs[0])

    def test_single_instance_lock_acquired_false_after_owner_death(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "dead_ctx.lock")
            lock = SingleInstanceLock(lock_path, raise_on_conflict=True)
            t = threading.Thread(target=lock.acquire)
            t.start()
            t.join()
            self.assertFalse(lock.acquired)
            force_single(lock_path, raise_on_conflict=True)
            release_single(lock_path)

    def test_private_directory_heals_missing_owner_write_permission(self):
        import stat
        import tempfile
        from yyds_lock.core import _prepare_private_directory

        if os.name == "nt" or not hasattr(os, "getuid"):
            self.skipTest("POSIX directory permissions are unavailable")

        with tempfile.TemporaryDirectory() as tmpdir:
            priv = os.path.join(tmpdir, "priv_ro")
            os.mkdir(priv, mode=0o500)
            try:
                _prepare_private_directory(priv)
                self.assertEqual(stat.S_IMODE(os.lstat(priv).st_mode), 0o700)
            finally:
                os.chmod(priv, 0o700)

    def test_unvisited_dead_owner_locks_are_proactively_reclaimed(self):
        import asyncio
        import gc
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:

            async def spawn_abandoned_tasks():
                for idx in range(30):

                    async def job(i=idx):
                        force_single(
                            os.path.join(tmpdir, f"abandoned_task_{i}.lock"),
                            raise_on_conflict=True,
                        )

                    await asyncio.create_task(job())
                await asyncio.sleep(0.01)

            asyncio.run(spawn_abandoned_tasks())
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._thread_acquisitions)

            for idx in range(10):
                p = os.path.join(tmpdir, f"abandoned_thread_{idx}.lock")
                t = threading.Thread(
                    target=lambda path=p: force_single(path, raise_on_conflict=True)
                )
                t.start()
                t.join()
                del t

            gc.collect()
            # Trigger housekeeping via a completely unrelated lock name.
            unrelated = os.path.join(tmpdir, "trigger_housekeep.lock")
            force_single(unrelated, raise_on_conflict=True)
            release_single(unrelated)
            self.assertFalse(core._lock_file_handles)
            self.assertFalse(core._thread_acquisitions)

    def test_readonly_shared_lock_file_fallback_on_posix(self):
        import tempfile

        if os.name == "nt":
            self.skipTest("POSIX flock read-only fallback test")

        with tempfile.TemporaryDirectory() as tmpdir:
            ro_lock = os.path.join(tmpdir, "shared_ro.lock")
            with open(ro_lock, "wb") as f:
                f.write(b"12345\n")
            os.chmod(ro_lock, 0o444)
            try:
                force_single(ro_lock, raise_on_conflict=True)
                release_single(ro_lock)
            finally:
                os.chmod(ro_lock, 0o600)

    def test_concurrent_opens_do_not_block_unrelated_locks_on_fork_gate(self):
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            slow_path = os.path.realpath(os.path.join(tmpdir, "slow.lock"))
            fast_path = os.path.realpath(os.path.join(tmpdir, "fast.lock"))
            in_slow_open = threading.Event()
            release_slow_open = threading.Event()
            orig_raw_open = core._open_raw_lock_file

            def hooked_raw_open(lock_path, allow_ro):
                if os.path.realpath(lock_path) == slow_path:
                    in_slow_open.set()
                    release_slow_open.wait(timeout=2)
                return orig_raw_open(lock_path, allow_ro)

            def slow_worker():
                force_single(slow_path, raise_on_conflict=True)
                release_single(slow_path)

            t = threading.Thread(target=slow_worker)
            with patch.object(core, "_open_raw_lock_file", side_effect=hooked_raw_open):
                t.start()
                self.assertTrue(in_slow_open.wait(timeout=2))
                t0 = time.monotonic()
                for _ in range(10):
                    force_single(fast_path, raise_on_conflict=True)
                    release_single(fast_path)
                fast_elapsed = time.monotonic() - t0
                release_slow_open.set()
                t.join(timeout=2)

            self.assertLess(fast_elapsed, 0.5)

    def test_cross_process_async_waiters_coalesce_behind_single_poll_leader(self):
        import asyncio
        import tempfile
        from unittest.mock import patch
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.join(tmpdir, "coalesce.lock")
            holder_code = (
                "import sys, time\n"
                "from yyds_lock import force_single\n"
                "force_single(sys.argv[1], raise_on_conflict=True)\n"
                "print('LOCKED', flush=True)\n"
                "time.sleep(0.22)\n"
            )
            proc = subprocess.Popen(
                [sys.executable, "-c", holder_code, lock_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(proc.stdout.readline().strip(), "LOCKED")
                open_calls = []
                orig_open = core._open_lock_handle

                def spy_open(*args, **kwargs):
                    open_calls.append(1)
                    return orig_open(*args, **kwargs)

                async def scenario():
                    async def contender():
                        await force_single_async(
                            lock_path, timeout=3.0, poll_interval=0.04
                        )
                        await asyncio.sleep(0.002)
                        release_single(lock_path)

                    with patch.object(core, "_open_lock_handle", side_effect=spy_open):
                        await asyncio.gather(*(contender() for _ in range(12)))

                asyncio.run(scenario())
                # 12 acquires + ~6 leader polls << 12 * 6 uncoalesced polls.
                self.assertLess(len(open_calls), 30)
            finally:
                proc.wait(timeout=3)
                proc.stdout.close()
                proc.stderr.close()

    def test_fifo_wakeup_order_for_async_and_sync_waiters(self):
        import asyncio
        import tempfile
        from yyds_lock import core

        with tempfile.TemporaryDirectory() as tmpdir:
            async_lock = os.path.join(tmpdir, "fifo_async.lock")
            async_order = []

            async def async_scenario():
                force_single(async_lock, raise_on_conflict=True)

                async def waiter(idx):
                    await force_single_async(async_lock, timeout=2.0, poll_interval=1.0)
                    async_order.append(idx)
                    release_single(async_lock)

                tasks = []
                for idx in range(5):
                    tasks.append(asyncio.create_task(waiter(idx)))
                    await asyncio.sleep(0.01)

                release_single(async_lock)
                await asyncio.gather(*tasks)

            asyncio.run(async_scenario())
            self.assertEqual(async_order, [0, 1, 2, 3, 4])

            sync_lock = os.path.join(tmpdir, "fifo_sync.lock")
            canonical_sync = os.path.realpath(sync_lock)
            sync_order = []
            force_single(sync_lock, raise_on_conflict=True)

            def sync_waiter(idx):
                force_single(sync_lock, block=True, raise_on_conflict=True)
                sync_order.append(idx)
                time.sleep(0.005)
                release_single(sync_lock)

            threads = []
            for idx in range(4):
                th = threading.Thread(target=sync_waiter, args=(idx,))
                th.start()
                threads.append(th)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    with core._registry_guard():
                        if len(core._sync_waiters.get(canonical_sync, ())) >= idx + 1:
                            break
                    time.sleep(0.002)

            release_single(sync_lock)
            for th in threads:
                th.join(timeout=2)
            self.assertEqual(sync_order, [0, 1, 2, 3])


if __name__ == "__main__":
    unittest.main()
