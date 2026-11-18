# -*- coding:utf-8 -*-

import atexit
import asyncio
from collections import deque
import errno
import getpass
import hashlib
import inspect
import logging
import math
import os
import stat
import sys
import tempfile
import threading
import time
import weakref
from functools import wraps
from typing import Any, Optional, Union


_default_logger = logging.getLogger("yyds_lock")
logger = _default_logger


class YYDSLockError(Exception):
    """Base exception for yyds-lock."""


class AlreadyLockedError(YYDSLockError):
    """Raised when a lock cannot be acquired because it is already held."""


class LockTimeoutError(AlreadyLockedError):
    """Raised when an asynchronous lock wait exceeds its timeout."""


class LockOperationError(YYDSLockError):
    """Raised when a lock file or operating-system lock operation fails."""


class _ForkReadWriteGate:
    """Shared-read / exclusive-write gate coordinating open()+registry vs fork().

    Normal lock operations take shared read access so slow filesystem opens on
    one path never block other threads operating on unrelated locks; only
    ``os.fork()`` takes exclusive write access to snapshot a quiescent state.
    """

    __slots__ = (
        "_lock",
        "_cond",
        "_readers",
        "_writer_thread",
        "_writer_depth",
        "_writers_waiting",
    )

    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._readers = {}
        self._writer_thread = None
        self._writer_depth = 0
        self._writers_waiting = 0

    def __enter__(self):
        ident = threading.get_ident()
        lock = self._lock
        lock.acquire()
        try:
            if self._writer_thread is None and self._writers_waiting == 0:
                self._readers[ident] = self._readers.get(ident, 0) + 1
                return self
            depth = self._readers.get(ident, 0)
            if depth > 0 or self._writer_thread == ident:
                self._readers[ident] = depth + 1
                return self
            while self._writer_thread is not None or self._writers_waiting > 0:
                self._cond.wait()
            self._readers[ident] = 1
            return self
        finally:
            lock.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        ident = threading.get_ident()
        lock = self._lock
        lock.acquire()
        try:
            new_depth = self._readers[ident] - 1
            if new_depth > 0:
                self._readers[ident] = new_depth
            else:
                del self._readers[ident]
                if not self._readers and self._writers_waiting > 0:
                    self._cond.notify_all()
        finally:
            lock.release()

    def read_guard(self):
        return self

    def current_thread_active(self):
        ident = threading.get_ident()
        with self._lock:
            return ident in self._readers or self._writer_thread == ident

    def write_acquire(self):
        ident = threading.get_ident()
        with self._cond:
            if self._writer_thread == ident:
                self._writer_depth += 1
                return
            self._writers_waiting += 1
            try:
                while self._writer_thread is not None or (
                    len(self._readers) > (1 if ident in self._readers else 0)
                ):
                    self._cond.wait()
                self._writer_thread = ident
                self._writer_depth = 1
            finally:
                self._writers_waiting -= 1

    def write_release(self):
        with self._cond:
            self._writer_depth -= 1
            if self._writer_depth == 0:
                self._writer_thread = None
                self._cond.notify_all()


class _OwnerKey:
    """Weakly-referenced execution owner wrapper so dead owners do not leak."""

    __slots__ = ("_ref", "_strong", "_hash")

    def __init__(self, owner, on_collect=None):
        self._hash = hash(id(owner))
        try:
            self._ref = (
                weakref.ref(owner, on_collect)
                if on_collect is not None
                else weakref.ref(owner)
            )
            self._strong = None
        except TypeError:
            self._ref = None
            self._strong = owner

    def resolve(self):
        return self._strong if self._ref is None else self._ref()

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, _OwnerKey) or self._hash != other._hash:
            return False
        left = self.resolve()
        right = other.resolve()
        return left is not None and left is right


# Every access to the registries below goes through _registry_guard.  The
# outer fork gate lets the pre-fork callback wait for registry mutations to
# finish without ever taking an inherited threading lock in the child.
_fork_gate = _ForkReadWriteGate()
_registry_lock = threading.Lock()

# Canonical path -> acquired lock information.
_lock_file_handles = {}

# Handles opened by this process but not currently stored in
# _lock_file_handles.  This includes handles being acquired or released, so a
# child forked at any point can close every inherited descriptor safely.
_pending_handles = set()

# Canonical path -> (Event, owner_identifier).  A path is present while a local
# thread is acquiring or releasing it.
_path_activity = {}

# Canonical path -> FIFO deque of threading.Event for blocked sync threads.
_sync_waiters = {}

# Canonical path -> FIFO deque of (event_loop, asyncio.Event) waiting for local
# ownership/activity or the cross-process poll leader to finish.
_async_waiters = {}

# Canonical path -> active leader poll_interval for single-flight cross-process
# async polling.
_async_poll_leaders = {}

# _OwnerKey -> logical lock key -> acquisition paths.
_thread_acquisitions = {}

# Lock-free queues populated by weakref / task-done callbacks so dead owners
# have their FDs and bookkeeping reclaimed even if their lock path is never
# requested again.
_stale_lock_paths = deque()
_dead_owner_keys = deque()
_acquire_op_counter = 0

# Cached canonical directory info for default bare lock names:
# (raw_target_dir, st_dev, st_ino) -> canonical_dir_path
_default_dir_cache = {}

# Live SingleInstanceLock instances so fork child cleanup can reset them
# atomically before any child thread can race on them.
_live_lock_instances = weakref.WeakSet()


class _RegistryGuard:
    __slots__ = ()

    def __enter__(self):
        _fork_gate.__enter__()
        try:
            _registry_lock.acquire()
        except BaseException:
            _fork_gate.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            _registry_lock.release()
        finally:
            _fork_gate.__exit__(exc_type, exc_val, exc_tb)


_REGISTRY_GUARD = _RegistryGuard()


def _registry_guard():
    return _REGISTRY_GUARD


def _coerce_path(value, parameter):
    try:
        value = os.fspath(value)
    except TypeError as exc:
        raise TypeError("{} must be a string or os.PathLike".format(parameter)) from exc

    if isinstance(value, bytes):
        raise TypeError("{} must resolve to text, not bytes".format(parameter))
    if not value:
        raise ValueError("{} cannot be empty".format(parameter))
    if parameter == "lock_name":
        if (
            value in {".", ".."}
            or value.endswith(("/", "\\"))
            or os.path.basename(value) in {".", ".."}
        ):
            raise ValueError("lock_name must refer to a file, not a directory")
    return value


def _is_bare_lock_name(lock_name):
    drive, _ = os.path.splitdrive(lock_name)
    return (
        not drive
        and not os.path.isabs(lock_name)
        and "/" not in lock_name
        and "\\" not in lock_name
    )


def _depends_on_cwd(lock_name, base_dir):
    if base_dir is not None:
        return not os.path.isabs(base_dir) and not os.path.isabs(lock_name)
    return not _is_bare_lock_name(lock_name) and not os.path.isabs(lock_name)


def _temp_lock_dir():
    """Return a per-user temp directory to avoid cross-user ownership races."""
    try:
        identity = str(os.getuid())
    except AttributeError:
        try:
            username = getpass.getuser()
        except Exception:
            username = "unknown"
        identity = hashlib.sha256(username.encode("utf-8", "replace")).hexdigest()[:12]
    return os.path.join(tempfile.gettempdir(), "yyds_lock-{}".format(identity))


def _prepare_directory(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory, mode=0o700, exist_ok=True)


def _prepare_private_directory(directory, _return_stat=False):
    """Create and validate a library-owned per-user lock directory."""
    if os.name != "nt" and hasattr(os, "getuid"):
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            _prepare_directory(directory)
            directory_stat = os.lstat(directory)
        if (
            not stat.S_ISDIR(directory_stat.st_mode)
            or directory_stat.st_uid != os.getuid()
        ):
            raise PermissionError(
                errno.EACCES,
                "lock directory is not owned by the current user",
                directory,
            )
        if stat.S_IMODE(directory_stat.st_mode) != 0o700:
            os.chmod(directory, 0o700)
        return (directory, directory_stat) if _return_stat else directory

    _prepare_directory(directory)
    return (directory, None) if _return_stat else directory


def _prepare_temp_directory(_return_stat=False):
    directory = _temp_lock_dir()
    return _prepare_private_directory(directory, _return_stat=_return_stat)


def _canonicalize_bare_join(target_dir, lock_name, dir_stat=None):
    """Join a prepared bare-lock directory and filename with minimal syscalls."""
    if dir_stat is None:
        try:
            dir_stat = os.lstat(target_dir)
        except OSError:
            return os.path.realpath(
                os.path.abspath(os.path.join(target_dir, lock_name))
            )

    if stat.S_ISDIR(dir_stat.st_mode):
        cache_key = (target_dir, dir_stat.st_dev, dir_stat.st_ino)
        canonical_dir = _default_dir_cache.get(cache_key)
        if canonical_dir is None:
            canonical_dir = os.path.realpath(os.path.abspath(target_dir))
            if len(_default_dir_cache) > 16:
                _default_dir_cache.clear()
            _default_dir_cache[cache_key] = canonical_dir

        joined = os.path.join(canonical_dir, lock_name)
        try:
            file_stat = os.lstat(joined)
        except OSError:
            # File does not exist yet; canonical_dir has no symlinks and
            # lock_name is a bare filename without separators.
            return joined
        if not stat.S_ISLNK(file_stat.st_mode) and os.name != "nt":
            return joined

    return os.path.realpath(os.path.abspath(os.path.join(target_dir, lock_name)))


def _resolve_lock_path(
    lock_name: Union[str, os.PathLike],
    base_dir: Optional[Union[str, os.PathLike]] = None,
    _prepare: bool = True,
) -> str:
    """Resolve a lock name to an absolute canonical path.

    Explicit paths and explicit ``base_dir`` values never silently move to a
    different directory.  Bare names using the default location fall back to
    a per-user temporary directory only when the home directory cannot be
    prepared.
    """
    lock_name = _coerce_path(lock_name, "lock_name")
    if base_dir is not None:
        base_dir = _coerce_path(base_dir, "base_dir")
        drive, _ = os.path.splitdrive(lock_name)
        if drive or os.path.isabs(lock_name) or lock_name.startswith(("/", "\\")):
            raise ValueError("base_dir cannot be used with an absolute lock_name")
        target_dir = os.path.abspath(base_dir)
        if _prepare:
            _prepare_directory(target_dir)
        return os.path.realpath(os.path.join(target_dir, lock_name))

    if not _is_bare_lock_name(lock_name):
        return os.path.realpath(os.path.abspath(lock_name))

    home = os.path.expanduser("~")
    home_is_usable = bool(
        home and home != "~" and os.path.isdir(home) and os.access(home, os.W_OK)
    )
    dir_stat = None
    if home_is_usable:
        target_dir = os.path.join(home, ".yyds_lock")
        if _prepare:
            try:
                target_dir, dir_stat = _prepare_private_directory(
                    target_dir, _return_stat=True
                )
            except OSError:
                target_dir, dir_stat = _prepare_temp_directory(_return_stat=True)
    else:
        if _prepare:
            target_dir, dir_stat = _prepare_temp_directory(_return_stat=True)
        else:
            target_dir = _temp_lock_dir()

    return _canonicalize_bare_join(target_dir, lock_name, dir_stat=dir_stat)


def _logical_lock_key(lock_name, base_dir):
    lock_name = _coerce_path(lock_name, "lock_name")
    logical_base = None
    if base_dir is not None:
        # Keep the caller's spelling stable across chdir().  The canonical
        # resolved path still provides alias detection in _claim_local_path.
        logical_base = _coerce_path(base_dir, "base_dir")
    return lock_name, logical_base


def _current_owner():
    """Use an asyncio Task when available, otherwise the current Thread."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return task if task is not None else threading.current_thread()


def _is_owner_alive(owner):
    if owner is None:
        return False
    if isinstance(owner, threading.Thread):
        return owner.is_alive()
    done_fn = getattr(owner, "done", None)
    if callable(done_fn):
        return not done_fn()
    is_alive_fn = getattr(owner, "is_alive", None)
    if callable(is_alive_fn):
        return bool(is_alive_fn())
    return True


def _resolve_info_owner(info):
    owner_ref = info.get("owner_ref")
    if owner_ref is not None:
        return owner_ref.resolve()
    return info.get("owner_thread")


def _owner_identifier(owner):
    thread_ident = getattr(owner, "ident", None)
    if thread_ident is not None:
        return thread_ident
    task_name = getattr(owner, "get_name", lambda: None)()
    if task_name:
        return "{} ({})".format(task_name, id(owner))
    return "async-task-{}".format(id(owner))


def _record_acquisition(owner, logical_key, lock_path):
    probe = _OwnerKey(owner)
    by_key = _thread_acquisitions.get(probe)
    if by_key is None:

        def _on_key_collect(_ref, _key_holder=[]):
            if _key_holder:
                _dead_owner_keys.append(_key_holder[0])

        tracked_key = _OwnerKey(owner, on_collect=_on_key_collect)
        _on_key_collect.__defaults__[0].append(tracked_key)
        by_key = {}
        _thread_acquisitions[tracked_key] = by_key
    by_key.setdefault(logical_key, []).append(lock_path)


def _peek_recorded_path(owner, logical_key):
    paths = _thread_acquisitions.get(_OwnerKey(owner), {}).get(logical_key)
    return paths[-1] if paths else None


def _pop_recorded_path(owner, logical_key):
    probe = _OwnerKey(owner)
    by_key = _thread_acquisitions.get(probe)
    if not by_key:
        return None

    paths = by_key.get(logical_key)
    if not paths:
        return None

    lock_path = paths.pop()
    if not paths:
        by_key.pop(logical_key, None)
    if not by_key:
        _thread_acquisitions.pop(probe, None)
    return lock_path


def _remove_recorded_path(owner, lock_path, preferred_key=None):
    """Remove one acquisition matching ``lock_path`` for ``owner``."""
    probe = _OwnerKey(owner)
    by_key = _thread_acquisitions.get(probe)
    if not by_key:
        return

    if preferred_key is not None:
        preferred_paths = by_key.get(preferred_key)
        if preferred_paths:
            for index in range(len(preferred_paths) - 1, -1, -1):
                if preferred_paths[index] == lock_path:
                    preferred_paths.pop(index)
                    if not preferred_paths:
                        by_key.pop(preferred_key, None)
                    if not by_key:
                        _thread_acquisitions.pop(probe, None)
                    return

    for logical_key, paths in reversed(list(by_key.items())):
        for index in range(len(paths) - 1, -1, -1):
            if paths[index] == lock_path:
                paths.pop(index)
                if not paths:
                    by_key.pop(logical_key, None)
                if not by_key:
                    _thread_acquisitions.pop(probe, None)
                return


def _purge_recorded_path(owner_or_key, lock_path):
    """Remove all recorded acquisitions of ``lock_path`` for a dead owner."""
    probe = (
        owner_or_key if isinstance(owner_or_key, _OwnerKey) else _OwnerKey(owner_or_key)
    )
    by_key = _thread_acquisitions.get(probe)
    if not by_key:
        return

    for logical_key in list(by_key.keys()):
        remaining = [path for path in by_key[logical_key] if path != lock_path]
        if remaining:
            by_key[logical_key] = remaining
        else:
            by_key.pop(logical_key, None)
    if not by_key:
        _thread_acquisitions.pop(probe, None)


def _write_lock_metadata(handle):
    try:
        offset = 1 if os.name == "nt" else 0
        # Truncate before writing because open(..., "a+b") sets O_APPEND,
        # which on POSIX forces writes to EOF regardless of seek().
        handle.truncate(offset)
        handle.seek(offset)
        payload = "{}\n".format(os.getpid()).encode("utf-8")
        handle.write(payload)
        handle.flush()
    except Exception:
        pass


def _read_lock_pid(lock_path):
    try:
        offset = 1 if os.name == "nt" else 0
        with open(lock_path, "rb") as f:
            if offset:
                f.seek(offset)
            content = f.read(64).decode("utf-8", "ignore").strip()
            if content:
                first_line = content.splitlines()[0].strip()
                if first_line.isdigit():
                    return int(first_line)
    except Exception:
        pass
    return None


def _conflict_message(
    lock_name=None,
    owner_pid=None,
    current_pid=None,
    owner_thread=None,
):
    current_pid = current_pid or os.getpid()
    prefix = "Lock '{}'".format(lock_name) if lock_name else "Lock"
    if owner_pid is not None:
        return "[yyds-lock] Failed to acquire lock: {} is already held by process (PID: {}). Current process (PID: {}) exiting.".format(
            prefix, owner_pid, current_pid
        )
    if owner_thread is not None:
        return "[yyds-lock] Failed to acquire lock: {} is already held by execution owner [{}] in this process. Current instance exiting.".format(
            prefix, owner_thread
        )
    return "[yyds-lock] Failed to acquire lock: {} is already held by another process. Current process (PID: {}) exiting.".format(
        prefix, current_pid
    )


def _emit_log(custom_logger, level: str, message: str, *args, **kwargs):
    if custom_logger is None:
        return False
    log_func = getattr(custom_logger, level, None)
    if callable(log_func):
        if args or kwargs:
            try:
                log_func(message, *args, **kwargs)
            except TypeError:
                formatted = message % args if args else message
                log_func(formatted)
        else:
            log_func(message)
        return True
    elif callable(custom_logger):
        if args or kwargs:
            try:
                custom_logger(message, *args, **kwargs)
            except TypeError:
                formatted = message % args if args else message
                custom_logger(formatted)
        else:
            custom_logger(message)
        return True
    return False


def _handle_conflict(
    lock_name,
    raise_on_conflict,
    detail,
    cause=None,
    custom_logger=None,
    owner_pid=None,
    owner_thread=None,
    current_pid=None,
):
    if raise_on_conflict:
        error = AlreadyLockedError(detail)
        if cause is not None:
            raise error from cause
        raise error

    message = _conflict_message(
        lock_name=lock_name,
        owner_pid=owner_pid,
        current_pid=current_pid,
        owner_thread=owner_thread,
    )
    if _emit_log(custom_logger, "error", message):
        pass
    elif logger.hasHandlers():
        logger.error(message)
    else:
        use_color = bool(getattr(sys.stderr, "isatty", lambda: False)())
        output = "\033[31m{}\033[0m".format(message) if use_color else message
        print(output, file=sys.stderr)
    raise SystemExit(1)


def _is_lock_conflict(error):
    conflict_errnos = {errno.EACCES, errno.EAGAIN}
    if hasattr(errno, "EWOULDBLOCK"):
        conflict_errnos.add(errno.EWOULDBLOCK)
    if error.errno in conflict_errnos:
        return True
    # ERROR_LOCK_VIOLATION and ERROR_SHARING_VIOLATION.
    return getattr(error, "winerror", None) in {32, 33}


def _operation_error(action, lock_path, error):
    return LockOperationError(
        "Unable to {} lock '{}': {}".format(action, lock_path, error)
    )


def _validate_async_wait_options(timeout, poll_interval):
    if timeout is not None:
        if isinstance(timeout, bool):
            raise TypeError("timeout must be a non-negative finite number or None")
        try:
            timeout = float(timeout)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                "timeout must be a non-negative finite number or None"
            ) from exc
        if timeout < 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be a non-negative finite number or None")

    if isinstance(poll_interval, bool):
        raise TypeError("poll_interval must be a positive finite number")
    try:
        poll_interval = float(poll_interval)
    except (TypeError, ValueError) as exc:
        raise TypeError("poll_interval must be a positive finite number") from exc
    if poll_interval <= 0 or not math.isfinite(poll_interval):
        raise ValueError("poll_interval must be a positive finite number")
    return timeout, poll_interval


def _open_raw_lock_file(lock_path, allow_readonly_fallback):
    # Open existing files without O_CREAT / O_APPEND first so shared lock
    # files in sticky directories (/tmp with fs.protected_regular=2) work
    # across cooperating users and writes do not suffer O_APPEND semantics.
    try:
        return open(lock_path, "r+b", buffering=0)
    except FileNotFoundError:
        try:
            return open(lock_path, "a+b", buffering=0)
        except PermissionError:
            if allow_readonly_fallback and os.name != "nt":
                try:
                    st = os.lstat(lock_path)
                except OSError:
                    st = None
                if st is not None and stat.S_ISREG(st.st_mode):
                    return open(lock_path, "rb", buffering=0)
            raise
    except PermissionError:
        if allow_readonly_fallback and os.name != "nt":
            try:
                st = os.lstat(lock_path)
            except OSError:
                st = None
            if st is not None and stat.S_ISREG(st.st_mode):
                return open(lock_path, "rb", buffering=0)
        raise


def _open_lock_handle(lock_path, allow_readonly_fallback=False):
    lock_dir = os.path.dirname(lock_path)
    if lock_dir and not os.path.isdir(lock_dir):
        _prepare_directory(lock_dir)

    # Opening and registering the pending descriptor are atomic with respect to
    # fork via shared read access on _fork_gate, without serializing opens
    # across unrelated threads.
    with _fork_gate.read_guard():
        handle = _open_raw_lock_file(lock_path, allow_readonly_fallback)
        try:
            with _registry_lock:
                _pending_handles.add(handle)
        except BaseException:
            try:
                handle.close()
            except Exception:
                pass
            raise
    return handle


def _discard_pending_handle(handle):
    try:
        handle.close()
    finally:
        with _registry_guard():
            _pending_handles.discard(handle)


def _prepare_windows_lock_byte(handle):
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()


def _acquire_os_lock(handle, block):
    if os.name == "nt":
        import msvcrt

        byte_prepared = False
        backoff = 0.002
        while True:
            try:
                if not byte_prepared:
                    _prepare_windows_lock_byte(handle)
                    byte_prepared = True
                handle.seek(0)
                # LK_LOCK only retries for a bounded period in the MS runtime.
                # Polling LK_NBLCK gives block=True consistent indefinite-wait
                # semantics and remains interruptible by KeyboardInterrupt.
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as exc:
                if not _is_lock_conflict(exc) or not block:
                    raise
                time.sleep(backoff)
                if backoff < 0.05:
                    backoff = min(0.05, backoff * 2)

    import fcntl

    mode = fcntl.LOCK_EX
    if not block:
        mode |= fcntl.LOCK_NB
    fcntl.flock(handle.fileno(), mode)


def _unlock_os_lock(handle):
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _wake_single_async_waiter(waiter_loop, wake_event):
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    if running_loop is waiter_loop:
        wake_event.set()
        return True
    try:
        waiter_loop.call_soon_threadsafe(wake_event.set)
        return True
    except RuntimeError:
        return False


def _wake_next_sync_waiter_locked(lock_path):
    queue = _sync_waiters.get(lock_path)
    if not queue:
        return
    while queue:
        waiter_event = queue.popleft()
        if not waiter_event.is_set():
            waiter_event.set()
            break
    if not queue:
        _sync_waiters.pop(lock_path, None)


def _wake_next_async_waiter_locked(lock_path):
    queue = _async_waiters.get(lock_path)
    if not queue:
        return
    while queue:
        waiter_loop, wake_event = queue.popleft()
        if wake_event.is_set():
            continue
        if _wake_single_async_waiter(waiter_loop, wake_event):
            break
    if not queue:
        _async_waiters.pop(lock_path, None)


def _notify_all_async_waiters_locked(lock_path):
    queue = _async_waiters.pop(lock_path, None)
    if not queue:
        return
    for waiter_loop, wake_event in queue:
        if not wake_event.is_set():
            _wake_single_async_waiter(waiter_loop, wake_event)


def _wake_next_waiters_locked(lock_path):
    _wake_next_sync_waiter_locked(lock_path)
    _wake_next_async_waiter_locked(lock_path)


def _activity_matches(lock_path, expected_event):
    entry = _path_activity.get(lock_path)
    return entry is not None and entry[0] is expected_event


def _stage_release_locked(lock_path, info, purge_all_refs=False):
    owner_key = info.get("owner_ref")
    if purge_all_refs and owner_key is not None:
        _purge_recorded_path(owner_key, lock_path)
    _lock_file_handles.pop(lock_path, None)
    handle_to_close = info["handle"]
    release_event = info["released_event"]
    _pending_handles.add(handle_to_close)
    _path_activity[lock_path] = (release_event, info["owner_thread_id"])
    return handle_to_close, release_event, info.get("owner_done_cb"), owner_key


def _finish_path_activity(lock_path, activity_event):
    with _registry_guard():
        if _activity_matches(lock_path, activity_event):
            _path_activity.pop(lock_path, None)
            activity_event.set()
            _wake_next_waiters_locked(lock_path)


def _finalize_handle_release(
    lock_path,
    handle_to_close,
    release_event,
    owner_done_cb=None,
    owner_key=None,
):
    if owner_done_cb is not None and owner_key is not None:
        live_owner = owner_key.resolve()
        if live_owner is not None and live_owner is _current_owner():
            try:
                live_owner.remove_done_callback(owner_done_cb)
            except Exception:
                pass

    unlock_error = None
    try:
        if handle_to_close.writable():
            _unlock_os_lock(handle_to_close)
        elif os.name != "nt":
            _unlock_os_lock(handle_to_close)
    except OSError as exc:
        unlock_error = exc
    finally:
        try:
            handle_to_close.close()
        finally:
            with _registry_guard():
                _pending_handles.discard(handle_to_close)
                if _activity_matches(lock_path, release_event):
                    _path_activity.pop(lock_path, None)
                release_event.set()
                _wake_next_waiters_locked(lock_path)
    return unlock_error


def _drain_stale_locks(force_scan=False):
    """Proactively reclaim FDs and bookkeeping left behind by dead owners."""
    if _fork_gate.current_thread_active():
        return

    if not force_scan and not _stale_lock_paths and not _dead_owner_keys:
        return

    staged = []
    with _registry_guard():
        while _dead_owner_keys:
            dead_key = _dead_owner_keys.popleft()
            _thread_acquisitions.pop(dead_key, None)

        candidate_paths = set()
        while _stale_lock_paths:
            candidate_paths.add(_stale_lock_paths.popleft())

        if force_scan:
            candidate_paths.update(_lock_file_handles.keys())
            for owner_key in list(_thread_acquisitions.keys()):
                if not _is_owner_alive(owner_key.resolve()):
                    _thread_acquisitions.pop(owner_key, None)

        for path in candidate_paths:
            info = _lock_file_handles.get(path)
            if info is not None and not _is_owner_alive(_resolve_info_owner(info)):
                staged.append(
                    (path,) + _stage_release_locked(path, info, purge_all_refs=True)
                )

    for path, handle_to_close, release_event, done_cb, owner_key in staged:
        _finalize_handle_release(
            path, handle_to_close, release_event, done_cb, owner_key
        )


def _maybe_housekeep():
    global _acquire_op_counter
    _acquire_op_counter += 1
    need_scan = bool(_lock_file_handles) and (_acquire_op_counter & 0x1F) == 0
    if _stale_lock_paths or _dead_owner_keys or need_scan:
        _drain_stale_locks(force_scan=need_scan)


def _claim_local_path(lock_path, logical_key, current_thread, block):
    """Claim the right to perform the OS acquisition for a local path."""
    while True:
        waiter_event = None
        conflict_owner = None
        stale_cleanup = None

        with _registry_guard():
            info = _lock_file_handles.get(lock_path)
            if info is not None:
                owner = _resolve_info_owner(info)
                if owner is current_thread:
                    info["ref_count"] += 1
                    _record_acquisition(current_thread, logical_key, lock_path)
                    return "reentrant", None
                if not _is_owner_alive(owner):
                    stale_cleanup = _stage_release_locked(
                        lock_path, info, purge_all_refs=True
                    )
                else:
                    conflict_owner = info["owner_thread_id"]
                    if block:
                        waiter_event = threading.Event()
                        _sync_waiters.setdefault(lock_path, deque()).append(
                            waiter_event
                        )
            else:
                activity = _path_activity.get(lock_path)
                if activity is None:
                    activity_event = threading.Event()
                    _path_activity[lock_path] = (
                        activity_event,
                        _owner_identifier(current_thread),
                    )
                    return "claimed", activity_event
                conflict_owner = activity[1]
                if block:
                    waiter_event = threading.Event()
                    _sync_waiters.setdefault(lock_path, deque()).append(waiter_event)

        if stale_cleanup is not None:
            handle_to_close, release_event, done_cb, owner_key = stale_cleanup
            _finalize_handle_release(
                lock_path, handle_to_close, release_event, done_cb, owner_key
            )
            continue

        if not block:
            return "conflict", conflict_owner

        completed_normally = False
        try:
            # Bounded wait notices promptly if a live owner crashes without
            # calling release_single().
            waiter_event.wait(timeout=0.2)
            completed_normally = True
        finally:
            with _registry_guard():
                queue = _sync_waiters.get(lock_path)
                if queue is not None:
                    try:
                        queue.remove(waiter_event)
                    except ValueError:
                        pass
                    if not queue:
                        _sync_waiters.pop(lock_path, None)
                if (
                    not completed_normally
                    and waiter_event.is_set()
                    and lock_path not in _lock_file_handles
                    and lock_path not in _path_activity
                ):
                    _wake_next_waiters_locked(lock_path)


def _rollback_acquired_handle(
    lock_path, handle, current_owner, activity_event, lock_info
):
    """Undo a failed in-memory commit after the OS lock was acquired."""
    done_cb = lock_info.get("owner_done_cb")
    if done_cb is not None:
        try:
            current_owner.remove_done_callback(done_cb)
        except Exception:
            pass

    with _registry_guard():
        if _lock_file_handles.get(lock_path) is lock_info:
            _lock_file_handles.pop(lock_path, None)
        _remove_recorded_path(current_owner, lock_path)
        _pending_handles.add(handle)
        _path_activity[lock_path] = (
            activity_event,
            _owner_identifier(current_owner),
        )

    try:
        _unlock_os_lock(handle)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass
    finally:
        with _registry_guard():
            _pending_handles.discard(handle)
            if _activity_matches(lock_path, activity_event):
                _path_activity.pop(lock_path, None)
                activity_event.set()
            lock_info["released_event"].set()
            _wake_next_waiters_locked(lock_path)


def _fallback_after_open_error(lock_name, base_dir, lock_path, error):
    if base_dir is not None or not _is_bare_lock_name(lock_name):
        return None
    if error.errno not in {
        errno.EACCES,
        errno.EPERM,
        errno.EROFS,
        errno.ENOENT,
        errno.ENOTDIR,
    }:
        return None

    fallback_path = os.path.realpath(
        os.path.abspath(os.path.join(_temp_lock_dir(), lock_name))
    )
    if fallback_path == lock_path:
        return None
    _prepare_temp_directory()
    return fallback_path


def _try_fast_reentrant_acquire(current_owner, logical_key, lock_name, base_dir):
    if _depends_on_cwd(lock_name, base_dir):
        return None
    # Reuse the original path without re-running fallback logic for a
    # reentrant call made with the same cwd-independent logical arguments.
    with _registry_guard():
        recorded_path = _peek_recorded_path(current_owner, logical_key)
        if recorded_path is not None:
            info = _lock_file_handles.get(recorded_path)
            if info is not None and _resolve_info_owner(info) is current_owner:
                info["ref_count"] += 1
                _record_acquisition(current_owner, logical_key, recorded_path)
                return recorded_path
    return None


def _build_lock_info(handle, current_owner, lock_path):
    def _on_owner_gc(_ref, _path=lock_path):
        _stale_lock_paths.append(_path)

    owner_ref = _OwnerKey(current_owner, on_collect=_on_owner_gc)
    done_cb = None
    if callable(getattr(current_owner, "add_done_callback", None)):

        def _on_task_done(_task, _path=lock_path):
            _stale_lock_paths.append(_path)
            _drain_stale_locks()

        try:
            current_owner.add_done_callback(_on_task_done)
            done_cb = _on_task_done
        except Exception:
            done_cb = None

    return {
        "handle": handle,
        "owner_ref": owner_ref,
        "owner_done_cb": done_cb,
        "owner_thread_id": _owner_identifier(current_owner),
        "ref_count": 1,
        "released_event": threading.Event(),
    }


def _try_acquire_resolved(
    lock_name,
    logical_key,
    lock_path,
    current_owner,
    block,
    base_dir,
):
    allow_ro_shared = base_dir is not None or not _is_bare_lock_name(lock_name)
    while True:
        claim, claim_data = _claim_local_path(
            lock_path, logical_key, current_owner, block
        )
        if claim == "reentrant":
            return "acquired", lock_path, None, None, None
        if claim == "conflict":
            owner = "unknown" if claim_data is None else claim_data
            return (
                "conflict",
                lock_path,
                "Lock '{}' is already acquired by execution owner {} in this process.".format(
                    lock_name, owner
                ),
                None,
                owner,
            )

        claimed_path = lock_path
        activity_event = claim_data
        handle = None
        committed = False
        try:
            try:
                if allow_ro_shared:
                    handle = _open_lock_handle(lock_path, allow_readonly_fallback=True)
                else:
                    handle = _open_lock_handle(lock_path)
            except OSError as exc:
                _finish_path_activity(claimed_path, activity_event)
                try:
                    fallback_path = _fallback_after_open_error(
                        lock_name, base_dir, lock_path, exc
                    )
                except OSError as fallback_exc:
                    raise _operation_error(
                        "prepare", lock_path, fallback_exc
                    ) from fallback_exc
                if fallback_path is not None:
                    lock_path = fallback_path
                    committed = True
                    continue
                raise _operation_error("open", lock_path, exc) from exc

            try:
                _acquire_os_lock(handle, block)
            except OSError as exc:
                if _is_lock_conflict(exc):
                    _discard_pending_handle(handle)
                    handle = None
                    _finish_path_activity(claimed_path, activity_event)
                    return (
                        "conflict",
                        lock_path,
                        "Lock '{}' is already acquired by another process.".format(
                            lock_name
                        ),
                        exc,
                        None,
                    )
                raise _operation_error("acquire", lock_path, exc) from exc

            _write_lock_metadata(handle)

            lock_info = _build_lock_info(handle, current_owner, lock_path)
            try:
                with _registry_guard():
                    _lock_file_handles[lock_path] = lock_info
                    _record_acquisition(current_owner, logical_key, lock_path)
                    _pending_handles.discard(handle)
                    if _activity_matches(claimed_path, activity_event):
                        _path_activity.pop(claimed_path, None)
                        activity_event.set()
                committed = True
            except BaseException:
                _rollback_acquired_handle(
                    lock_path,
                    handle,
                    current_owner,
                    activity_event,
                    lock_info,
                )
                handle = None
                committed = True
                raise
            return "acquired", lock_path, None, None, None
        finally:
            if not committed:
                try:
                    if handle is not None:
                        _discard_pending_handle(handle)
                finally:
                    _finish_path_activity(claimed_path, activity_event)


def _acquire_single(
    lock_name,
    block=False,
    raise_on_conflict=False,
    base_dir=None,
    custom_logger=None,
):
    _maybe_housekeep()
    logical_key = _logical_lock_key(lock_name, base_dir)
    lock_name = logical_key[0]
    current_thread = _current_owner()
    if block and not isinstance(current_thread, threading.Thread):
        raise ValueError(
            "block=True cannot be used from an asyncio task; "
            "await force_single_async() instead"
        )

    fast_path = _try_fast_reentrant_acquire(
        current_thread, logical_key, lock_name, base_dir
    )
    if fast_path is not None:
        return fast_path, logical_key

    try:
        lock_path = _resolve_lock_path(lock_name, base_dir)
    except OSError as exc:
        raise _operation_error("prepare", lock_name, exc) from exc

    status, resolved_path, detail, cause, owner_thread = _try_acquire_resolved(
        lock_name,
        logical_key,
        lock_path,
        current_thread,
        block,
        base_dir,
    )
    if status == "conflict":
        owner_pid = (
            _read_lock_pid(resolved_path)
            if not raise_on_conflict and cause is not None
            else None
        )
        _handle_conflict(
            lock_name,
            raise_on_conflict,
            detail,
            cause=cause,
            custom_logger=custom_logger,
            owner_pid=owner_pid,
            owner_thread=owner_thread,
        )
    return resolved_path, logical_key


def force_single(
    lock_name: Union[str, os.PathLike] = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: Optional[Union[str, os.PathLike]] = None,
    logger: Any = None,
):
    """Acquire a process-, thread-, and task-exclusive named file lock.

    Repeated acquisition by the same live execution owner is reentrant.  The
    matching number of ``release_single`` calls is required to release it.
    """
    _acquire_single(
        lock_name=lock_name,
        block=block,
        raise_on_conflict=raise_on_conflict,
        base_dir=base_dir,
        custom_logger=logger,
    )


async def _acquire_single_async(
    lock_name="yyds_instance.lock",
    timeout=None,
    poll_interval=0.05,
    base_dir=None,
):
    _maybe_housekeep()
    timeout, poll_interval = _validate_async_wait_options(timeout, poll_interval)
    deadline = None if timeout is None else time.monotonic() + timeout

    logical_key = _logical_lock_key(lock_name, base_dir)
    coerced_lock_name = logical_key[0]
    current_owner = _current_owner()

    fast_path = _try_fast_reentrant_acquire(
        current_owner, logical_key, coerced_lock_name, base_dir
    )
    if fast_path is not None:
        return fast_path, logical_key

    try:
        lock_path = _resolve_lock_path(coerced_lock_name, base_dir)
    except OSError as exc:
        raise _operation_error("prepare", coerced_lock_name, exc) from exc

    loop = asyncio.get_running_loop()
    is_poll_leader = False
    acquired = False

    try:
        while True:
            # Coalesce cross-process followers behind an active local poll
            # leader so N coroutines do not hammer open()+flock() in parallel.
            skip_try = False
            if not is_poll_leader:
                with _registry_guard():
                    leader_interval = _async_poll_leaders.get(lock_path)
                    if leader_interval is not None and leader_interval <= poll_interval:
                        skip_try = True

            if not skip_try:
                status, new_path, detail, cause, _ = _try_acquire_resolved(
                    coerced_lock_name,
                    logical_key,
                    lock_path,
                    current_owner,
                    False,
                    base_dir,
                )
                if new_path != lock_path and is_poll_leader:
                    with _registry_guard():
                        _async_poll_leaders.pop(lock_path, None)
                    is_poll_leader = False
                lock_path = new_path

                if status == "acquired":
                    acquired = True
                    return lock_path, logical_key
            else:
                detail = "Lock '{}' is already acquired by another process.".format(
                    coerced_lock_name
                )
                cause = None

            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                conflict_exc = AlreadyLockedError(detail)
                if cause is not None:
                    conflict_exc.__cause__ = cause
                raise LockTimeoutError(
                    "Timed out after {:.3f}s waiting for lock '{}'.".format(
                        timeout, coerced_lock_name
                    )
                ) from conflict_exc

            wait_entry = None
            local_owner = None
            owner_dead = False
            with _registry_guard():
                info = _lock_file_handles.get(lock_path)
                local_busy = info is not None or lock_path in _path_activity
                if info is not None:
                    local_owner = _resolve_info_owner(info)
                    if not _is_owner_alive(local_owner):
                        owner_dead = True
                        local_busy = False

                if local_busy:
                    if is_poll_leader:
                        _async_poll_leaders.pop(lock_path, None)
                        is_poll_leader = False
                    wake_event = asyncio.Event()
                    wait_entry = (loop, wake_event)
                    _async_waiters.setdefault(lock_path, deque()).append(wait_entry)
                elif not is_poll_leader:
                    leader_interval = _async_poll_leaders.get(lock_path)
                    if leader_interval is None or poll_interval < leader_interval:
                        if cause is not None:
                            _async_poll_leaders[lock_path] = poll_interval
                            is_poll_leader = True
                    else:
                        wake_event = asyncio.Event()
                        wait_entry = (loop, wake_event)
                        _async_waiters.setdefault(lock_path, deque()).append(wait_entry)

            if owner_dead:
                await asyncio.sleep(0)
                continue

            if wait_entry is not None:
                done_cb = None
                same_loop_owner = (
                    callable(getattr(local_owner, "add_done_callback", None))
                    and getattr(local_owner, "get_loop", lambda: None)() is loop
                )
                if same_loop_owner:

                    def _on_owner_done(_task, _loop=loop, _event=wake_event):
                        _wake_single_async_waiter(_loop, _event)

                    done_cb = _on_owner_done
                    try:
                        local_owner.add_done_callback(done_cb)
                    except Exception:
                        done_cb = None

                completed_wait = False
                try:
                    # If a Thread, foreign event loop Task, or poll leader holds
                    # the path, periodically re-check in case of abrupt death.
                    if not same_loop_owner:
                        step = 0.2 if remaining is None else min(0.2, remaining)
                    else:
                        step = remaining

                    if step is None:
                        await wake_event.wait()
                    else:
                        try:
                            await asyncio.wait_for(wake_event.wait(), timeout=step)
                        except asyncio.TimeoutError:
                            pass
                    completed_wait = True
                finally:
                    if done_cb is not None:
                        try:
                            local_owner.remove_done_callback(done_cb)
                        except Exception:
                            pass
                    with _registry_guard():
                        queue = _async_waiters.get(lock_path)
                        if queue is not None:
                            try:
                                queue.remove(wait_entry)
                            except ValueError:
                                pass
                            if not queue:
                                _async_waiters.pop(lock_path, None)
                        if (
                            not completed_wait
                            and wake_event.is_set()
                            and lock_path not in _lock_file_handles
                            and lock_path not in _path_activity
                        ):
                            _wake_next_waiters_locked(lock_path)
            elif not is_poll_leader and cause is None:
                # Local holder or leader just cleared; yield once and retry.
                await asyncio.sleep(0)
            else:
                delay = (
                    poll_interval
                    if remaining is None
                    else min(poll_interval, remaining)
                )
                await asyncio.sleep(delay)
    finally:
        if is_poll_leader:
            with _registry_guard():
                _async_poll_leaders.pop(lock_path, None)
                if (
                    not acquired
                    and lock_path not in _lock_file_handles
                    and lock_path not in _path_activity
                ):
                    _wake_next_async_waiter_locked(lock_path)


async def force_single_async(
    lock_name: Union[str, os.PathLike] = "yyds_instance.lock",
    timeout: Optional[float] = None,
    poll_interval: float = 0.05,
    base_dir: Optional[Union[str, os.PathLike]] = None,
    logger: Any = None,
):
    """Wait asynchronously until a named lock is acquired by this Task.

    The operating-system acquisition remains non-blocking on every attempt,
    so the event loop stays responsive.  A finite timeout raises
    ``LockTimeoutError`` when contention does not clear in time.
    """
    del logger  # Retained for signature symmetry with force_single.
    await _acquire_single_async(
        lock_name=lock_name,
        timeout=timeout,
        poll_interval=poll_interval,
        base_dir=base_dir,
    )


def _emit_unlock_warning(lock_path, unlock_error, logger_arg):
    if logger_arg is not None:
        _emit_log(
            logger_arg,
            "warning",
            "Unable to explicitly unlock '{}': {}".format(lock_path, unlock_error),
        )
    elif logger.hasHandlers():
        logger.warning("Unable to explicitly unlock '%s': %s", lock_path, unlock_error)


def _release_exact_path(
    lock_path,
    owner,
    logical_key=None,
    display_name=None,
    logger_arg=None,
    strict_owner=True,
):
    handle_to_close = None
    release_event = None
    done_cb = None
    owner_key = None

    with _registry_guard():
        info = _lock_file_handles.get(lock_path)
        if info is None:
            return
        info_owner = _resolve_info_owner(info)
        if info_owner is not owner:
            if not strict_owner:
                return
            raise RuntimeError(
                "Cannot release lock '{}' owned by execution owner {} from owner {}.".format(
                    display_name or lock_path,
                    info["owner_thread_id"],
                    _owner_identifier(owner),
                )
            )

        _remove_recorded_path(owner, lock_path, preferred_key=logical_key)

        info["ref_count"] -= 1
        if info["ref_count"] > 0:
            return

        handle_to_close, release_event, done_cb, owner_key = _stage_release_locked(
            lock_path, info
        )

    unlock_error = _finalize_handle_release(
        lock_path, handle_to_close, release_event, done_cb, owner_key
    )
    if unlock_error is not None:
        _emit_unlock_warning(lock_path, unlock_error, logger_arg)


def _release_single_for_owner(
    lock_name: Union[str, os.PathLike],
    owner: Any,
    base_dir: Optional[Union[str, os.PathLike]] = None,
    logger_arg: Any = None,
    strict_owner: bool = True,
):
    logical_key = _logical_lock_key(lock_name, base_dir)
    lock_name = logical_key[0]

    lock_path = None
    used_recorded_path = False

    if _depends_on_cwd(lock_name, base_dir):
        try:
            candidate_path = _resolve_lock_path(lock_name, base_dir, _prepare=False)
        except OSError:
            candidate_path = None
        if candidate_path is not None:
            with _registry_guard():
                info = _lock_file_handles.get(candidate_path)
                if info is not None and _resolve_info_owner(info) is owner:
                    lock_path = candidate_path

    if lock_path is None:
        with _registry_guard():
            lock_path = _pop_recorded_path(owner, logical_key)
            used_recorded_path = lock_path is not None

    if lock_path is None:
        try:
            lock_path = _resolve_lock_path(lock_name, base_dir, _prepare=False)
        except OSError:
            return

    handle_to_close = None
    release_event = None
    done_cb = None
    owner_key = None
    with _registry_guard():
        info = _lock_file_handles.get(lock_path)
        if info is None:
            return
        if _resolve_info_owner(info) is not owner:
            if not strict_owner:
                return
            raise RuntimeError(
                "Cannot release lock '{}' owned by execution owner {} from owner {}.".format(
                    lock_name,
                    info["owner_thread_id"],
                    _owner_identifier(owner),
                )
            )

        if not used_recorded_path:
            _remove_recorded_path(owner, lock_path)

        info["ref_count"] -= 1
        if info["ref_count"] > 0:
            return

        handle_to_close, release_event, done_cb, owner_key = _stage_release_locked(
            lock_path, info
        )

    unlock_error = _finalize_handle_release(
        lock_path, handle_to_close, release_event, done_cb, owner_key
    )
    if unlock_error is not None:
        _emit_unlock_warning(lock_path, unlock_error, logger_arg)


def release_single(
    lock_name: Union[str, os.PathLike] = "yyds_instance.lock",
    base_dir: Optional[Union[str, os.PathLike]] = None,
    logger: Any = None,
):
    """Release one acquisition owned by the current execution owner."""
    _maybe_housekeep()
    _release_single_for_owner(
        lock_name,
        _current_owner(),
        base_dir=base_dir,
        logger_arg=logger,
        strict_owner=True,
    )


class SingleInstanceLock:
    """Reusable context manager for a single named acquisition."""

    def __init__(
        self,
        lock_name="yyds_instance.lock",
        block=False,
        raise_on_conflict=False,
        base_dir=None,
        logger=None,
        timeout=None,
        poll_interval=0.05,
    ):
        self.lock_name = lock_name
        self.block = block
        self.raise_on_conflict = raise_on_conflict
        self.base_dir = base_dir
        self.logger = logger
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._acquired = False
        self._acquiring = False
        self._owner_ref = None
        self._acquired_path = None
        self._acquired_key = None
        self._state_lock = threading.Lock()
        self._pid = os.getpid()
        with _registry_guard():
            _live_lock_instances.add(self)

    def _reset_after_fork(self, current_pid):
        self._acquired = False
        self._acquiring = False
        self._owner_ref = None
        self._acquired_path = None
        self._acquired_key = None
        self._state_lock = threading.Lock()
        self._pid = current_pid

    def _reset_after_fork_if_needed(self):
        if hasattr(os, "register_at_fork"):
            return
        current_pid = os.getpid()
        if current_pid != self._pid:
            with self._state_lock:
                if current_pid != self._pid:
                    self._acquired = False
                    self._acquiring = False
                    self._owner_ref = None
                    self._acquired_path = None
                    self._acquired_key = None
                    self._pid = current_pid

    @property
    def acquired(self):
        self._reset_after_fork_if_needed()
        if not self._acquired:
            return False
        owner_ref = self._owner_ref
        acquired_path = self._acquired_path
        if owner_ref is None or acquired_path is None:
            return False
        owner = owner_ref.resolve()
        if not _is_owner_alive(owner):
            return False
        with _registry_guard():
            info = _lock_file_handles.get(acquired_path)
            return info is not None and _resolve_info_owner(info) is owner

    def acquire(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if self._acquired or self._acquiring:
                raise RuntimeError("This SingleInstanceLock object is already acquired")
            self._acquiring = True

        acquired_path = None
        acquired_key = None
        try:
            acquired_path, acquired_key = _acquire_single(
                lock_name=self.lock_name,
                block=self.block,
                raise_on_conflict=self.raise_on_conflict,
                base_dir=self.base_dir,
                custom_logger=self.logger,
            )
        finally:
            with self._state_lock:
                self._acquiring = False
                if acquired_path is not None:
                    self._owner_ref = _OwnerKey(_current_owner())
                    self._acquired_path = acquired_path
                    self._acquired_key = acquired_key
                    self._acquired = True
        return self

    async def acquire_async(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if self._acquired or self._acquiring:
                raise RuntimeError("This SingleInstanceLock object is already acquired")
            self._acquiring = True

        acquired_path = None
        acquired_key = None
        try:
            if self.block:
                acquired_path, acquired_key = await _acquire_single_async(
                    lock_name=self.lock_name,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                    base_dir=self.base_dir,
                )
            else:
                acquired_path, acquired_key = _acquire_single(
                    lock_name=self.lock_name,
                    block=False,
                    raise_on_conflict=self.raise_on_conflict,
                    base_dir=self.base_dir,
                    custom_logger=self.logger,
                )
        finally:
            with self._state_lock:
                self._acquiring = False
                if acquired_path is not None:
                    self._owner_ref = _OwnerKey(_current_owner())
                    self._acquired_path = acquired_path
                    self._acquired_key = acquired_key
                    self._acquired = True
        return self

    def release(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if not self._acquired:
                return
            current_owner = _current_owner()
            owner = None if self._owner_ref is None else self._owner_ref.resolve()
            if owner is not current_owner:
                raise RuntimeError(
                    "SingleInstanceLock must be released by the execution owner that acquired it"
                )
            _release_exact_path(
                self._acquired_path,
                current_owner,
                logical_key=self._acquired_key,
                display_name=self._acquired_key[0] if self._acquired_key else None,
                logger_arg=self.logger,
                strict_owner=True,
            )
            self._acquired = False
            self._owner_ref = None
            self._acquired_path = None
            self._acquired_key = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    async def __aenter__(self):
        return await self.acquire_async()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.release()


def single_decorator(
    lock_name: Any = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: Optional[Union[str, os.PathLike]] = None,
    timeout: Optional[float] = None,
    poll_interval: float = 0.05,
    logger: Any = None,
):
    """Decorate sync, coroutine, generator, or async-generator functions."""
    if timeout is not None and not block:
        raise ValueError("timeout requires block=True")

    def decorator(func):
        def resolved_name(args, kwargs):
            return lock_name(*args, **kwargs) if callable(lock_name) else lock_name

        if inspect.isasyncgenfunction(func):

            @wraps(func)
            async def async_generator_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                owner = _current_owner()
                if block:
                    acquired_path, acquired_key = await _acquire_single_async(
                        lock_name=resolved_lock_name,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        base_dir=base_dir,
                    )
                else:
                    acquired_path, acquired_key = _acquire_single(
                        lock_name=resolved_lock_name,
                        block=False,
                        raise_on_conflict=raise_on_conflict,
                        base_dir=base_dir,
                        custom_logger=logger,
                    )

                released = [False]

                def release_once():
                    if not released[0]:
                        released[0] = True
                        _release_exact_path(
                            acquired_path,
                            owner,
                            logical_key=acquired_key,
                            display_name=acquired_key[0],
                            logger_arg=logger,
                            strict_owner=False,
                        )

                agen = None
                finalizer = None
                agen_closed = False
                try:
                    agen = func(*args, **kwargs)
                    finalizer = weakref.finalize(agen, release_once)
                    value = None
                    pending_exc = None
                    while True:
                        try:
                            if pending_exc is None:
                                item = await agen.asend(value)
                            else:
                                exc_to_throw = pending_exc
                                pending_exc = None
                                item = await agen.athrow(exc_to_throw)
                        except StopAsyncIteration:
                            agen_closed = True
                            break
                        try:
                            value = yield item
                            pending_exc = None
                        except GeneratorExit:
                            agen_closed = True
                            await agen.aclose()
                            raise
                        except BaseException as exc:
                            value = None
                            pending_exc = exc
                finally:
                    try:
                        if agen is not None and not agen_closed:
                            agen_closed = True
                            await agen.aclose()
                    finally:
                        if finalizer is not None:
                            finalizer.detach()
                        release_once()

            return async_generator_wrapper

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                owner = _current_owner()
                if block:
                    acquired_path, acquired_key = await _acquire_single_async(
                        lock_name=resolved_lock_name,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        base_dir=base_dir,
                    )
                else:
                    acquired_path, acquired_key = _acquire_single(
                        lock_name=resolved_lock_name,
                        block=False,
                        raise_on_conflict=raise_on_conflict,
                        base_dir=base_dir,
                        custom_logger=logger,
                    )
                try:
                    return await func(*args, **kwargs)
                finally:
                    _release_exact_path(
                        acquired_path,
                        owner,
                        logical_key=acquired_key,
                        display_name=acquired_key[0],
                        logger_arg=logger,
                        strict_owner=True,
                    )

            return async_wrapper

        if inspect.isgeneratorfunction(func):
            if timeout is not None:
                raise ValueError("timeout is only supported by asynchronous decorators")

            @wraps(func)
            def generator_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                owner = _current_owner()
                acquired_path, acquired_key = _acquire_single(
                    lock_name=resolved_lock_name,
                    block=block,
                    raise_on_conflict=raise_on_conflict,
                    base_dir=base_dir,
                    custom_logger=logger,
                )
                try:
                    yield from func(*args, **kwargs)
                finally:
                    _release_exact_path(
                        acquired_path,
                        owner,
                        logical_key=acquired_key,
                        display_name=acquired_key[0],
                        logger_arg=logger,
                        strict_owner=False,
                    )

            return generator_wrapper

        if timeout is not None:
            raise ValueError("timeout is only supported by asynchronous decorators")

        @wraps(func)
        def wrapper(*args, **kwargs):
            resolved_lock_name = resolved_name(args, kwargs)
            owner = _current_owner()
            acquired_path, acquired_key = _acquire_single(
                lock_name=resolved_lock_name,
                block=block,
                raise_on_conflict=raise_on_conflict,
                base_dir=base_dir,
                custom_logger=logger,
            )
            try:
                return func(*args, **kwargs)
            finally:
                _release_exact_path(
                    acquired_path,
                    owner,
                    logical_key=acquired_key,
                    display_name=acquired_key[0],
                    logger_arg=logger,
                    strict_owner=True,
                )

        return wrapper

    return decorator


def _release_main_thread_locks():
    """Release MainThread locks before threading._shutdown joins worker threads."""
    main_thread = threading.main_thread()
    staged = []
    with _registry_guard():
        for path, info in list(_lock_file_handles.items()):
            if _resolve_info_owner(info) is main_thread:
                staged.append(
                    (path,) + _stage_release_locked(path, info, purge_all_refs=True)
                )

    for path, handle_to_close, release_event, done_cb, owner_key in staged:
        _finalize_handle_release(
            path, handle_to_close, release_event, done_cb, owner_key
        )


def _cleanup_all():
    with _registry_guard():
        acquired_handles = [info["handle"] for info in _lock_file_handles.values()]
        released_events = [
            info["released_event"] for info in _lock_file_handles.values()
        ]
        pending_handles = list(_pending_handles)
        unique_handles = {
            id(handle): handle for handle in acquired_handles + pending_handles
        }
        all_handles = list(unique_handles.values())
        activity_events = [entry[0] for entry in _path_activity.values()]
        sync_queues = list(_sync_waiters.values())
        waiter_paths = list(_async_waiters.keys())
        _lock_file_handles.clear()
        # Keep every descriptor visible to the fork hook until it is actually
        # closed below.
        _pending_handles.clear()
        _pending_handles.update(all_handles)
        _path_activity.clear()
        _sync_waiters.clear()
        _async_poll_leaders.clear()
        _thread_acquisitions.clear()
        _stale_lock_paths.clear()
        _dead_owner_keys.clear()
        for event in activity_events:
            event.set()
        for event in released_events:
            event.set()
        for queue in sync_queues:
            for waiter_event in queue:
                waiter_event.set()
        for path in waiter_paths:
            _notify_all_async_waiters_locked(path)

    acquired_set = set(acquired_handles)
    for handle in acquired_handles:
        try:
            _unlock_os_lock(handle)
        except Exception:
            pass
        try:
            handle.close()
        except Exception:
            pass

    for handle in pending_handles:
        if handle not in acquired_set:
            try:
                handle.close()
            except Exception:
                pass

    with _registry_guard():
        for handle in all_handles:
            _pending_handles.discard(handle)


if hasattr(threading, "_register_atexit"):
    threading._register_atexit(_release_main_thread_locks)
atexit.register(_cleanup_all)


if hasattr(os, "register_at_fork"):

    def _before_fork():
        _fork_gate.write_acquire()

    def _after_fork_parent():
        _fork_gate.write_release()

    def _after_fork_child():
        # Do not call LOCK_UN in the child: the open file description may be
        # shared with the parent.  Closing only the child's duplicate leaves
        # the parent's lock intact.
        global _fork_gate, _registry_lock
        global _lock_file_handles, _pending_handles
        global _path_activity, _sync_waiters, _async_waiters
        global _async_poll_leaders, _thread_acquisitions

        handles = [info["handle"] for info in _lock_file_handles.values()]
        handles.extend(_pending_handles)
        seen = set()
        for handle in handles:
            if id(handle) in seen:
                continue
            seen.add(id(handle))
            try:
                handle.close()
            except Exception:
                pass

        _lock_file_handles = {}
        _pending_handles = set()
        _path_activity = {}
        _sync_waiters = {}
        _async_waiters = {}
        _async_poll_leaders = {}
        _thread_acquisitions = {}
        _stale_lock_paths.clear()
        _dead_owner_keys.clear()
        # Never acquire or release threading locks inherited from vanished
        # threads.  Fresh objects make the child immediately usable.
        _registry_lock = threading.Lock()
        _fork_gate = _ForkReadWriteGate()
        current_pid = os.getpid()
        for lock_instance in list(_live_lock_instances):
            lock_instance._reset_after_fork(current_pid)

    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_parent,
        after_in_child=_after_fork_child,
    )
