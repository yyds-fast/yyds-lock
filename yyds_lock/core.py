# -*- coding:utf-8 -*-

import atexit
import asyncio
import errno
import getpass
import hashlib
import inspect
import logging
import os
import stat
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from functools import wraps


logger = logging.getLogger("yyds_lock")


class YYDSLockError(Exception):
    """Base exception for yyds-lock."""


class AlreadyLockedError(YYDSLockError):
    """Raised when a lock cannot be acquired because it is already held."""


class LockOperationError(YYDSLockError):
    """Raised when a lock file or operating-system lock operation fails."""


# Every access to the registries below goes through _registry_guard.  The
# outer fork gate lets the pre-fork callback wait for registry mutations to
# finish without ever taking an inherited threading lock in the child.
_fork_gate = threading.RLock()
_registry_lock = threading.Lock()

# Canonical path -> acquired lock information.
_lock_file_handles = {}

# Handles opened by this process but not currently stored in
# _lock_file_handles.  This includes handles being acquired or released, so a
# child forked at any point can close every inherited descriptor safely.
_pending_handles = set()

# Canonical path -> Event.  A path is present while a local thread is acquiring
# or releasing it.  This prevents platform-specific file-lock semantics from
# allowing two threads in one process to overwrite each other's registry data.
_path_activity = {}

# Execution owner (Thread or asyncio Task) -> logical lock key -> acquisition
# paths.  Recording the path chosen at acquire time means release_single does
# not have to repeat fallback decisions after permissions or the working
# directory change.
_thread_acquisitions = {}


@contextmanager
def _registry_guard():
    with _fork_gate:
        with _registry_lock:
            yield


def _coerce_path(value, parameter):
    try:
        value = os.fspath(value)
    except TypeError as exc:
        raise TypeError("{} must be a string or os.PathLike".format(parameter)) from exc

    if isinstance(value, bytes):
        raise TypeError("{} must resolve to text, not bytes".format(parameter))
    if not value:
        raise ValueError("{} cannot be empty".format(parameter))
    return value


def _is_bare_lock_name(lock_name):
    return (
        not os.path.isabs(lock_name)
        and "/" not in lock_name
        and "\\" not in lock_name
    )


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
    os.makedirs(directory, mode=0o700, exist_ok=True)


def _prepare_temp_directory():
    directory = _temp_lock_dir()
    _prepare_directory(directory)
    if os.name != "nt" and hasattr(os, "getuid"):
        directory_stat = os.lstat(directory)
        if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_uid != os.getuid():
            raise PermissionError(
                errno.EACCES,
                "temporary lock directory is not owned by the current user",
                directory,
            )
        if stat.S_IMODE(directory_stat.st_mode) & 0o077:
            os.chmod(directory, 0o700)
    return directory


def _resolve_lock_path(lock_name: str, base_dir: str = None) -> str:
    """Resolve a lock name to an absolute canonical path.

    Explicit paths and explicit ``base_dir`` values never silently move to a
    different directory.  Bare names using the default location fall back to
    a per-user temporary directory only when the home directory cannot be
    prepared.
    """
    lock_name = _coerce_path(lock_name, "lock_name")
    if base_dir is not None:
        base_dir = _coerce_path(base_dir, "base_dir")

    if not _is_bare_lock_name(lock_name):
        return os.path.realpath(os.path.abspath(lock_name))

    if base_dir is not None:
        target_dir = os.path.abspath(base_dir)
        _prepare_directory(target_dir)
    else:
        home = os.path.expanduser("~")
        home_is_usable = bool(
            home
            and home != "~"
            and os.path.isdir(home)
            and os.access(home, os.W_OK)
        )
        target_dir = os.path.join(home, ".yyds_lock") if home_is_usable else _temp_lock_dir()
        try:
            if target_dir == _temp_lock_dir():
                _prepare_temp_directory()
            else:
                _prepare_directory(target_dir)
        except OSError:
            target_dir = _prepare_temp_directory()

    return os.path.realpath(os.path.abspath(os.path.join(target_dir, lock_name)))


def _logical_lock_key(lock_name, base_dir):
    lock_name = _coerce_path(lock_name, "lock_name")
    normalized_base = None
    if base_dir is not None:
        normalized_base = os.path.abspath(_coerce_path(base_dir, "base_dir"))
    return lock_name, normalized_base


def _current_owner():
    """Use an asyncio Task when available, otherwise the current Thread."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return task if task is not None else threading.current_thread()


def _owner_identifier(owner):
    thread_ident = getattr(owner, "ident", None)
    if thread_ident is not None:
        return thread_ident
    task_name = getattr(owner, "get_name", lambda: None)()
    if task_name:
        return "{} ({})".format(task_name, id(owner))
    return "async-task-{}".format(id(owner))


def _record_acquisition(thread, logical_key, lock_path):
    by_key = _thread_acquisitions.setdefault(thread, {})
    by_key.setdefault(logical_key, []).append(lock_path)


def _peek_recorded_path(thread, logical_key):
    paths = _thread_acquisitions.get(thread, {}).get(logical_key)
    return paths[-1] if paths else None


def _pop_recorded_path(thread, logical_key):
    by_key = _thread_acquisitions.get(thread)
    if not by_key:
        return None

    paths = by_key.get(logical_key)
    if not paths:
        return None

    lock_path = paths.pop()
    if not paths:
        by_key.pop(logical_key, None)
    if not by_key:
        _thread_acquisitions.pop(thread, None)
    return lock_path


def _remove_recorded_path(thread, lock_path):
    """Remove one acquisition when release uses an equivalent path spelling."""
    by_key = _thread_acquisitions.get(thread)
    if not by_key:
        return

    for logical_key, paths in reversed(list(by_key.items())):
        for index in range(len(paths) - 1, -1, -1):
            if paths[index] == lock_path:
                paths.pop(index)
                if not paths:
                    by_key.pop(logical_key, None)
                if not by_key:
                    _thread_acquisitions.pop(thread, None)
                return


def _conflict_message():
    return "[yyds-lock] 错误: 脚本进程已在运行中，当前实例自动退出！"


def _handle_conflict(lock_name, raise_on_conflict, detail, cause=None):
    if raise_on_conflict:
        error = AlreadyLockedError(detail)
        if cause is not None:
            raise error from cause
        raise error

    message = _conflict_message()
    if logger.hasHandlers():
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


def _open_lock_handle(lock_path):
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        _prepare_directory(lock_dir)

    # Opening and registering the pending descriptor are atomic with respect to
    # fork.  The potentially blocking OS lock call happens after the gate is
    # released.
    with _fork_gate:
        handle = open(lock_path, "a+b")
        with _registry_lock:
            _pending_handles.add(handle)
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

        _prepare_windows_lock_byte(handle)
        while True:
            handle.seek(0)
            try:
                # LK_LOCK only retries for a bounded period in the MS runtime.
                # Polling LK_NBLCK gives block=True consistent indefinite-wait
                # semantics and remains interruptible by KeyboardInterrupt.
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as exc:
                if not _is_lock_conflict(exc) or not block:
                    raise
                time.sleep(0.05)

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


def _claim_local_path(lock_path, logical_key, current_thread, block):
    """Claim the right to perform the OS acquisition for a local path."""
    while True:
        wait_event = None
        conflict_owner = None

        with _registry_guard():
            info = _lock_file_handles.get(lock_path)
            if info is not None:
                if info["owner_thread"] is current_thread:
                    info["ref_count"] += 1
                    _record_acquisition(current_thread, logical_key, lock_path)
                    return "reentrant", None
                conflict_owner = _owner_identifier(info["owner_thread"])
                wait_event = info["released_event"]
            else:
                wait_event = _path_activity.get(lock_path)
                if wait_event is None:
                    activity_event = threading.Event()
                    _path_activity[lock_path] = activity_event
                    return "claimed", activity_event

        if not block:
            return "conflict", conflict_owner
        wait_event.wait()


def _finish_path_activity(lock_path, activity_event):
    with _registry_guard():
        if _path_activity.get(lock_path) is activity_event:
            _path_activity.pop(lock_path, None)
            activity_event.set()


def _fallback_after_open_error(lock_name, base_dir, lock_path, error):
    if base_dir is not None or not _is_bare_lock_name(lock_name):
        return None
    if error.errno not in {errno.EACCES, errno.EPERM, errno.EROFS, errno.ENOENT, errno.ENOTDIR}:
        return None

    fallback_path = os.path.realpath(
        os.path.abspath(os.path.join(_temp_lock_dir(), lock_name))
    )
    if fallback_path == lock_path:
        return None
    _prepare_temp_directory()
    return fallback_path


def force_single(
    lock_name: str = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: str = None,
):
    """Acquire a process-, thread-, and task-exclusive named file lock.

    Repeated acquisition by the same live execution owner is reentrant.  The
    matching number of ``release_single`` calls is required to release it.
    """
    logical_key = _logical_lock_key(lock_name, base_dir)
    lock_name = logical_key[0]
    current_thread = _current_owner()
    if block and not isinstance(current_thread, threading.Thread):
        raise ValueError(
            "block=True cannot be used from an asyncio task; run the blocking call in an executor"
        )

    # Reuse the original path without re-running fallback logic for a
    # reentrant call made with the same logical arguments.
    with _registry_guard():
        recorded_path = _peek_recorded_path(current_thread, logical_key)
        if recorded_path is not None:
            info = _lock_file_handles.get(recorded_path)
            if info is not None and info["owner_thread"] is current_thread:
                info["ref_count"] += 1
                _record_acquisition(current_thread, logical_key, recorded_path)
                return

    try:
        lock_path = _resolve_lock_path(lock_name, base_dir)
    except OSError as exc:
        raise _operation_error("prepare", lock_name, exc) from exc

    while True:
        claim, claim_data = _claim_local_path(
            lock_path, logical_key, current_thread, block
        )
        if claim == "reentrant":
            return
        if claim == "conflict":
            owner = "unknown" if claim_data is None else claim_data
            _handle_conflict(
                lock_name,
                raise_on_conflict,
                "Lock '{}' is already acquired by execution owner {} in this process.".format(
                    lock_name, owner
                ),
            )

        activity_event = claim_data
        try:
            handle = _open_lock_handle(lock_path)
        except OSError as exc:
            _finish_path_activity(lock_path, activity_event)
            try:
                fallback_path = _fallback_after_open_error(
                    lock_name, base_dir, lock_path, exc
                )
            except OSError as fallback_exc:
                raise _operation_error("prepare", lock_path, fallback_exc) from fallback_exc
            if fallback_path is not None:
                lock_path = fallback_path
                continue
            raise _operation_error("open", lock_path, exc) from exc

        try:
            _acquire_os_lock(handle, block)
        except OSError as exc:
            _discard_pending_handle(handle)
            _finish_path_activity(lock_path, activity_event)
            if _is_lock_conflict(exc):
                _handle_conflict(
                    lock_name,
                    raise_on_conflict,
                    "Lock '{}' is already acquired by another process.".format(lock_name),
                    cause=exc,
                )
            raise _operation_error("acquire", lock_path, exc) from exc
        except BaseException:
            _discard_pending_handle(handle)
            _finish_path_activity(lock_path, activity_event)
            raise

        with _registry_guard():
            _pending_handles.discard(handle)
            _lock_file_handles[lock_path] = {
                "handle": handle,
                "owner_thread": current_thread,
                # Kept for diagnostics and compatibility with code inspecting
                # the previous private registry shape.
                "owner_thread_id": _owner_identifier(current_thread),
                "ref_count": 1,
                "released_event": threading.Event(),
            }
            _record_acquisition(current_thread, logical_key, lock_path)
            if _path_activity.get(lock_path) is activity_event:
                _path_activity.pop(lock_path, None)
                activity_event.set()
        return


def release_single(lock_name: str = "yyds_instance.lock", base_dir: str = None):
    """Release one acquisition owned by the current execution owner."""
    logical_key = _logical_lock_key(lock_name, base_dir)
    lock_name = logical_key[0]
    current_thread = _current_owner()

    with _registry_guard():
        lock_path = _pop_recorded_path(current_thread, logical_key)
        used_recorded_path = lock_path is not None

    if lock_path is None:
        try:
            lock_path = _resolve_lock_path(lock_name, base_dir)
        except OSError:
            return

    handle_to_close = None
    release_event = None
    with _registry_guard():
        info = _lock_file_handles.get(lock_path)
        if info is None:
            return
        if info["owner_thread"] is not current_thread:
            raise RuntimeError(
                "Cannot release lock '{}' owned by execution owner {} from owner {}.".format(
                    lock_name, info["owner_thread_id"], _owner_identifier(current_thread)
                )
            )

        # If the release path was an equivalent spelling rather than the
        # original logical key, keep the acquisition bookkeeping balanced.
        if not used_recorded_path:
            _remove_recorded_path(current_thread, lock_path)

        info["ref_count"] -= 1
        if info["ref_count"] > 0:
            return

        _lock_file_handles.pop(lock_path, None)
        handle_to_close = info["handle"]
        release_event = info["released_event"]
        _pending_handles.add(handle_to_close)
        _path_activity[lock_path] = release_event

    unlock_error = None
    try:
        _unlock_os_lock(handle_to_close)
    except OSError as exc:
        unlock_error = exc
    finally:
        try:
            handle_to_close.close()
        finally:
            with _registry_guard():
                _pending_handles.discard(handle_to_close)
                if _path_activity.get(lock_path) is release_event:
                    _path_activity.pop(lock_path, None)
                    release_event.set()

    if unlock_error is not None and logger.hasHandlers():
        logger.warning("Unable to explicitly unlock '%s': %s", lock_path, unlock_error)


class SingleInstanceLock:
    """Reusable context manager for a single named acquisition."""

    def __init__(
        self,
        lock_name="yyds_instance.lock",
        block=False,
        raise_on_conflict=False,
        base_dir=None,
    ):
        self.lock_name = lock_name
        self.block = block
        self.raise_on_conflict = raise_on_conflict
        self.base_dir = base_dir
        self._acquired = False
        self._owner = None
        self._state_lock = threading.Lock()
        self._pid = os.getpid()

    def _reset_after_fork_if_needed(self):
        current_pid = os.getpid()
        if current_pid != self._pid:
            self._acquired = False
            self._owner = None
            self._state_lock = threading.Lock()
            self._pid = current_pid

    @property
    def acquired(self):
        self._reset_after_fork_if_needed()
        return self._acquired

    def acquire(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if self._acquired:
                raise RuntimeError("This SingleInstanceLock object is already acquired")
            force_single(
                self.lock_name,
                block=self.block,
                raise_on_conflict=self.raise_on_conflict,
                base_dir=self.base_dir,
            )
            self._owner = _current_owner()
            self._acquired = True
        return self

    def release(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if not self._acquired:
                return
            if self._owner is not _current_owner():
                raise RuntimeError(
                    "SingleInstanceLock must be released by the execution owner that acquired it"
                )
            release_single(self.lock_name, self.base_dir)
            self._acquired = False
            self._owner = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


def single_decorator(
    lock_name: str = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: str = None,
):
    """Decorate sync, coroutine, generator, or async-generator functions."""

    def decorator(func):
        def resolved_name(args, kwargs):
            return lock_name(*args, **kwargs) if callable(lock_name) else lock_name

        if inspect.isasyncgenfunction(func):
            if block:
                raise ValueError(
                    "block=True is not supported for async decorators; it would block the event loop"
                )

            @wraps(func)
            async def async_generator_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                force_single(resolved_lock_name, block, raise_on_conflict, base_dir)
                try:
                    async for item in func(*args, **kwargs):
                        yield item
                finally:
                    release_single(resolved_lock_name, base_dir)

            return async_generator_wrapper

        if inspect.iscoroutinefunction(func):
            if block:
                raise ValueError(
                    "block=True is not supported for async decorators; it would block the event loop"
                )

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                force_single(resolved_lock_name, block, raise_on_conflict, base_dir)
                try:
                    return await func(*args, **kwargs)
                finally:
                    release_single(resolved_lock_name, base_dir)

            return async_wrapper

        if inspect.isgeneratorfunction(func):
            @wraps(func)
            def generator_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                force_single(resolved_lock_name, block, raise_on_conflict, base_dir)
                try:
                    yield from func(*args, **kwargs)
                finally:
                    release_single(resolved_lock_name, base_dir)

            return generator_wrapper

        @wraps(func)
        def wrapper(*args, **kwargs):
            resolved_lock_name = resolved_name(args, kwargs)
            force_single(resolved_lock_name, block, raise_on_conflict, base_dir)
            try:
                return func(*args, **kwargs)
            finally:
                release_single(resolved_lock_name, base_dir)

        return wrapper


    return decorator


def _cleanup_all():
    with _registry_guard():
        acquired_handles = [
            info["handle"] for info in _lock_file_handles.values()
        ]
        pending_handles = list(_pending_handles)
        unique_handles = {
            id(handle): handle for handle in acquired_handles + pending_handles
        }
        all_handles = list(unique_handles.values())
        activity_events = list(_path_activity.values())
        _lock_file_handles.clear()
        # Keep every descriptor visible to the fork hook until it is actually
        # closed below.
        _pending_handles.clear()
        _pending_handles.update(all_handles)
        _path_activity.clear()
        _thread_acquisitions.clear()
        for event in activity_events:
            event.set()

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
        if handle not in acquired_handles:
            try:
                handle.close()
            except Exception:
                pass

    with _registry_guard():
        for handle in all_handles:
            _pending_handles.discard(handle)


atexit.register(_cleanup_all)


if hasattr(os, "register_at_fork"):
    def _before_fork():
        _fork_gate.acquire()


    def _after_fork_parent():
        _fork_gate.release()


    def _after_fork_child():
        # Do not call LOCK_UN in the child: the open file description may be
        # shared with the parent.  Closing only the child's duplicate leaves
        # the parent's lock intact.
        global _fork_gate, _registry_lock
        global _lock_file_handles, _pending_handles
        global _path_activity, _thread_acquisitions

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
        _thread_acquisitions = {}
        # Never acquire or release threading locks inherited from vanished
        # threads.  Fresh objects make the child immediately usable.
        _registry_lock = threading.Lock()
        _fork_gate = threading.RLock()


    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_parent,
        after_in_child=_after_fork_child,
    )
