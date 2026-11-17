# -*- coding:utf-8 -*-

import atexit
import asyncio
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
from contextlib import contextmanager
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

# Canonical path -> set of (event_loop, asyncio.Event) waiting for local
# ownership/activity on the path to clear.
_async_waiters = {}

# Execution owner (Thread or asyncio Task) -> logical lock key -> acquisition
# paths.  Recording the path chosen at acquire time means release_single does
# not have to repeat fallback decisions after permissions or the working
# directory change.
_thread_acquisitions = {}

# Live SingleInstanceLock instances so fork child cleanup can reset them
# atomically before any child thread can race on them.
_live_lock_instances = weakref.WeakSet()


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
    os.makedirs(directory, mode=0o700, exist_ok=True)


def _prepare_private_directory(directory):
    """Create and validate a library-owned per-user lock directory."""
    _prepare_directory(directory)
    if os.name != "nt" and hasattr(os, "getuid"):
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
        if stat.S_IMODE(directory_stat.st_mode) & 0o077:
            os.chmod(directory, 0o700)
    return directory


def _prepare_temp_directory():
    directory = _temp_lock_dir()
    return _prepare_private_directory(directory)


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
        if drive or os.path.isabs(lock_name):
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
    if home_is_usable:
        target_dir = os.path.join(home, ".yyds_lock")
        if _prepare:
            try:
                _prepare_private_directory(target_dir)
            except OSError:
                target_dir = _prepare_temp_directory()
    else:
        target_dir = _prepare_temp_directory() if _prepare else _temp_lock_dir()

    return os.path.realpath(os.path.abspath(os.path.join(target_dir, lock_name)))


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
    if isinstance(owner, threading.Thread):
        return owner.is_alive()
    done_fn = getattr(owner, "done", None)
    if callable(done_fn):
        return not done_fn()
    is_alive_fn = getattr(owner, "is_alive", None)
    if callable(is_alive_fn):
        return bool(is_alive_fn())
    return True


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


def _purge_recorded_path(owner, lock_path):
    """Remove all recorded acquisitions of ``lock_path`` for a dead owner."""
    by_key = _thread_acquisitions.get(owner)
    if not by_key:
        return

    for logical_key in list(by_key.keys()):
        remaining = [path for path in by_key[logical_key] if path != lock_path]
        if remaining:
            by_key[logical_key] = remaining
        else:
            by_key.pop(logical_key, None)
    if not by_key:
        _thread_acquisitions.pop(owner, None)


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
        try:
            log_func(message, *args, **kwargs)
        except TypeError:
            formatted = message % args if args else message
            log_func(formatted)
        return True
    elif callable(custom_logger):
        try:
            custom_logger(message, *args, **kwargs)
        except TypeError:
            formatted = message % args if args else message
            custom_logger(formatted)
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


def _open_lock_handle(lock_path):
    lock_dir = os.path.dirname(lock_path)
    if lock_dir and not os.path.isdir(lock_dir):
        _prepare_directory(lock_dir)

    # Opening and registering the pending descriptor are atomic with respect to
    # fork.  The potentially blocking OS lock call happens after the gate is
    # released.
    with _fork_gate:
        handle = open(lock_path, "a+b", buffering=0)
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


def _notify_async_waiters_locked(lock_path):
    waiters = _async_waiters.pop(lock_path, None)
    if not waiters:
        return
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    for waiter_loop, wake_event in waiters:
        if running_loop is waiter_loop:
            wake_event.set()
        else:
            try:
                waiter_loop.call_soon_threadsafe(wake_event.set)
            except RuntimeError:
                pass


def _wake_single_async_waiter(waiter_loop, wake_event):
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    if running_loop is waiter_loop:
        wake_event.set()
    else:
        try:
            waiter_loop.call_soon_threadsafe(wake_event.set)
        except RuntimeError:
            pass


def _stage_release_locked(lock_path, info, purge_all_refs=False):
    if purge_all_refs:
        _purge_recorded_path(info["owner_thread"], lock_path)
    _lock_file_handles.pop(lock_path, None)
    handle_to_close = info["handle"]
    release_event = info["released_event"]
    _pending_handles.add(handle_to_close)
    _path_activity[lock_path] = release_event
    return handle_to_close, release_event


def _finish_path_activity(lock_path, activity_event):
    with _registry_guard():
        if _path_activity.get(lock_path) is activity_event:
            _path_activity.pop(lock_path, None)
            activity_event.set()
            _notify_async_waiters_locked(lock_path)


def _finalize_handle_release(lock_path, handle_to_close, release_event):
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
                    _notify_async_waiters_locked(lock_path)
    return unlock_error


def _claim_local_path(lock_path, logical_key, current_thread, block):
    """Claim the right to perform the OS acquisition for a local path."""
    while True:
        wait_event = None
        conflict_owner = None
        stale_cleanup = None

        with _registry_guard():
            info = _lock_file_handles.get(lock_path)
            if info is not None:
                owner = info["owner_thread"]
                if owner is current_thread:
                    info["ref_count"] += 1
                    _record_acquisition(current_thread, logical_key, lock_path)
                    return "reentrant", None
                if not _is_owner_alive(owner):
                    stale_cleanup = _stage_release_locked(
                        lock_path, info, purge_all_refs=True
                    )
                else:
                    conflict_owner = _owner_identifier(owner)
                    wait_event = info["released_event"]
            else:
                wait_event = _path_activity.get(lock_path)
                if wait_event is None:
                    activity_event = threading.Event()
                    _path_activity[lock_path] = activity_event
                    return "claimed", activity_event

        if stale_cleanup is not None:
            handle_to_close, release_event = stale_cleanup
            _finalize_handle_release(lock_path, handle_to_close, release_event)
            continue

        if not block:
            return "conflict", conflict_owner
        # Use a bounded wait so we notice promptly if a live owner crashes
        # or exits without calling release_single().
        wait_event.wait(timeout=0.2)


def _rollback_acquired_handle(
    lock_path, handle, current_owner, activity_event, lock_info
):
    """Undo a failed in-memory commit after the OS lock was acquired."""
    with _registry_guard():
        if _lock_file_handles.get(lock_path) is lock_info:
            _lock_file_handles.pop(lock_path, None)
        _remove_recorded_path(current_owner, lock_path)
        _pending_handles.add(handle)
        _path_activity[lock_path] = activity_event

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
            if _path_activity.get(lock_path) is activity_event:
                _path_activity.pop(lock_path, None)
                activity_event.set()
            lock_info["released_event"].set()
            _notify_async_waiters_locked(lock_path)


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
        return False
    # Reuse the original path without re-running fallback logic for a
    # reentrant call made with the same cwd-independent logical arguments.
    with _registry_guard():
        recorded_path = _peek_recorded_path(current_owner, logical_key)
        if recorded_path is not None:
            info = _lock_file_handles.get(recorded_path)
            if info is not None and info["owner_thread"] is current_owner:
                info["ref_count"] += 1
                _record_acquisition(current_owner, logical_key, recorded_path)
                return True
    return False


def _try_acquire_resolved(
    lock_name,
    logical_key,
    lock_path,
    current_owner,
    block,
    base_dir,
):
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

            lock_info = {
                "handle": handle,
                "owner_thread": current_owner,
                # Kept for diagnostics and compatibility with code inspecting
                # the previous private registry shape.
                "owner_thread_id": _owner_identifier(current_owner),
                "ref_count": 1,
                "released_event": threading.Event(),
            }
            try:
                with _registry_guard():
                    _lock_file_handles[lock_path] = lock_info
                    _record_acquisition(current_owner, logical_key, lock_path)
                    _pending_handles.discard(handle)
                    if _path_activity.get(claimed_path) is activity_event:
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
    logical_key = _logical_lock_key(lock_name, base_dir)
    lock_name = logical_key[0]
    current_thread = _current_owner()
    if block and not isinstance(current_thread, threading.Thread):
        raise ValueError(
            "block=True cannot be used from an asyncio task; "
            "await force_single_async() instead"
        )

    if _try_fast_reentrant_acquire(current_thread, logical_key, lock_name, base_dir):
        return

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
            custom_logger=logger,
            owner_pid=owner_pid,
            owner_thread=owner_thread,
        )


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
    timeout, poll_interval = _validate_async_wait_options(timeout, poll_interval)
    deadline = None if timeout is None else time.monotonic() + timeout

    logical_key = _logical_lock_key(lock_name, base_dir)
    coerced_lock_name = logical_key[0]
    current_owner = _current_owner()

    if _try_fast_reentrant_acquire(
        current_owner, logical_key, coerced_lock_name, base_dir
    ):
        return

    try:
        lock_path = _resolve_lock_path(coerced_lock_name, base_dir)
    except OSError as exc:
        raise _operation_error("prepare", coerced_lock_name, exc) from exc

    loop = asyncio.get_running_loop()

    while True:
        status, lock_path, detail, cause, _ = _try_acquire_resolved(
            coerced_lock_name,
            logical_key,
            lock_path,
            current_owner,
            False,
            base_dir,
        )
        if status == "acquired":
            return

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
                local_owner = info["owner_thread"]
                if not _is_owner_alive(local_owner):
                    owner_dead = True
                    local_busy = False
            if local_busy:
                wake_event = asyncio.Event()
                wait_entry = (loop, wake_event)
                _async_waiters.setdefault(lock_path, set()).add(wait_entry)

        if owner_dead:
            await asyncio.sleep(0)
            continue

        if wait_entry is not None:
            done_cb = None
            if callable(getattr(local_owner, "add_done_callback", None)):

                def _on_owner_done(_task, _loop=loop, _event=wake_event):
                    _wake_single_async_waiter(_loop, _event)

                done_cb = _on_owner_done
                try:
                    local_owner.add_done_callback(done_cb)
                except Exception:
                    done_cb = None

            try:
                # If a live Thread holds the lock, periodically re-check in case
                # that thread exits abruptly without calling release_single().
                if isinstance(local_owner, threading.Thread):
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
            finally:
                if done_cb is not None:
                    try:
                        local_owner.remove_done_callback(done_cb)
                    except Exception:
                        pass
                with _registry_guard():
                    waiters = _async_waiters.get(lock_path)
                    if waiters is not None:
                        waiters.discard(wait_entry)
                        if not waiters:
                            _async_waiters.pop(lock_path, None)
        elif cause is None:
            # A local holder released the lock between our check and registering
            # the waiter; yield once and retry immediately.
            await asyncio.sleep(0)
        else:
            delay = poll_interval if remaining is None else min(poll_interval, remaining)
            await asyncio.sleep(delay)


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
                if info is not None and info["owner_thread"] is owner:
                    lock_path = candidate_path

    if lock_path is None:
        with _registry_guard():
            lock_path = _pop_recorded_path(owner, logical_key)
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
        if info["owner_thread"] is not owner:
            if not strict_owner:
                return
            raise RuntimeError(
                "Cannot release lock '{}' owned by execution owner {} from owner {}.".format(
                    lock_name,
                    info["owner_thread_id"],
                    _owner_identifier(owner),
                )
            )

        # If the release path was an equivalent spelling rather than the
        # original logical key, keep the acquisition bookkeeping balanced.
        if not used_recorded_path:
            _remove_recorded_path(owner, lock_path)

        info["ref_count"] -= 1
        if info["ref_count"] > 0:
            return

        handle_to_close, release_event = _stage_release_locked(lock_path, info)

    unlock_error = _finalize_handle_release(lock_path, handle_to_close, release_event)

    if unlock_error is not None:
        if logger_arg is not None:
            _emit_log(
                logger_arg,
                "warning",
                "Unable to explicitly unlock '%s': %s",
                lock_path,
                unlock_error,
            )
        elif logger.hasHandlers():
            logger.warning(
                "Unable to explicitly unlock '%s': %s", lock_path, unlock_error
            )


def release_single(
    lock_name: Union[str, os.PathLike] = "yyds_instance.lock",
    base_dir: Optional[Union[str, os.PathLike]] = None,
    logger: Any = None,
):
    """Release one acquisition owned by the current execution owner."""
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
        self._owner = None
        self._state_lock = threading.Lock()
        self._pid = os.getpid()
        with _registry_guard():
            _live_lock_instances.add(self)

    def _reset_after_fork(self, current_pid):
        self._acquired = False
        self._acquiring = False
        self._owner = None
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
                    self._owner = None
                    self._pid = current_pid

    @property
    def acquired(self):
        self._reset_after_fork_if_needed()
        return self._acquired

    def acquire(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if self._acquired or self._acquiring:
                raise RuntimeError("This SingleInstanceLock object is already acquired")
            self._acquiring = True

        acquired_ok = False
        try:
            force_single(
                self.lock_name,
                block=self.block,
                raise_on_conflict=self.raise_on_conflict,
                base_dir=self.base_dir,
                logger=self.logger,
            )
            acquired_ok = True
        finally:
            with self._state_lock:
                self._acquiring = False
                if acquired_ok:
                    self._owner = _current_owner()
                    self._acquired = True
        return self

    async def acquire_async(self):
        self._reset_after_fork_if_needed()
        with self._state_lock:
            if self._acquired or self._acquiring:
                raise RuntimeError("This SingleInstanceLock object is already acquired")
            self._acquiring = True

        acquired_ok = False
        try:
            if self.block:
                await force_single_async(
                    self.lock_name,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                    base_dir=self.base_dir,
                    logger=self.logger,
                )
            else:
                force_single(
                    self.lock_name,
                    block=False,
                    raise_on_conflict=self.raise_on_conflict,
                    base_dir=self.base_dir,
                    logger=self.logger,
                )
            acquired_ok = True
        finally:
            with self._state_lock:
                self._acquiring = False
                if acquired_ok:
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
            release_single(self.lock_name, self.base_dir, logger=self.logger)
            self._acquired = False
            self._owner = None

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
                    await force_single_async(
                        resolved_lock_name,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        base_dir=base_dir,
                        logger=logger,
                    )
                else:
                    force_single(
                        resolved_lock_name,
                        block=False,
                        raise_on_conflict=raise_on_conflict,
                        base_dir=base_dir,
                        logger=logger,
                    )

                released = [False]

                def release_once():
                    if not released[0]:
                        released[0] = True
                        _release_single_for_owner(
                            resolved_lock_name,
                            owner,
                            base_dir=base_dir,
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
                if block:
                    await force_single_async(
                        resolved_lock_name,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        base_dir=base_dir,
                        logger=logger,
                    )
                else:
                    force_single(
                        resolved_lock_name,
                        block=False,
                        raise_on_conflict=raise_on_conflict,
                        base_dir=base_dir,
                        logger=logger,
                    )
                try:
                    return await func(*args, **kwargs)
                finally:
                    release_single(resolved_lock_name, base_dir, logger=logger)

            return async_wrapper

        if inspect.isgeneratorfunction(func):
            if timeout is not None:
                raise ValueError("timeout is only supported by asynchronous decorators")

            @wraps(func)
            def generator_wrapper(*args, **kwargs):
                resolved_lock_name = resolved_name(args, kwargs)
                owner = _current_owner()
                force_single(
                    resolved_lock_name,
                    block,
                    raise_on_conflict,
                    base_dir,
                    logger=logger,
                )
                try:
                    yield from func(*args, **kwargs)
                finally:
                    _release_single_for_owner(
                        resolved_lock_name,
                        owner,
                        base_dir=base_dir,
                        logger_arg=logger,
                        strict_owner=False,
                    )

            return generator_wrapper

        if timeout is not None:
            raise ValueError("timeout is only supported by asynchronous decorators")

        @wraps(func)
        def wrapper(*args, **kwargs):
            resolved_lock_name = resolved_name(args, kwargs)
            force_single(
                resolved_lock_name,
                block,
                raise_on_conflict,
                base_dir,
                logger=logger,
            )
            try:
                return func(*args, **kwargs)
            finally:
                release_single(resolved_lock_name, base_dir, logger=logger)

        return wrapper

    return decorator


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
        activity_events = list(_path_activity.values())
        waiter_paths = list(_async_waiters.keys())
        _lock_file_handles.clear()
        # Keep every descriptor visible to the fork hook until it is actually
        # closed below.
        _pending_handles.clear()
        _pending_handles.update(all_handles)
        _path_activity.clear()
        _thread_acquisitions.clear()
        for event in activity_events:
            event.set()
        for event in released_events:
            event.set()
        for path in waiter_paths:
            _notify_async_waiters_locked(path)

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
        global _path_activity, _async_waiters, _thread_acquisitions

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
        _async_waiters = {}
        _thread_acquisitions = {}
        # Never acquire or release threading locks inherited from vanished
        # threads.  Fresh objects make the child immediately usable.
        _registry_lock = threading.Lock()
        _fork_gate = threading.RLock()
        current_pid = os.getpid()
        for lock_instance in list(_live_lock_instances):
            lock_instance._reset_after_fork(current_pid)

    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_parent,
        after_in_child=_after_fork_child,
    )
