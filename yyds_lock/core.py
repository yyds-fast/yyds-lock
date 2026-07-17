# -*- coding:utf-8 -*-

import os
import sys
import platform
import threading
import logging
import atexit
import tempfile
from functools import wraps

logger = logging.getLogger("yyds_lock")

class YYDSLockError(Exception):
    """Base exception for yyds-lock."""
    pass

class AlreadyLockedError(YYDSLockError):
    """Raised when a lock cannot be acquired because it is already held."""
    pass

# Global registry locks and handles:
# _registry_lock protects access to _lock_file_handles
_registry_lock = threading.Lock()

# _lock_file_handles maps absolute canonical lock file paths to info dicts:
# {
#     "handle": file_object,
#     "owner_thread_id": int,
#     "ref_count": int
# }
_lock_file_handles = {}

def _resolve_lock_path(lock_name: str, base_dir: str = None) -> str:
    """
    Resolves the lock name to an absolute canonical path.
    If the determined directory is not writable, falls back to the system temp directory.
    """
    if os.path.isabs(lock_name) or "/" in lock_name or "\\" in lock_name:
        path = os.path.abspath(lock_name)
    else:
        if base_dir:
            target_dir = os.path.abspath(base_dir)
        else:
            try:
                home = os.path.expanduser("~")
                if home and os.path.isdir(home) and os.access(home, os.W_OK):
                    target_dir = os.path.join(home, ".yyds_lock")
                else:
                    target_dir = os.path.join(tempfile.gettempdir(), "yyds_lock")
            except Exception:
                target_dir = os.path.join(tempfile.gettempdir(), "yyds_lock")

        # Verify target directory is writable, fallback to tempdir if not
        try:
            os.makedirs(target_dir, exist_ok=True)
            # Try to test write permissions on the directory with a thread-unique file
            test_file = os.path.join(target_dir, f".yyds_lock_write_test_{os.getpid()}_{threading.get_ident()}")
            with open(test_file, "w") as f:
                f.write("")
            os.remove(test_file)
        except Exception:
            target_dir = os.path.join(tempfile.gettempdir(), "yyds_lock")
            try:
                os.makedirs(target_dir, exist_ok=True)
            except Exception:
                pass

        path = os.path.abspath(os.path.join(target_dir, lock_name))
    return os.path.realpath(path)

def force_single(
    lock_name: str = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: str = None
):
    """
    Enforces that only a single instance of the script/process/thread runs.
    
    :param lock_name: Name of the lock file, or a relative/absolute path.
    :param block: If True, blocks and queues until the lock is available.
    :param raise_on_conflict: If True, raises AlreadyLockedError instead of exiting.
    :param base_dir: Optional base directory to override default path.
    """
    lock_path = _resolve_lock_path(lock_name, base_dir)
    current_thread = threading.get_ident()
    
    # 1. Thread-safe registry check for existing lock within the same process
    with _registry_lock:
        if lock_path in _lock_file_handles:
            info = _lock_file_handles[lock_path]
            if info["owner_thread_id"] == current_thread:
                # Reentrancy: same thread increments reference count
                info["ref_count"] += 1
                return
            else:
                # Conflict: Another thread in the same process holds the lock
                if not block:
                    if raise_on_conflict:
                        raise AlreadyLockedError(
                            f"Lock '{lock_name}' is already acquired by thread {info['owner_thread_id']} in this process."
                        )
                    else:
                        msg = f"[yyds-lock] 错误: 脚本进程已在运行中，当前实例自动退出！"
                        logger.error(msg)
                        if not logger.handlers and not logging.getLogger().handlers:
                            print(f"\033[31m{msg}\033[0m", file=sys.stderr)
                        sys.exit(1)
                # If block=True, we release the _registry_lock and block at OS level below.

    # 2. OS-level lock acquisition.
    # Note: We do this OUTSIDE the global _registry_lock to avoid blocking other threads.
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        try:
            os.makedirs(lock_dir, exist_ok=True)
        except Exception:
            # Final fallback to temp dir if os.makedirs fails
            if not os.path.isabs(lock_name) and "/" not in lock_name and "\\" not in lock_name:
                temp_dir = os.path.join(tempfile.gettempdir(), "yyds_lock")
                lock_path = os.path.realpath(os.path.abspath(os.path.join(temp_dir, lock_name)))
                lock_dir = os.path.dirname(lock_path)
                try:
                    os.makedirs(lock_dir, exist_ok=True)
                except Exception:
                    pass
            else:
                raise

    handle = open(lock_path, "a+")
    sys_type = platform.system().lower()
    acquired = False
    
    try:
        if sys_type == "windows":
            import msvcrt
            mode = msvcrt.LK_LOCK if block else msvcrt.LK_NBLCK
            handle.seek(0)
            msvcrt.locking(handle.fileno(), mode, 1)
        else:
            import fcntl
            mode = fcntl.LOCK_EX
            if not block:
                mode |= fcntl.LOCK_NB
            fcntl.flock(handle.fileno(), mode)
        acquired = True
    except (IOError, OSError) as e:
        if raise_on_conflict:
            raise AlreadyLockedError(f"Lock '{lock_name}' is already acquired by another process.") from e
        else:
            msg = f"[yyds-lock] 错误: 脚本进程已在运行中，当前实例自动退出！"
            logger.error(msg)
            if not logger.handlers and not logging.getLogger().handlers:
                print(f"\033[31m{msg}\033[0m", file=sys.stderr)
            sys.exit(1)
    finally:
        if not acquired:
            handle.close()

    # 3. Register successfully acquired OS lock in the registry
    with _registry_lock:
        # Check if the registry was updated by another thread while we were blocking (should be impossible)
        _lock_file_handles[lock_path] = {
            "handle": handle,
            "owner_thread_id": current_thread,
            "ref_count": 1
        }

def release_single(lock_name: str = "yyds_instance.lock", base_dir: str = None):
    """
    Manually releases a lock if it was acquired by the current thread.
    """
    lock_path = _resolve_lock_path(lock_name, base_dir)
    current_thread = threading.get_ident()
    handle_to_close = None
    
    with _registry_lock:
        if lock_path not in _lock_file_handles:
            return
        
        info = _lock_file_handles[lock_path]
        if info["owner_thread_id"] != current_thread:
            raise RuntimeError(
                f"Cannot release lock '{lock_name}' owned by thread {info['owner_thread_id']} from thread {current_thread}."
            )
            
        info["ref_count"] -= 1
        if info["ref_count"] > 0:
            return
            
        # Ref count is 0, pop from registry and prepare to close
        _lock_file_handles.pop(lock_path)
        handle_to_close = info["handle"]
        
    # Close outside the registry lock to avoid holding it during OS call
    if handle_to_close:
        try:
            sys_type = platform.system().lower()
            if sys_type == "windows":
                import msvcrt
                handle_to_close.seek(0)
                msvcrt.locking(handle_to_close.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle_to_close.fileno(), fcntl.LOCK_UN)
        except (IOError, OSError):
            pass
        finally:
            handle_to_close.close()

def single_decorator(
    lock_name: str = "yyds_instance.lock",
    block: bool = False,
    raise_on_conflict: bool = False,
    base_dir: str = None
):
    """
    Decorator syntax sugar for running a function as a single instance.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Resolve dynamic lock name if callable
            resolved_lock_name = lock_name(*args, **kwargs) if callable(lock_name) else lock_name
            
            force_single(resolved_lock_name, block, raise_on_conflict, base_dir)
            try:
                return func(*args, **kwargs)
            finally:
                release_single(resolved_lock_name, base_dir)
        return wrapper
    return decorator

# Atexit cleanup hook
def _cleanup_all():
    with _registry_lock:
        lock_paths = list(_lock_file_handles.keys())
        for lock_path in lock_paths:
            info = _lock_file_handles.pop(lock_path, None)
            if info:
                handle = info["handle"]
                try:
                    sys_type = platform.system().lower()
                    if sys_type == "windows":
                        import msvcrt
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
                finally:
                    try:
                        handle.close()
                    except Exception:
                        pass

atexit.register(_cleanup_all)

# Fork safety hook
if hasattr(os, "register_at_fork"):
    def _child_after_fork():
        # In child process after fork, close parent handles but do NOT call LOCK_UN
        # so that the lock remains held by the parent process.
        global _lock_file_handles
        with _registry_lock:
            for info in _lock_file_handles.values():
                try:
                    info["handle"].close()
                except Exception:
                    pass
            _lock_file_handles.clear()

    os.register_at_fork(after_in_child=_child_after_fork)
