# -*- coding:utf-8 -*-

import os
import sys
import platform
import threading
from functools import wraps

# Global registry for active locks: maps absolute lock file paths to info dicts:
# {
#     "handle": file_object,
#     "ref_count": int
# }
_lock_file_handles = {}
_global_lock = threading.Lock()

def _resolve_lock_path(lock_name: str) -> str:
    """
    Resolves the lock name to an absolute canonical path.
    """
    if os.path.isabs(lock_name) or "/" in lock_name or "\\" in lock_name:
        return os.path.abspath(lock_name)
    else:
        return os.path.abspath(os.path.join(os.path.expanduser("~"), lock_name))

def force_single(lock_name: str = "yyds_instance.lock", block: bool = False):
    """
    Enforces that only a single instance of the script/process runs.
    
    :param lock_name: Name of the lock file, created in the user's home directory.
                      Can also be a relative or absolute path.
    :param block: If True, blocks and queues until the lock is available.
                  If False, instantly prints an error message to stderr and exits with code 1.
    """
    global _lock_file_handles
    
    lock_path = _resolve_lock_path(lock_name)
    
    with _global_lock:
        # If the current process already holds this lock, increment reference count and return safely
        if lock_path in _lock_file_handles:
            _lock_file_handles[lock_path]["ref_count"] += 1
            return
            
    # Ensure parent directory exists
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)
        
    # Open the lock file in append/read-write mode
    handle = open(lock_path, "a+")
    sys_type = platform.system().lower()
    
    if sys_type == "windows":
        import msvcrt
        mode = msvcrt.LK_LOCK if block else msvcrt.LK_NBLCK
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), mode, 1)
        except (IOError, OSError):
            handle.close()
            print(f"\033[31m[yyds-lock] 错误: 脚本进程已在运行中，当前实例自动退出！\033[0m", file=sys.stderr)
            sys.exit(1)
            
    else:  # Linux / macOS / Unix
        import fcntl
        mode = fcntl.LOCK_EX
        if not block:
            mode |= fcntl.LOCK_NB
            
        try:
            fcntl.flock(handle.fileno(), mode)
        except (IOError, OSError):
            handle.close()
            print(f"\033[31m[yyds-lock] 错误: 脚本进程已在运行中，当前实例自动退出！\033[0m", file=sys.stderr)
            sys.exit(1)
            
    with _global_lock:
        # Double check to prevent rare race condition
        if lock_path in _lock_file_handles:
            # Another thread acquired it in the meantime (highly unlikely with _global_lock, but safe fallback)
            _lock_file_handles[lock_path]["ref_count"] += 1
            # Release our newly acquired OS lock as we already have one
            try:
                if sys_type == "windows":
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except (IOError, OSError):
                pass
            handle.close()
        else:
            _lock_file_handles[lock_path] = {
                "handle": handle,
                "ref_count": 1
            }

def release_single(lock_name: str = "yyds_instance.lock"):
    """
    Manually releases a lock if it was acquired by the current process.
    """
    global _lock_file_handles
    
    lock_path = _resolve_lock_path(lock_name)
    handle_to_close = None
    
    with _global_lock:
        lock_info = _lock_file_handles.get(lock_path)
        if not lock_info:
            return
            
        lock_info["ref_count"] -= 1
        if lock_info["ref_count"] > 0:
            return
            
        # Ref count reached 0, proceed to release lock
        handle_to_close = lock_info["handle"]
        _lock_file_handles.pop(lock_path, None)
        
    if handle_to_close:
        sys_type = platform.system().lower()
        try:
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

def single_decorator(lock_name: str = "yyds_instance.lock", block: bool = False):
    """
    Decorator syntax sugar for running a function as a single instance.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            force_single(lock_name, block)
            try:
                return func(*args, **kwargs)
            finally:
                release_single(lock_name)
        return wrapper
    return decorator
