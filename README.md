# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[中文文档](README_CN.md)

`yyds-lock` is an industrial-grade, ultra-lightweight, zero-dependency Python library that guarantees single-instance execution of scripts, processes, or threads using operating system level advisory file locks. It is ideal for cron jobs, automation scripts, schedulers, and background daemons.

## Key Features

- 🛡️ **Immunity to Crashes / Force Kills**: Unlike PID files or stale lock files that cause permanent lockups if a process is terminated forcefully (`kill -9`, crash, or power loss), `yyds-lock` binds the lock to the process file descriptor. The OS automatically and instantly releases the lock as soon as the process ends.
- 🪶 **Zero Dependencies**: 100% pure Python standard library. Package size is less than 5KB and does not pollute your runtime environment.
- 🎛️ **Dual Modes**: Supports both "Instant Exit" (non-blocking, terminates immediately if another instance is running) and "Queue / Wait" (blocking, waits for the existing instance to finish).
- 🧵 **Thread-Safety & Isolation**: Safe to use in multi-threaded programs. Different threads running under the same process are isolated and will block or raise conflicts on the same lock.
- 🔱 **Fork-Safety**: Automatically handles Unix process forks (`multiprocessing`, Celery, Gunicorn, etc.) by closing inherited locks in child processes without unlocking the parent.
- 📁 **Inaccessible Directory Fallback**: If the home directory is read-only or does not exist (e.g., in headless Docker containers), the library automatically and safely falls back to the system temporary directory.
- 🧹 **Automatic Cleanup**: Registers an `atexit` cleanup hook to close file descriptors cleanly on interpreter shutdown, preventing python `ResourceWarning`.
- 💻 **Cross-Platform**: Seamlessly works on Linux, macOS (using `fcntl.flock`), and Windows (using `msvcrt.locking`).

---

## Installation

```bash
pip install -U yyds-lock
```

---

## Usage

You can protect your script using any of the following approaches:

### Pattern A: Direct Call (Best for straightforward scripts / entrypoints)

Place this call at the very top of your entrypoint script. If another instance of the script is already running, the new instance will immediately print an error and exit with status code `1`.

```python
import time
import yyds_lock

# Force single-instance execution.
yyds_lock.force_single(lock_name="my_automation.lock", block=False)

print("Running heavy automation task...")
time.sleep(300)
```

### Pattern B: Decorator with Dynamic Lock Names

Decorate your functions to enforce mutual exclusion. The `lock_name` parameter can also be a callable (e.g. lambda function) that dynamically generates the lock name based on function arguments.

```python
import yyds_lock

# 1. Static lock name
@yyds_lock.single_decorator(lock_name="my_task.lock", block=False)
def main():
    print("Executing single instance task safely...")

# 2. Dynamic lock name based on arguments
@yyds_lock.single_decorator(lock_name=lambda job_id: f"job_{job_id}.lock", block=False)
def process_job(job_id):
    print(f"Processing job {job_id} exclusively...")

if __name__ == "__main__":
    main()
    process_job(42)
```

### Pattern C: Handle Lock Conflict (Exception Raising)

If you prefer to handle the locking failure programmatically (e.g., to perform custom cleanups, log warnings, or run fallback logic) instead of immediately terminating the process, set `raise_on_conflict=True` to raise `AlreadyLockedError`:

```python
import yyds_lock
from yyds_lock import AlreadyLockedError

try:
    yyds_lock.force_single(lock_name="my_automation.lock", block=False, raise_on_conflict=True)
except AlreadyLockedError:
    print("Failed to acquire lock. Running fallback script instead...")
    # Add custom fallback actions here
```

---

## Configuration / Arguments

Both `force_single` and `single_decorator` accept the following arguments:

- `lock_name` (str or callable): The filename/path of the lock, or a callable returning a string when using the decorator.
  - If a simple filename is given (e.g. `"my_job.lock"`), it is automatically created in a hidden directory `.yyds_lock` under the user's home directory (`~/.yyds_lock`).
  - If an absolute or relative path is given (e.g., `"/var/run/my_job.lock"`), it is created at that specific path. The parent directories will be created automatically if they do not exist.
- `block` (bool):
  - `False` (default): Exit immediately (or raise) if the lock cannot be acquired.
  - `True`: Block and queue, waiting for the active process/thread to finish and release the lock.
- `raise_on_conflict` (bool):
  - `False` (default): Immediately log an error and call `sys.exit(1)` when the lock is already held.
  - `True`: Raise `AlreadyLockedError` when the lock is already held, allowing the caller to catch it.
- `base_dir` (str, optional): Overrides the default folder directory (`~/.yyds_lock`) where simple filenames are saved.

---

## Logging

`yyds-lock` uses Python's standard `logging` library. All lock conflicts and warning outputs are logged using:
```python
import logging
logger = logging.getLogger("yyds_lock")
```
By default, if you have not configured any handlers for your logging system, `yyds-lock` will automatically print user-friendly colored error messages to `sys.stderr` to maintain simplicity for basic scripts.

---

## How It Works Under the Hood

1. **Linux / macOS**: Uses `fcntl.flock(fd, fcntl.LOCK_EX)` for exclusive advisory locking.
2. **Windows**: Uses `msvcrt.locking(fd, msvcrt.LK_LOCK, 1)` to lock the first byte of the file.
3. **Thread Safety**: Uses a thread-safe global registry with reentrancy checks mapped to `threading.get_ident()`. File descriptor locking calls are executed outside the global lock, preventing deadlocks when threads block and wait.
4. **Fork-Safety**: Automatically tracks forks and closes open descriptors in child processes post-fork (via `os.register_at_fork`).
5. **Clean Reclamation**: Locks are released when:
   - An explicit `release_single` call is executed.
   - The decorated function finishes execution.
   - Python exit handlers run (`atexit`).
   - The process terminates or is killed, prompting the operating system to reclaim all file descriptors and release the locks.
