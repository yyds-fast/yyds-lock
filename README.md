# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[中文文档](README_CN.md)

`yyds-lock` is a lightweight, zero-dependency Python library for single-instance execution of cooperating scripts, processes, threads, and asyncio tasks using operating-system advisory file locks. It is designed for cron jobs, automation scripts, schedulers, and local background daemons.

## Key Features

- 🛡️ **Immunity to Crashes / Force Kills**: Unlike PID files or stale lock files that cause permanent lockups if a process is terminated forcefully (`kill -9`, crash, or power loss), `yyds-lock` binds the lock to the process file descriptor. The OS automatically and instantly releases the lock as soon as the process ends.
- 🪶 **Zero Dependencies**: 100% Python standard library with no runtime packages to install.
- 🎛️ **Dual Modes**: Supports both "Instant Exit" (non-blocking, terminates immediately if another instance is running) and "Queue / Wait" (blocking, waits for the existing instance to finish).
- 🧵 **Thread-Safety & Isolation**: Safe to use in multi-threaded programs. Different threads running under the same process are isolated and will block or raise conflicts on the same lock.
- ⚡ **Async-Safe Waiting**: `force_single_async` and asynchronous decorators wait without blocking the event loop and support finite timeouts.
- 🔱 **Fork-Safety**: Coordinates registry mutations with Unix `fork()` and closes both acquired and in-flight inherited descriptors in the child without unlocking the parent.
- 📁 **Inaccessible Directory Fallback**: If the home directory is read-only or unavailable, bare lock names fall back to a private per-user directory under the system temporary directory. An explicit `base_dir` never silently moves.
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
    yyds_lock.force_single(
        lock_name="my_automation.lock", block=False, raise_on_conflict=True
    )
except AlreadyLockedError:
    print("Failed to acquire lock. Running fallback script instead...")
    # Add custom fallback actions here
```

Operational failures such as permission errors or unsupported filesystem locking raise `LockOperationError` rather than being misreported as lock contention.

### Pattern D: Context Manager

Use `SingleInstanceLock` when the lock should have an explicit lexical lifetime (supports both `with` and `async with`):

```python
from yyds_lock import SingleInstanceLock

with SingleInstanceLock("my_task.lock", raise_on_conflict=True):
    run_task()

# In async code, block=True and timeout can be used with async with:
# async with SingleInstanceLock("my_task.lock", block=True, timeout=10):
#     await run_async_task()
```

The decorator also preserves the lock for the full lifetime of coroutine, generator, and async-generator functions. Asynchronous decorators safely support `block=True` without blocking the event loop.

### Pattern E: Async Waiting with a Timeout

Use `force_single_async` directly from the Task that will eventually release the lock:

```python
from yyds_lock import LockTimeoutError, force_single_async, release_single


async def run_job():
    try:
        await force_single_async("my_async_job.lock", timeout=10)
    except LockTimeoutError:
        return

    try:
        await do_work()
    finally:
        release_single("my_async_job.lock")
```

Do not acquire in an executor and release from the event-loop Task: lock ownership belongs to the Thread or Task that performed the acquisition. If an executor is required, the complete acquire/work/release scope must run in one executor call.

---

## Configuration / Arguments

`force_single` and `single_decorator` share the following core arguments:

- `lock_name` (str or `os.PathLike`): The filename/path of the lock. `single_decorator` additionally accepts a callable that returns the lock name from the decorated function's arguments.
  - If a simple filename is given (e.g. `"my_job.lock"`), it is automatically created in a hidden directory `.yyds_lock` under the user's home directory (`~/.yyds_lock`).
  - If an absolute or relative path is given (e.g., `"/var/run/my_job.lock"`), it is created at that specific path. The parent directories will be created automatically if they do not exist.
- `block` (bool):
  - `False` (default): Exit immediately (or raise) if the lock cannot be acquired.
  - `True`: Block and queue, waiting for the active process/thread to finish and release the lock.
- `raise_on_conflict` (bool):
  - `False` (default): Immediately log an error and call `sys.exit(1)` when the lock is already held.
  - `True`: Raise `AlreadyLockedError` when the lock is already held, allowing the caller to catch it.
- `base_dir` (str or `os.PathLike`, optional): Overrides the default base folder (`~/.yyds_lock`) for simple filenames and relative paths. Cannot be combined with an absolute `lock_name`; failure to use an explicit directory raises `LockOperationError` without silently falling back.
- `logger` (optional): Pass an already initialized Logger instance (such as `yyds-logger`, standard library `logging.Logger`, `loguru`, or a custom wrapper). When a conflict or error occurs, messages are emitted directly to this logger.

`force_single_async` accepts `lock_name`, `base_dir`, `logger`, plus:

- `timeout` (float or `None`): Maximum wait in seconds. `None` waits indefinitely; expiration raises `LockTimeoutError`.
- `poll_interval` (float): Delay between non-blocking attempts, defaulting to `0.05` seconds.

For coroutine and async-generator decorators, `timeout` and `poll_interval` configure async waiting when `block=True`.

---

## Logging

`yyds-lock` supports two ways of emitting conflict and warning messages:

### 1. Passing a Custom Logger (Recommended)
Pass an existing logger instance to `force_single`, `SingleInstanceLock`, or `@single_decorator`:

```python
from yyds_lock import force_single
from yyds_logger import Logger  # or loguru, standard logging.Logger

my_logger = Logger("./logs/my_spider.log", "debug").logger

# Conflict error will be written directly to my_logger upon exit
force_single(lock_name="my_spider.lock", block=False, logger=my_logger)
```

### 2. Default `yyds_lock` Logger
If no custom logger is passed, `yyds-lock` uses the standard library logger named `"yyds_lock"`:
```python
import logging

logger = logging.getLogger("yyds_lock")
```
If an effective handler is configured, conflicts are emitted once through logging. Otherwise, `yyds-lock` prints one message to `sys.stderr` (colored only when stderr is a terminal).

---

## How It Works Under the Hood

1. **Linux / macOS**: Uses `fcntl.flock(fd, fcntl.LOCK_EX)` for exclusive advisory locking.
2. **Windows**: Uses interruptible polling around `msvcrt.LK_NBLCK` to lock the first byte, giving `block=True` true wait-until-available behavior instead of the CRT's bounded retry window.
3. **Thread / Task Safety**: Tracks the owning live `Thread` or asyncio `Task` object, supports same-owner reentrancy, and serializes local path transitions before calling the OS lock.
4. **Fork-Safety**: Uses all three `os.register_at_fork` phases. A fork gate snapshots registry mutations, and the child replaces inherited synchronization primitives after closing acquired and in-flight descriptors.
5. **Clean Reclamation**: Locks are released when:
   - An explicit `release_single` call is executed.
   - The decorated function finishes execution.
   - Python exit handlers run (`atexit`).
   - The process terminates or is killed, prompting the operating system to reclaim all file descriptors and release the locks.

## Scope and Limitations

- Locks are advisory: every participant must cooperate by locking the same canonical file path.
- Mutual exclusion is local to processes that share the same underlying filesystem. Separate containers, hosts, or non-shared temporary directories require a distributed lock instead.
- Network filesystems may implement `flock` differently; validate the target filesystem before relying on it for critical coordination.
- To avoid cross-process `flock` unlink/inode races, lock files are intentionally not deleted on release; if you use high-cardinality dynamic lock names (e.g., per-request UUIDs), periodically prune unused files or bound lock-name cardinality.
- Async lock ownership is bound to the current `asyncio.Task`; calling APIs that spawn a child `Task` (such as `asyncio.wait_for(...)`) to acquire the same lock inside an already-locked coroutine treats the child task as a separate contender.
- `sys.exit(1)` raises `SystemExit` in the calling execution context. In libraries and worker threads, `raise_on_conflict=True` is usually the safer integration mode.
