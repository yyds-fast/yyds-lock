# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[中文文档](README_CN.md)

`yyds-lock` is an ultra-lightweight, zero-dependency Python library that guarantees single-instance execution of scripts/processes using operating system level advisory file locks. It is ideal for cron jobs, automation scripts, schedulers, and background daemons.

## Key Features

- 🛡️ **Immunity to Crashes / Force Kills**: Unlike simple PID files or "lock files" that leave stale markers behind if the script crashes, is killed (`kill -9`), or suffers power loss, `yyds-lock` binds the lock to the process file descriptor. The OS automatically and instantly releases the lock as soon as the process ends.
- 🪶 **Zero Dependencies**: 100% pure Python standard library. The installation size is less than 5KB and does not pollute your environment.
- 🎛️ **Dual Modes**: Supports both "Instant Exit" (non-blocking, terminates immediately if another instance is running) and "Queue / Wait" (blocking, waits for the existing instance to finish).
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

Place this call at the very top of your entrypoint script. If another instance of the script is already running, the new instance will immediately print an error to stderr and exit with status code `1`.

```python
import time
import yyds_lock

# Force single-instance execution.
yyds_lock.force_single(lock_name="my_automation.lock", block=False)

print("Running heavy automation task...")
time.sleep(300)
```

### Pattern B: Decorator (Best for structured functions/main entrypoints)

Decorate your `main` function to enforce mutual exclusion.

```python
import yyds_lock

@yyds_lock.single_decorator(lock_name="my_task.lock", block=False)
def main():
    print("Executing single instance task safely...")

if __name__ == "__main__":
    main()
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

- `lock_name` (str): The filename/path of the lock.
  - If a simple filename is given (e.g. `"my_job.lock"`), it is automatically created in the user's home directory (`~`).
  - If an absolute or relative path is given (e.g., `"/var/run/my_job.lock"`), it is created at that specific path. The parent directories will be created automatically if they do not exist.
- `block` (bool):
  - `False` (default): Exit immediately if the lock cannot be acquired.
  - `True`: Block and queue, waiting for the active process to finish and release the lock.
- `raise_on_conflict` (bool):
  - `False` (default): Immediately print an error and call `sys.exit(1)` when the lock is already held.
  - `True`: Raise `AlreadyLockedError` when the lock is already held, allowing the caller to catch it.

---

## How It Works Under the Hood

1. **Linux / macOS**: Uses `fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)` for exclusive advisory locking.
2. **Windows**: Uses `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` to lock the first byte of the file.
3. The library stores the open file handles in a global dictionary inside the Python runtime. This keeps the file descriptor open and prevents garbage collection (GC) from releasing the lock prematurely.
4. When the process terminates (normally, via Exception, `sys.exit`, crash, `kill -9`, or power failure), the OS closes the file descriptors, releasing the locks instantly.

