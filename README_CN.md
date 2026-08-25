# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[English](README.md)

`yyds-lock` 是一个轻量级、零运行时依赖的 Python 单实例锁工具库。它通过操作系统建议性文件锁（Advisory File Lock），协调使用同一路径的脚本、进程、线程和 asyncio Task，适合定时任务（Crontab）、自动化脚本、调度器以及单机后台服务。

## 核心特性

- 🛡️ **防崩溃死锁（Immunity to Crashes）**：不同于传统的 PID 文件或创建临时文件（如 `lock.txt`）的方案，如果脚本遇到 `kill -9` 强杀、机房突然断电或代码崩溃，残留的文件会导致下次启动永久死锁。`yyds-lock` 锁是绑定在操作系统的文件描述符及进程上的，当进程结束的瞬间，操作系统会自动释放该锁，**绝无僵尸死锁隐患**。
- 🪶 **零第三方依赖（Zero Dependencies）**：100% Python 标准库实现，不安装任何运行时依赖包。
- 🎛️ **双模式切换（Dual Modes）**：既支持发现重复进程后“优雅秒退”模式（非阻塞，打印红色错误并以退出码 `1` 退出），也支持“排队等候”模式（阻塞等待前一个实例运行结束）。
- 🧵 **线程与任务隔离**：同一进程内的不同线程或 asyncio Task 可以正确互斥，分别等待或触发冲突；重入仅对同一个仍存活的执行所有者生效。
- ⚡ **异步安全等待**：`force_single_async` 和异步装饰器不会阻塞事件循环，并支持有限超时。
- 🔱 **进程派生安全（Fork-Safety）**：在 Unix `fork()` 前同步注册表变更；子进程关闭已获取及获取中的继承句柄，并重建线程同步对象，不会解锁父进程。
- 📁 **只读目录自动降级**：家目录不可写或不存在时，裸文件名会降级到系统临时目录下的当前用户私有目录；显式 `base_dir` 失败时不会静默换路径。
- 🧹 **自动资源清理**：注册 `atexit` 清理钩子，在 Python 解释器正常退出时自动关闭所有打开的文件锁句柄，彻底消除 `ResourceWarning` 警告。
- 💻 **跨平台兼容**：在 Windows 上使用 `msvcrt.locking`，在 Linux 和 macOS 上使用 `fcntl.flock`。

---

## 安装方法

```bash
pip install -U yyds-lock
```

---

## 使用姿势

您可以通过以下几种简单的方式保护您的脚本：

### 姿势 A：直接调用（适合平铺直叙的单兵脚本）

在脚本最开头调用即可。如果发现已有实例在运行，当前实例会自动输出错误到 stderr 并以退出码 `1` 退出。

```python
import time
import yyds_lock

# 强制脚本单实例运行（发现重复则优雅秒退）
yyds_lock.force_single(lock_name="my_automation.lock", block=False)

print("脚本开始执行耗时任务...")
time.sleep(300)
```

### 姿势 B：装饰器与动态锁名（适合有主函数的规范项目）

使用装饰器来声明某个主函数在同一时间内只能运行一个实例。`lock_name` 可以传入一个 Callable（如 `lambda` 函数），用于根据函数的入参动态生成锁文件名。

```python
import yyds_lock

# 1. 静态锁文件名
@yyds_lock.single_decorator(lock_name="my_task.lock", block=False)
def main():
    print("安全运行中，绝无重复启动隐患...")

# 2. 动态锁文件名（根据参数动态互斥）
@yyds_lock.single_decorator(lock_name=lambda user_id: f"user_{user_id}.lock", block=False)
def process_user(user_id):
    print(f"正在排他性地处理用户 {user_id} 的数据...")

if __name__ == "__main__":
    main()
    process_user(1001)
```

### 姿势 C：处理锁冲突（抛出异常模式）

如果您不希望直接退出进程，而是希望通过代码捕捉锁冲突（例如执行自定义清理逻辑、打印日志告警、或者执行降级任务），可以设置 `raise_on_conflict=True` 使得在发生冲突时抛出 `AlreadyLockedError` 异常：

```python
import yyds_lock
from yyds_lock import AlreadyLockedError

try:
    yyds_lock.force_single(lock_name="my_automation.lock", block=False, raise_on_conflict=True)
except AlreadyLockedError:
    print("未能成功获取锁，正在执行降级脚本...")
    # 在这里添加自定义的降级/备份逻辑
```

权限错误、文件系统不支持锁等运行故障会抛出 `LockOperationError`，不会再伪装成锁竞争。

### 姿势 D：上下文管理器

需要明确控制锁的代码作用域时，可以使用 `SingleInstanceLock`：

```python
from yyds_lock import SingleInstanceLock

with SingleInstanceLock("my_task.lock", raise_on_conflict=True):
    run_task()
```

装饰器也会在 coroutine、generator 和 async generator 的完整执行周期内持锁；异步装饰器可以安全使用 `block=True`，不会阻塞事件循环。

### 姿势 E：带超时的异步等待

应当在最终负责释放锁的同一个 Task 中调用 `force_single_async`：

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

不要在 executor 中获取后回到事件循环 Task 释放：锁归属于实际执行获取操作的 Thread 或 Task。若必须使用 executor，获取、业务执行和释放必须放在同一次 executor 调用中完成。

---

## 配置参数说明

`force_single` 和 `single_decorator` 共享以下核心参数：

- `lock_name` (str 或 `os.PathLike`)：锁文件的名称/路径。只有 `single_decorator` 额外支持 Callable，可根据被装饰函数的参数动态生成锁名。
  - 如果仅传入文件名（如 `"my_job.lock"`），锁文件会自动创建在用户家目录的隐藏文件夹 `~/.yyds_lock` 下（若不存在或不可写，会自动安全降级至系统临时目录）。
  - 如果传入的是相对路径或绝对路径（如 `"/var/run/my_job.lock"`），则会在对应路径创建。如果父级目录不存在，会自动创建。
- `block` (bool)：
  - `False`（默认值）：非阻塞模式。如果发现锁已被占用，立即引发冲突处理。
  - `True`：阻塞排队模式。如果发现锁已被占用，当前实例会处于挂起等待状态，直到上一个实例结束并释放锁后，才接棒继续运行。
- `raise_on_conflict` (bool)：
  - `False`（默认值）：锁被占用时，直接向日志系统或 stderr 输出错误并以退出码 `1` 退出进程。
  - `True`：锁被占用时，抛出 `AlreadyLockedError` 异常，允许调用者捕获并进行自定义处理。
- `base_dir` (str 或 `os.PathLike`, 可选)：覆盖裸文件名默认使用的 `~/.yyds_lock`。显式目录不可用时抛出 `LockOperationError`，不会静默降级。
- `logger` (可选)：传入已初始化好的 Logger 对象（如 `yyds-logger`、标准库 `logging.Logger`、`loguru` 或自定义包装器）。当发生冲突或错误时，直接通过该 logger 记录日志。

`force_single_async` 接受 `lock_name`、`base_dir`、`logger`，以及：

- `timeout` (float 或 `None`)：最长等待秒数。`None` 表示无限等待；超时会抛出 `LockTimeoutError`。
- `poll_interval` (float)：非阻塞重试间隔，默认 `0.05` 秒。

对于 coroutine 和 async-generator 装饰器，`block=True` 时可通过 `timeout` 和 `poll_interval` 配置异步等待。

---

## 企业级日志系统集成

`yyds-lock` 支持两种方式输出冲突与告警日志：

### 1. 传入已初始化的自定义 Logger（推荐，如 yyds-logger）
直接将已有 logger 实例作为参数传入 `force_single`、`SingleInstanceLock` 或 `@single_decorator` 中，锁冲突时会自动记录到对应的日志文件：

```python
from yyds_lock import force_single
from yyds_logger import Logger  # 或自定义 logger / loguru

my_logger = Logger('./logs/my_spider.log', 'debug').logger

# 锁发生冲突时，将自动通过 my_logger.error(...) 写入日志文件并退出
force_single(lock_name="my_spider.lock", block=False, logger=my_logger)
```

### 2. 标准库 `yyds_lock` 全局 Logger
若未传入自定义 logger，`yyds-lock` 默认使用名为 `"yyds_lock"` 的标准库 Logger：
```python
import logging
logger = logging.getLogger("yyds_lock")
```
配置了有效 Handler 时，冲突只通过日志输出一次；没有 Handler 时，只向 `sys.stderr` 输出一次（仅在终端环境使用颜色）。

---

## 技术原理

1. **Linux / macOS**：底层调用 `fcntl.flock(fd, fcntl.LOCK_EX)` 对打开的文件加上排他性锁定。
2. **Windows**：通过可中断轮询 `msvcrt.LK_NBLCK` 锁定首字节，使 `block=True` 真正等待到锁可用，不受 CRT 固定重试次数限制。
3. **线程与任务隔离**：以仍存活的 `Thread` 或 asyncio `Task` 对象作为所有者，支持同一所有者重入，并在调用 OS 锁前串行化进程内的同路径状态迁移。
4. **派生安全 (Fork-Safety)**：使用 `os.register_at_fork` 的三个阶段；fork gate 同步注册表变更，子进程关闭已获取及获取中的句柄后重建同步对象。
5. **垃圾回收与自动清理**：在内存中保持对文件句柄的引用以防止 Python GC 提早关闭文件。以下情况会释放文件锁：
   - 显式调用 `release_single`。
   - 装饰器函数执行结束。
   - 解释器退出阶段的 `atexit` 清理钩子被调用。
   - 进程退出/被杀死/断电关机，操作系统内核自动回收进程占用的所有文件描述符，此时文件锁将被**瞬间强行释放**。

## 适用边界

- 文件锁是建议性的，所有参与者必须使用同一规范化路径并主动配合加锁。
- 互斥范围仅覆盖共享同一底层文件系统的进程。不同主机、文件系统隔离的容器或各自的临时目录需要分布式锁。
- 网络文件系统对 `flock` 的实现可能不同，关键任务上线前应验证实际挂载环境。
- `sys.exit(1)` 本质是在调用上下文抛出 `SystemExit`；在库代码和工作线程中通常更适合设置 `raise_on_conflict=True`。
