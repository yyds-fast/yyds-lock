# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[English](README.md)

`yyds-lock` 是一个**工业级**、极轻量级、**零依赖（Zero-dependency）** 的 Python 进程与线程单例锁工具库。它通过操作系统底层的建议性文件锁（Advisory File Lock）来保证同一时间只有一个脚本实例/进程/线程处于运行状态。非常适合定时任务（Crontab）、自动化脚本、抢票爬虫、后台守护服务以及多线程任务排队。

## 核心特性

- 🛡️ **防崩溃死锁（Immunity to Crashes）**：不同于传统的 PID 文件或创建临时文件（如 `lock.txt`）的方案，如果脚本遇到 `kill -9` 强杀、机房突然断电或代码崩溃，残留的文件会导致下次启动永久死锁。`yyds-lock` 锁是绑定在操作系统的文件描述符及进程上的，当进程结束的瞬间，操作系统会自动释放该锁，**绝无僵尸死锁隐患**。
- 🪶 **零第三方依赖（Zero Dependencies）**：100% 纯 Python 标准库实现。打包后大小不到 5KB，对系统和运行环境零污染。
- 🎛️ **双模式切换（Dual Modes）**：既支持发现重复进程后“优雅秒退”模式（非阻塞，打印红色错误并以退出码 `1` 退出），也支持“排队等候”模式（阻塞等待前一个实例运行结束）。
- 🧵 **线程安全与隔离**：原生支持多线程环境。同一进程内的不同线程可以被正确互斥隔离，分别排队或触发冲突退出，重入逻辑仅针对同一线程生效。
- 🔱 **进程派生安全（Fork-Safety）**：自动兼容 Unix 上的进程 fork 机制（如 `multiprocessing`、Celery、Gunicorn 等），防止子进程继承父进程句柄导致的文件锁泄漏及父进程被意外解锁。
- 📁 **只读目录自动降级**：如果用户家目录不可写或不存在（例如 headless Docker 容器中），库会自动、安全地降级到系统临时目录。
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

---

## 配置参数说明

`force_single` 和 `single_decorator` 均接受以下参数：

- `lock_name` (str 或 callable)：锁文件的名称/路径，或者在装饰器模式下传入的动态生成锁名的 Callable。
  - 如果仅传入文件名（如 `"my_job.lock"`），锁文件会自动创建在用户家目录的隐藏文件夹 `~/.yyds_lock` 下（若不存在或不可写，会自动安全降级至系统临时目录）。
  - 如果传入的是相对路径或绝对路径（如 `"/var/run/my_job.lock"`），则会在对应路径创建。如果父级目录不存在，会自动创建。
- `block` (bool)：
  - `False`（默认值）：非阻塞模式。如果发现锁已被占用，立即引发冲突处理。
  - `True`：阻塞排队模式。如果发现锁已被占用，当前实例会处于挂起等待状态，直到上一个实例结束并释放锁后，才接棒继续运行。
- `raise_on_conflict` (bool)：
  - `False`（默认值）：锁被占用时，直接向日志系统或 stderr 输出错误并以退出码 `1` 退出进程。
  - `True`：锁被占用时，抛出 `AlreadyLockedError` 异常，允许调用者捕获并进行自定义处理。
- `base_dir` (str, 可选)：手动指定锁文件存放的目录，用于覆盖默认的 `~/.yyds_lock`。

---

## 企业级日志系统集成

`yyds-lock` 集成了 Python 标准的 `logging` 日志系统。所有锁冲突和运行警告均通过以下 Logger 输出：
```python
import logging
logger = logging.getLogger("yyds_lock")
```
如果您的项目未配置任何 Log Handler，为了保证轻量化脚本的易用性，本库会自动向 `sys.stderr` 打印彩色的控制台警告信息。

---

## 技术原理

1. **Linux / macOS**：底层调用 `fcntl.flock(fd, fcntl.LOCK_EX)` 对打开的文件加上排他性锁定。
2. **Windows**：底层调用 `msvcrt.locking(fd, msvcrt.LK_LOCK, 1)` 对文件的首字节加锁。
3. **线程级排他与防死锁**：在内存中使用基于 `threading.get_ident()` 线程 ID 的全局注册表来跟踪重入状态。操作系统级别的文件锁获取与释放逻辑均在 Python 级别全局互斥锁之外运行，彻底避免了并发排队时的多线程死锁。
4. **派生安全 (Fork-Safety)**：在 Unix 平台上通过 `os.register_at_fork` 注册钩子，子进程启动后自动关闭继承的锁文件描述符而不触发 `LOCK_UN`，防止影响父进程的锁定状态。
5. **垃圾回收与自动清理**：在内存中保持对文件句柄的引用以防止 Python GC 提早关闭文件。以下情况会释放文件锁：
   - 显式调用 `release_single`。
   - 装饰器函数执行结束。
   - 解释器退出阶段的 `atexit` 清理钩子被调用。
   - 进程退出/被杀死/断电关机，操作系统内核自动回收进程占用的所有文件描述符，此时文件锁将被**瞬间强行释放**。
