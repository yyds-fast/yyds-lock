# yyds-lock

[![PyPI version](https://img.shields.io/pypi/v/yyds-lock.svg)](https://pypi.org/project/yyds-lock/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`yyds-lock` 是一个极其轻量级、**零依赖（Zero-dependency）** 的 Python 进程单例锁工具库。它通过操作系统底层的建议性文件锁（Advisory File Lock）来保证同一时间只有一个脚本实例处于运行状态。非常适合定时任务（Crontab）、自动化脚本、抢票爬虫以及各种后台服务进程。

## 核心特性

- 🛡️ **防崩溃死锁（Immunity to Crashes）**：不同于传统的 PID 文件或创建临时文件（如 `lock.txt`）的方案，如果脚本遇到 `kill -9` 强杀、机房突然断电或代码奔溃，残留的文件会导致下次启动永久死锁。`yyds-lock` 锁是绑定在操作系统的文件描述符及进程上的，当进程结束的瞬间，操作系统会自动释放该锁，**绝无僵尸死锁隐患**。
- 🪶 **零第三方依赖（Zero Dependencies）**：100% 纯 Python 标准库实现。打包后包大小不到 5KB，对系统和环境零污染。
- 🎛️ **双模式切换（Dual Modes）**：既支持发现重复进程后“优雅秒退”模式（非阻塞，打印红色错误并以退出码 `1` 退出），也支持“排队等候”模式（阻塞等待前一个实例运行结束）。
- 💻 **跨平台兼容**：在 Windows 上使用 `msvcrt.locking`，在 Linux 和 macOS 上使用 `fcntl.flock`。

---

## 安装方法

```bash
pip install -U yyds-lock
```

---

## 使用姿势

您可以通过以下两种极其简单的方式保护您的脚本：

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

### 姿势 B：装饰器模式（适合有 main 函数的规范项目）

使用装饰器来声明某个主函数在同一时间内只能运行一个实例。

```python
import yyds_lock

@yyds_lock.single_decorator(lock_name="my_task.lock", block=False)
def main():
    print("安全运行中，绝无重复启动隐患...")

if __name__ == "__main__":
    main()
```

---

## 配置参数说明

`force_single` 和 `single_decorator` 均接受以下参数：

- `lock_name` (str)：锁文件的名称或路径。
  - 如果仅传入文件名（如 `"my_job.lock"`），锁文件会自动创建在用户家目录（`~`）下。
  - 如果传入的是相对路径或绝对路径（如 `"/var/run/my_job.lock"`），则会在对应路径创建。如果父级目录不存在，会自动创建。
- `block` (bool)：
  - `False`（默认值）：非阻塞模式。如果发现锁已被占用，立即打印错误并退出程序。
  - `True`：阻塞排队模式。如果发现锁已被占用，当前实例会处于挂起等待状态，直到上一个实例结束并释放锁后，才接棒继续运行。

---

## 技术原理

1. **Linux / macOS**：底层调用 `fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)` 对打开的文件加上排他性锁定。
2. **Windows**：底层调用 `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` 对文件的首字节加锁。
3. 为了防止 Python 的垃圾回收机制（GC）在垃圾回收时过早自动关闭文件描述符导致锁失效，本库会在内存中使用全局字典保持对文件句柄的引用。
4. 一旦进程退出（无论是正常退出、遭遇未捕获异常、通过 `sys.exit` 退出，还是被 `kill -9` 强杀或断电关机），操作系统内核都会回收进程所占有的所有文件描述符，此时文件锁将被**瞬间释放**。
