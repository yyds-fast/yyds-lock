# -*- coding:utf-8 -*-
"""
yyds-lock 使用演示 Demo

测试并发锁效果的方法：
1. 打开终端 1，运行：python example/demo.py
2. 迅速打开终端 2，运行：python example/demo.py
3. 预期效果：终端 2 会立刻输出错误并写入日志文件后退出（退出码 1），而终端 1 不受影响继续运行。
"""

import sys
import time
import yyds_lock

try:
    from yyds_logger import YydsLogger
    logger = YydsLogger("yyds_demo", log_dir="./logs").logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("yyds_demo")


# ----------------------------------------------------
# 姿势 A：直接调用形式 (适合脚本/爬虫开头直接加锁)
# ----------------------------------------------------
def run_direct_demo():
    lock_file = "yyds_demo_job.lock"
    logger.info(f"【直接调用演示】尝试获取单例锁: {lock_file}...")

    # force_single 传入已初始化好的 logger
    # 当锁被占用触发退出时，错误日志将自动同步写入日志文件和控制台
    yyds_lock.force_single(lock_name=lock_file, block=False, logger=logger)

    logger.info("🎉 成功获取锁！当前进程独占运行中。")
    logger.info("正在模拟耗时任务，请尝试在另一个终端窗口运行: python example/demo.py")

    for i in range(10):
        logger.info(f" -> 正在执行业务任务中... ({i + 1}/10)")
        time.sleep(1)

    logger.info("任务完成，程序退出后锁将被系统自动安全释放。")


# ----------------------------------------------------
# 姿势 B：装饰器形式 (适合定时任务/函数的自动锁保护)
# ----------------------------------------------------
@yyds_lock.single_decorator(
    lock_name="yyds_demo_decorator.lock",
    block=False,
    logger=logger,
)
def run_decorator_demo():
    logger.info("【装饰器演示】成功进入 decorated 函数！当前进程独占运行中。")
    time.sleep(3)
    logger.info("装饰器函数执行结束。")


if __name__ == "__main__":
    try:
        run_direct_demo()
    except KeyboardInterrupt:
        logger.warning("用户手动中止运行。")
        sys.exit(0)
