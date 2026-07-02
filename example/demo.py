# -*- coding:utf-8 -*-
"""
yyds-lock 使用演示 Demo

测试并发锁效果的方法：
1. 打开终端 1，运行：python demo.py
2. 迅速打开终端 2，运行：python demo.py
3. 预期效果：终端 2 会立刻输出红色错误并退出（退出码 1），而终端 1 不受影响继续运行。
"""

import time
import sys
import yyds_lock

# ----------------------------------------------------
# 姿势 A：直接调用形式 (适合单兵脚本开头直接锁死)
# ----------------------------------------------------
def run_direct_demo():
    lock_file = "yyds_demo_job.lock"
    print(f"【直接调用演示】尝试获取单例锁: {lock_file}...")
    
    # force_single 会自动在用户家目录下创建锁文件
    # block=False 表示如果锁已被占用，立即打印错误并退出
    yyds_lock.force_single(lock_name=lock_file, block=False)
    
    print("\a🎉 成功获取锁！当前进程独占运行中。")
    print("正在模拟耗时任务，请尝试在另一个终端窗口运行: python demo.py\n")
    
    for i in range(15):
        print(f" -> 正在执行任务中... ({i+1}/15)")
        time.sleep(1)
        
    print("\n任务完成，程序退出后锁将被系统自动释放。")

# ----------------------------------------------------
# 姿势 B：装饰器形式 (适合规范项目中的 main 函数)
# ----------------------------------------------------
@yyds_lock.single_decorator(lock_name="yyds_demo_decorator.lock", block=False)
def run_decorator_demo():
    print("【装饰器演示】成功进入 decorated 函数！当前进程独占运行中。")
    time.sleep(3)
    print("装饰器函数执行结束。")

if __name__ == "__main__":
    # 默认运行直接调用演示，方便用户在双终端中测试互斥退出效果
    try:
        run_direct_demo()
    except KeyboardInterrupt:
        print("\n用户手动中止运行。")
        sys.exit(0)
