#!/bin/bash

echo "正在查找并停止pipeline进程..."

# 查找pipeline进程
PARTITION_PIDS=$(ps aux | grep "pipeline" | grep -v grep | awk '{print $2}')

# 显示找到的进程
echo "找到的pipeline进程PID: $PARTITION_PIDS"

# Kill pipeline进程
if [ -n "$PARTITION_PIDS" ]; then
    echo "正在停止pipeline进程..."
    for pid in $PARTITION_PIDS; do
        echo "Killing PID: $pid"
        kill -9 $pid
        if [ $? -eq 0 ]; then
            echo "成功停止进程 PID: $pid"
        else
            echo "停止进程 PID: $pid 失败"
        fi
    done
else
    echo "未找到pipeline进程"
fi

# 等待一下并验证进程是否真的被停止
sleep 2

echo "验证进程是否已停止..."
REMAINING_PARTITION=$(ps aux | grep "pipeline" | grep -v grep | wc -l)

if [ $REMAINING_PARTITION -eq 0 ]; then
    echo "✅ pipeline进程已成功停止"
else
    echo "❌ 仍有进程未停止:"
    echo "  - pipeline: $REMAINING_PARTITION 个进程"
fi

echo "history_stop脚本执行完成" 