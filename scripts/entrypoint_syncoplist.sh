#!/bin/bash
# entrypoint_syncoplist.sh

echo "[*] 启动 115 数据库同步任务..."

# 执行数据拉取 (传递来自 docker run 或 compose command 的参数)
p115updatedb "$@"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[+] 115 数据拉取成功，开始注入 OpenList 索引..."
    # 调用注入脚本
    python3 /app/sync_openlist.py
    if [ $? -eq 0 ]; then
        echo "[+] OpenList 索引同步完成。"
    else
        echo "[!] OpenList 注入失败。"
        exit 1
    fi
else
    echo "[!] 115 数据拉取失败 (Exit Code: $EXIT_CODE)，终止后续索引同步。"
    exit $EXIT_CODE
fi