import sqlite3
import time
import os
from pathlib import Path

# ================= 配置区域 (支持环境变量) =================
p115_db_path = os.getenv('P115_DB_PATH', '/data/115two.db')
openlist_db_path = os.getenv('OPENLIST_DB_PATH', '/data/data.db')
mount_prefix = os.getenv('MOUNT_PREFIX', '/two')
# =======================================================

def cleanup_sqlite_temp_files(db_path_str):
    """强制清理 SQLite 产生的 -shm 和 -wal 残留文件"""
    try:
        for ext in ["-shm", "-wal"]:
            # 【修复点2】放弃 pathlib 的 with_suffix 洁癖，直接用字符串暴力拼接后缀
            temp_file = Path(str(db_path_str) + ext)
            if temp_file.exists():
                temp_file.unlink()
                print(f"[*] 已成功清理残留文件: {temp_file.name}")
    except Exception as e:
        print(f"[!] 清理临时文件时出现警告: {e}")

def sync_index():
    print(f"[*] 开始读取 115 数据库: {p115_db_path}")
    start_time = time.time()
    
    try:
        # 【修复点1】放弃极易出错的 URI 和 mode=ro，使用标准直连
        conn_115 = sqlite3.connect(p115_db_path)
        cursor_115 = conn_115.cursor()
        
        # 抓取底层数据
        cursor_115.execute("SELECT id, parent_id, name, is_dir, size FROM data;")
        all_rows = cursor_115.fetchall()
        conn_115.close()
        
        print(f"[+] 成功读取 {len(all_rows)} 条数据，开始内存寻址...")
        
        # 内存重构目录树 (防断链设计)
        nodes = {str(r[0]): {'parent_id': str(r[1]), 'name': r[2], 'is_dir': r[3], 'size': r[4]} for r in all_rows}
        valid_nodes = []
        
        for node_id, info in nodes.items():
            current_id = node_id
            path_parts = []
            is_broken = False
            
            while current_id != '0':
                if current_id not in nodes:
                    is_broken = True
                    break
                path_parts.insert(0, nodes[current_id]['name'])
                current_id = nodes[current_id]['parent_id']
                
            if is_broken or not path_parts:
                continue
                
            name = path_parts[-1]
            parent_path = mount_prefix + '/' + '/'.join(path_parts[:-1]) if len(path_parts) > 1 else mount_prefix
            valid_nodes.append((parent_path, name, info['is_dir'], info['size']))
            
        print(f"[+] 路径重构完毕！有效路径: {len(valid_nodes)} 条。")

        # 写入 OpenList 数据库
        if valid_nodes:
            print(f"[*] 准备更新 OpenList 数据库: {openlist_db_path}")
            conn_openlist = sqlite3.connect(openlist_db_path)
            cursor_openlist = conn_openlist.cursor()
            
            cursor_openlist.execute("BEGIN TRANSACTION;")
            cursor_openlist.execute("DELETE FROM x_search_nodes WHERE parent = ? OR parent LIKE ?;", (mount_prefix, f"{mount_prefix}/%"))
            cursor_openlist.executemany("INSERT INTO x_search_nodes (parent, name, is_dir, size) VALUES (?, ?, ?, ?);", valid_nodes)
            
            conn_openlist.commit()
            conn_openlist.close()
            
            print(f"[+] 完美搞定！总耗时 {time.time() - start_time:.2f} 秒。")
        else:
            print("[-] 警告：没有生成任何有效路径，请检查。")

    except Exception as e:
        print(f"[!] 发生致命错误: {e}")
        
    finally:
        # 确保收尾工作绝对触发
        cleanup_sqlite_temp_files(p115_db_path)

if __name__ == "__main__":
    sync_index()