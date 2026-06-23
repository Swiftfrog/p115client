import sqlite3
from pathlib import Path

# ================= 配置区 =================
BASE_DIR = Path(".") 
MASTER_DB = "master_upload.db"
# ==========================================

def init_master_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS state (
                file_path TEXT PRIMARY KEY,
                status TEXT,
                sha1 TEXT,
                size INTEGER,
                upload_id TEXT,
                oss_url TEXT,
                oss_callback TEXT,
                archived_path TEXT,
                last_updated TEXT,
                target_pid TEXT
            )
        ''')

def merge_databases():
    init_master_db(MASTER_DB)
    
    master_path = Path(MASTER_DB).resolve()
    db_files = [f for f in BASE_DIR.rglob("*.db") 
                if f.resolve() != master_path and f.suffix == '.db']
    
    if not db_files:
        print("❌ 没有找到任何 .db 文件")
        return

    print(f"🔍 找到 {len(db_files)} 个数据库，准备合并...")

    # 设置 timeout=60，遇到被 Docker 锁住的库时，多等一会儿
    with sqlite3.connect(MASTER_DB, timeout=60.0) as master_conn:
        master_cursor = master_conn.cursor()
        
        for idx, db_file in enumerate(db_files):
            db_path = str(db_file.absolute())
            alias = f"source_db_{idx}"  # 动态命名，避免 already in use
            
            print(f"📥 正在合并: {db_file.parent.name}/{db_file.name}")
            
            try:
                master_cursor.execute(f"ATTACH DATABASE ? AS {alias}", (db_path,))
                
                # 🚀 动态检测源库列名，兼容缺少 target_pid 的旧版数据库
                master_cursor.execute(f"PRAGMA {alias}.table_info(state)")
                source_cols = {row[1] for row in master_cursor.fetchall()}
                
                all_cols = ['file_path', 'status', 'sha1', 'size', 'upload_id', 
                           'oss_url', 'oss_callback', 'archived_path', 'last_updated', 'target_pid']
                # 源库有的列直接读取，缺的列填 NULL
                select_parts = [c if c in source_cols else f"NULL AS {c}" for c in all_cols]
                select_sql = ', '.join(select_parts)
                
                master_cursor.execute(f'''
                    INSERT INTO state 
                    SELECT {select_sql} FROM {alias}.state
                    WHERE true
                    ON CONFLICT(file_path) DO UPDATE SET
                        status = CASE 
                            WHEN excluded.status = 'success' THEN 'success'
                            ELSE state.status 
                        END,
                        sha1 = COALESCE(excluded.sha1, state.sha1),
                        size = COALESCE(excluded.size, state.size),
                        upload_id = COALESCE(excluded.upload_id, state.upload_id),
                        oss_url = COALESCE(excluded.oss_url, state.oss_url),
                        oss_callback = COALESCE(excluded.oss_callback, state.oss_callback),
                        archived_path = COALESCE(excluded.archived_path, state.archived_path),
                        last_updated = MAX(COALESCE(excluded.last_updated, ''), COALESCE(state.last_updated, '')),
                        target_pid = COALESCE(excluded.target_pid, state.target_pid)
                ''')
                master_conn.commit()
            except Exception as e:
                print(f"   ⚠️ 合并失败跳过: {e}")
            finally:
                try:
                    master_cursor.execute(f"DETACH DATABASE {alias}")
                except Exception:
                    pass

    with sqlite3.connect(MASTER_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM state")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*), SUM(size) FROM state WHERE status='success'")
        row = cursor.fetchone()
        count = row[0] if row else 0
        total_size = row[1] if (row and row[1]) else 0
        size_tb = total_size / (1024**4) if total_size else 0
        
        cursor.execute("SELECT COUNT(*) FROM state WHERE status='pending'")
        pending = cursor.fetchone()[0]
        
        print("\n✨ 合并完成！")
        print(f"📊 主库总览：{total} 条记录，成功 {count} 个 ({size_tb:.2f} TB)，待传 {pending} 个")

if __name__ == "__main__":
    merge_databases()
