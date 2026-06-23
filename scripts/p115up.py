import json
import sqlite3
import time
import hashlib
import math
import shutil
import requests

import threading
import os
import logging
import signal
import sys
import random
import concurrent.futures
from pathlib import Path
from os.path import getsize
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from p115client import P115Client
from p115client.tool.upload import P115MultipartUpload
from p115oss.api import _UPLOAD_TOKEN
from p115oss import upload_file_init as oss_upload_init

# ==================== 🛠️ 用户配置区 ====================
COOKIE_PATH = Path("/config/115-cookies.txt").expanduser()
LOCAL_FOLDER = Path("/data/sync")                   # 本地源目录
SUCCESS_FOLDER = Path("/data/finished")             # 成功归档目录
TARGET_DIR_ID = os.environ.get("TARGET_DIR_ID", "0")        # 网盘父目录ID
STATE_FILE = Path("/config/upload_state.db")            # 状态文件 (.db)
LOG_FILE = Path("/config/upload.log")               # 日志文件路径

def parse_size(size_val):
    """支持将 10M, 1G 这样的人性化字符串解析为字节数"""
    if isinstance(size_val, (int, float)): return int(size_val)
    size_str = str(size_val).strip().upper()
    if not size_str: return 0
    if size_str.endswith('K'): return int(float(size_str[:-1]) * 1024)
    if size_str.endswith('M'): return int(float(size_str[:-1]) * 1024**2)
    if size_str.endswith('G'): return int(float(size_str[:-1]) * 1024**3)
    if size_str.endswith('T'): return int(float(size_str[:-1]) * 1024**4)
    return int(size_str) # 如果没有单位，默认视为纯字节数字

PART_SIZE               = parse_size(os.environ.get("PART_SIZE", "100M"))                # 绝对不能小于 10M
SIMPLE_UPLOAD_LIMIT     = parse_size(os.environ.get("SIMPLE_UPLOAD_LIMIT", "500M"))      # 常规文件阈值
DIRECT_UPLOAD_THRESHOLD = parse_size(os.environ.get("DIRECT_UPLOAD_THRESHOLD", "5M"))    # 🚀 新增：极小文件直传阈值

MAX_RETRIES         = int(os.environ.get("MAX_RETRIES", 5))
MAX_WORKERS         = int(os.environ.get("MAX_WORKERS", 1))
RAPID_ONLY          = int(os.environ.get("RAPID_ONLY", 0)) 
SKIP_UPLOADED       = int(os.environ.get("SKIP_UPLOADED", 1))
FORCE_UPLOAD        = int(os.environ.get("FORCE_UPLOAD", 0))       # 🚀 强制跳过秒传，直接物理上传
RAPID_MAX_RETRIES   = int(os.environ.get("RAPID_MAX_RETRIES", 3))  # 🚀 秒传 sig invalid 最大重试次数，超过后自动降级普通上传

MIN_DELAY           = float(os.environ.get("MIN_DELAY", 2))
MAX_DELAY           = float(os.environ.get("MAX_DELAY", 3))

# 🚀 自定义忽略文件后缀 (用逗号分隔，默认已包含常见的下载临时文件)
DEFAULT_IGNORED_EXTS = ".!qb,.aria2,.part,.crdownload,.tmp,.td,.xltd,.downloading,.nfo"
IGNORED_EXTS_ENV     = os.environ.get("IGNORED_EXTS", DEFAULT_IGNORED_EXTS)

# 默认 45 分钟热更新凭证
TOKEN_REFRESH_INTERVAL = int(os.environ.get("TOKEN_REFRESH_INTERVAL", 2700))

# =======================================================

# ==================== 📝 日志系统配置 ====================
logger = logging.getLogger("p115up")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 1. 写入 upload.log 文件
fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

# 2. 写入 docker logs 终端
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
# =======================================================

# 全局统计字典
STATS = {
    "total": 0, "rapid_success": 0, "regular_success": 0,
    "skipped": 0, "failed": 0, "ignored": 0, "total_size": 0
}
STATS_LOCK = threading.Lock()
DIR_LOCK = threading.Lock() 
DIR_CACHE = {} 

API_DELAY_LOCK = threading.Lock()

# 🌐 全局网络错误标志：任何线程检测到网络断开时设置，主循环检查此标志以中断当前轮并重启
NETWORK_ERROR = threading.Event()

# ==================== 🛡️ 防风控 405 拦截状态管理器 ====================
WAF_LOCK = threading.Lock()
WAF_405_COUNT = 0
WAF_RESUME_TIME = 0

def reset_waf_counter():
    global WAF_405_COUNT
    with WAF_LOCK:
        WAF_405_COUNT = 0

def is_network_error(err):
    """判断是否为网络连接类错误（断网/DNS失败/连接拒绝等）"""
    err_str = str(err).lower()
    network_keywords = [
        'network is unreachable', 'name or service not known',
        'temporary failure in name resolution', 'connection refused',
        'no route to host', 'connection reset', 'connection aborted',
        'errno 101', 'errno 111', 'errno 110', 'errno 104',
        'timed out', 'timeout',
    ]
    return any(kw in err_str for kw in network_keywords)

def wait_for_network(max_wait=600, check_interval=30):
    """网络断开时阻塞等待恢复，最长等 max_wait 秒"""
    import socket
    logger.warning(f"🌐 检测到网络异常，开始等待网络恢复（最长 {max_wait}s）...")
    print(f"\n🌐 网络似乎断开了，等待恢复中...")
    waited = 0
    while waited < max_wait:
        time.sleep(check_interval)
        waited += check_interval
        try:
            socket.create_connection(("114.114.114.114", 53), timeout=5).close()
            logger.info(f"🌐 网络已恢复！（等待了 {waited}s）")
            print(f"   ✅ 网络已恢复（等待了 {waited}s）")
            return True
        except OSError:
            logger.debug(f"网络仍未恢复，已等待 {waited}s...")
    logger.error(f"🌐 等待网络恢复超时（{max_wait}s）")
    return False

def check_waf_block():
    global WAF_RESUME_TIME
    while True:
        now = time.time()
        if now < WAF_RESUME_TIME:
            sleep_sec = WAF_RESUME_TIME - now
            time.sleep(min(sleep_sec, 60)) 
        else:
            break

def handle_possible_waf(error_msg):
    global WAF_405_COUNT, WAF_RESUME_TIME
    err_str = str(error_msg).lower()
    
    if "405" in err_str or "method not allowed" in err_str or "block_url_tips" in err_str:
        with WAF_LOCK:
            WAF_405_COUNT += 1
            current_count = WAF_405_COUNT
            
            if current_count == 10:
                logger.error("🚨 连续检测到 10 次 405/WAF拦截错误！触发防风控机制，全局挂起 35 分钟...")
                print(f"\n🛑 [风控告警] 触发 115 频率限制，全局挂起 35 分钟以保平安...\n")
                WAF_RESUME_TIME = time.time() + (35 * 60)
                WAF_405_COUNT = 0
# ========================================================================

# ==================== 📦 日志统计与归档处理 ====================
def print_summary():
    with STATS_LOCK:
        gb_size = STATS['total_size'] / (1024**3)
        summary_lines = [
            "==================== 📊 任务执行摘要 ====================",
            f" 📂 扫描有效文件: {STATS['total']} (已忽略垃圾: {STATS['ignored']})",
            f" ⏭️ 已传跳过:     {STATS['skipped']}",
            f" ⚡ 秒传成功:     {STATS['rapid_success']}",
            f" 📦 普通成功:     {STATS['regular_success']}",
            f" ❌ 失败/中断:    {STATS['failed']}",
            f" 💾 本轮迁移总量: {gb_size:.2f} GB",
            "========================================================"
        ]
    
    for line in summary_lines:
        logger.info(line)
    print("\n" + "\n".join(summary_lines) + "\n")

def archive_log():
    try:
        print_summary()
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
            logger.removeHandler(handler)
        
        if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            new_log_path = LOG_FILE.with_name(f"upload_{timestamp}.log")
            LOG_FILE.rename(new_log_path)
            print(f"\n📦 本次任务日志已归档至: {new_log_path.name}")
    except Exception as e:
        print(f"\n⚠️ 日志归档失败: {e}")

def handle_exit(signum, frame):
    logger.warning(f"收到终止信号 ({signum})，准备安全退出并归档日志...")
    print(f"\n🛑 收到系统终止信号 ({signum})，正在安全退出...")
    # 🚀 修复 #8：退出前也执行 WAL checkpoint
    try:
        state = StateHandler(STATE_FILE)
        state.checkpoint()
    except Exception:
        pass
    archive_log()
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)
# =======================================================

# ==================== 🗄️ SQLite 状态管理器 ====================
class StateHandler:
    def __init__(self, path):
        raw_path = Path(path)
        self.db_path = raw_path.with_suffix('.db')
        self.lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    def _init_db(self):
        with self.lock:
            conn = self._conn
            conn.execute('''
                CREATE TABLE IF NOT EXISTS state (
                    file_path TEXT PRIMARY KEY,
                    status TEXT, sha1 TEXT, size INTEGER,
                    upload_id TEXT, oss_url TEXT, oss_callback TEXT,
                    archived_path TEXT, last_updated TEXT
                )
            ''')
            try:
                conn.execute("ALTER TABLE state ADD COLUMN target_pid TEXT")
            except sqlite3.OperationalError:
                pass
            conn.commit()

    def checkpoint(self):
        """🚀 新增：强制执行 WAL 归档，防止长时间运行导致 .db-wal 文件无限膨胀"""
        with self.lock:
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception as e:
                logger.warning(f"WAL Checkpoint 执行异常: {e}")

    def get(self, file_path):
        with self.lock:
            conn = self._conn
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM state WHERE file_path=?", (str(file_path),))
            row = cursor.fetchone()
            conn.row_factory = None
            if not row:
                return {}
            data = dict(row)
            for k, v in data.items():
                if isinstance(v, str) and (v.startswith('{') or v.startswith('[')):
                    try: data[k] = json.loads(v)
                    except json.JSONDecodeError: pass
            return data

    def find_sha1_by_name_and_size(self, filename, size):
        with self.lock:
            cursor = self._conn.cursor()
            cursor.execute('''
                SELECT sha1 FROM state 
                WHERE (file_path LIKE ? OR file_path LIKE ? OR file_path = ?) 
                  AND size = ? AND sha1 IS NOT NULL
                LIMIT 1
            ''', ('%/' + str(filename), '%\\' + str(filename), str(filename), int(size)))
            row = cursor.fetchone()
            return row[0] if row else None

    def update(self, file_path, **kwargs):
        with self.lock:
            for k, v in kwargs.items():
                if isinstance(v, (dict, list)):
                    kwargs[k] = json.dumps(v, ensure_ascii=False)

            conn = self._conn
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM state WHERE file_path=?", (str(file_path),))
            row = cursor.fetchone()
            conn.row_factory = None
            data = dict(row) if row else {}

            data.update(kwargs)
            data['file_path'] = str(file_path)
            data['last_updated'] = time.strftime("%Y-%m-%d %H:%M:%S")

            keys = list(data.keys())
            values = [data[k] for k in keys]
            cols = ', '.join(keys)
            placeholders = ', '.join(['?'] * len(keys))

            cursor.execute(f'''
                INSERT OR REPLACE INTO state ({cols})
                VALUES ({placeholders})
            ''', tuple(values))
            conn.commit()

    def clear_session(self, file_path):
        with self.lock:
            cursor = self._conn.cursor()
            cursor.execute('''
                UPDATE state 
                SET upload_id = NULL, oss_url = NULL, oss_callback = NULL 
                WHERE file_path = ?
            ''', (str(file_path),))
            self._conn.commit()
# ====================================================================================

def is_valid_file(path: Path):
    name = path.name
    if name.startswith('.'): return False 
    if name.startswith('~$'): return False
    
    ignored_names = {'thumbs.db', 'desktop.ini', 'icon\r', '$recycle.bin', 'system volume information'}
    if name.lower() in ignored_names: return False
    
    for part in path.parts:
        if part in ['__MACOSX', '$RECYCLE.BIN']: return False
        
    exts = [e.strip().lower() for e in IGNORED_EXTS_ENV.split(',') if e.strip()]
    lower_name = name.lower()
    for suffix in exts:
        if not suffix.startswith('.'):
            suffix = '.' + suffix
        if lower_name.endswith(suffix): 
            return False
            
    return True



def move_to_success(src_path: Path, state: StateHandler):
    try:
        f_size = getsize(src_path)
        rel_path = src_path.relative_to(LOCAL_FOLDER)
        dst_path = SUCCESS_FOLDER / rel_path
        if not dst_path.parent.exists(): dst_path.parent.mkdir(parents=True, exist_ok=True)
        if dst_path.exists():
            stem = dst_path.stem
            suffix = dst_path.suffix
            counter = 1
            while dst_path.exists():
                dst_path = dst_path.parent / f"{stem}_{counter}{suffix}"
                counter += 1
        shutil.move(str(src_path), str(dst_path))
        state.update(src_path, status='success', archived_path=str(dst_path))
        state.clear_session(src_path)
        with STATS_LOCK: STATS['total_size'] += f_size
        logger.info(f"归档成功: {src_path.name} -> {dst_path}")
        return True
    except Exception as e:
        logger.error(f"归档失败 {src_path.name}: {e}")
        print(f"⚠️ 归档失败 {src_path.name}: {e}")
        return False

def find_child_in_pid(client, parent_pid, target_name):
    page = 1
    page_size = 1000
    max_pages = 10 
    
    while page <= max_pages:
        if page > 1: smart_sleep() 
            
        payload = {'cid': parent_pid, 'limit': page_size, 'offset': (page-1)*page_size, 'show_dir': 1}
        resp = client.fs_files(payload)
        if not resp.get('state'): return None, False
        data = resp.get('data', [])
        if not data: break
            
        for item in data:
            if item.get('n') == target_name:
                is_file = 'fid' in item
                is_dir = not is_file
                item_id = str(item.get('fid')) if is_file else str(item.get('cid'))
                return item_id, is_dir
        
        if len(data) < page_size: break
        page += 1
        
    if page > max_pages:
        logger.warning(f"目录查询达到最大翻页限制 ({max_pages}页)，强制中断防止死循环。")
        
    return None, False

def get_target_pid(client, local_file_path):
    try: relative_parent = local_file_path.parent.relative_to(LOCAL_FOLDER)
    except ValueError: return TARGET_DIR_ID

    if str(relative_parent) == ".": return TARGET_DIR_ID
    str_rel_path = str(relative_parent)

    with DIR_LOCK:
        if str_rel_path in DIR_CACHE: return DIR_CACHE[str_rel_path]

    with DIR_LOCK:
        if str_rel_path in DIR_CACHE: return DIR_CACHE[str_rel_path]

        try:
            smart_sleep()
            resp = client.fs_makedirs_app(str_rel_path, pid=TARGET_DIR_ID)
            if resp.get('state'):
                new_pid = str(resp.get('cid'))
                DIR_CACHE[str_rel_path] = new_pid
                logger.info(f"一次性创建网盘目录树: {str_rel_path} (ID: {new_pid})")
                return new_pid
            else:
                logger.warning(f"fs_makedirs_app 返回失败: {resp}，回退到逐级创建模式")
        except Exception as e:
            logger.warning(f"fs_makedirs_app 调用异常: {e}，回退到逐级创建模式")

    # ========== 回退模式 ==========
    current_pid = TARGET_DIR_ID
    parts = relative_parent.parts
    accumulated_path = Path("")

    for folder_name in parts:
        accumulated_path = accumulated_path / folder_name
        str_acc_path = str(accumulated_path)

        with DIR_LOCK:
            if str_acc_path in DIR_CACHE:
                current_pid = DIR_CACHE[str_acc_path]
                continue
            
            smart_sleep()
            found_id, is_dir = find_child_in_pid(client, current_pid, folder_name)
            
            if found_id:
                if is_dir:
                    current_pid = found_id
                    DIR_CACHE[str_acc_path] = current_pid
                else:
                    logger.error(f"目录创建冲突: '{folder_name}' 存在同名文件")
                    return current_pid 
            else:
                try:
                    smart_sleep()
                    resp = client.fs_mkdir({'cname': folder_name, 'pid': current_pid})
                    if resp.get('state'):
                        new_pid = str(resp.get('id') or resp.get('file_id') or resp.get('cid'))
                        current_pid = new_pid
                        DIR_CACHE[str_acc_path] = current_pid
                        logger.info(f"新建网盘目录: {str_acc_path} (ID: {current_pid})")
                    else:
                        found_id_retry, is_dir_retry = find_child_in_pid(client, current_pid, folder_name)
                        if found_id_retry and is_dir_retry:
                            current_pid = found_id_retry
                            DIR_CACHE[str_acc_path] = current_pid
                        else: return TARGET_DIR_ID
                except Exception as e:
                    logger.error(f"创建目录异常 '{folder_name}': {e}")
                    return TARGET_DIR_ID
    return current_pid

def cleanup_empty_dirs(root_path):
    print(f"\n🧹 正在清理空目录及系统残留垃圾: {root_path}")
    deleted_count = 0
    root_path_str = str(Path(root_path).resolve())

    def is_garbage_file(fname):
        fname_lower = fname.lower()
        return (
            fname.startswith('._') or         
            fname == '.DS_Store' or           
            fname_lower in ['thumbs.db', 'desktop.ini']  
        )

    for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
        if '__MACOSX' in dirpath: continue
        
        if str(Path(dirpath).resolve()) == root_path_str:
            continue

        try:
            all_garbage = all(is_garbage_file(f) for f in filenames)
            
            if not dirnames and all_garbage:
                for f in filenames:
                    file_to_del = os.path.join(dirpath, f)
                    os.remove(file_to_del)
                
                os.rmdir(dirpath)
                deleted_count += 1
        except Exception as e:
            logger.debug(f"清理目录跳过 {dirpath}: {e}")
            pass
            
    if deleted_count > 0:
        logger.info(f"清理完毕: 删除了 {deleted_count} 个本地空目录")
        print(f"   ✅ 共清理 {deleted_count} 个空目录 (已自动清除跨平台残留文件)")
    else:
        print(f"   ✅ 没有发现空目录")

def smart_sleep():
    check_waf_block() 
    with API_DELAY_LOCK:
        if MAX_DELAY > 0:
            actual_min = min(MIN_DELAY, MAX_DELAY)
            sleep_time = random.uniform(actual_min, MAX_DELAY)
            time.sleep(sleep_time)

def _is_sig_invalid(resp):
    """判断是否为 115 服务端签名校验失败（statuscode 702 / sig invalid）"""
    if isinstance(resp, dict):
        return resp.get("statuscode") == 702 or "sig invalid" in str(resp.get("statusmsg", "")).lower()
    return False

def check_rapid_upload_task(file_path, client, state):
    try:
        logger.info(f"🔍 [读取文件名] 准备处理: {file_path.name}")
        file_state = state.get(file_path) or {}
        cached_sha1 = file_state.get('sha1')
        cached_size = file_state.get('size')
        cached_status = file_state.get('status')
        current_size = getsize(file_path)

        if SKIP_UPLOADED == 1 and cached_status == 'success' and cached_size == current_size:
            logger.info(f"   ⏭️ [已传跳过] 数据库标记为已完成，无需重复处理: {file_path.name}")
            with STATS_LOCK: STATS['skipped'] += 1
            return ("skipped", file_path, None)

        saved_upload_id = file_state.get('upload_id')
        if cached_status == 'pending' and saved_upload_id and cached_size == current_size:
            logger.info(f"   🔄 [断点续传] 发现未完成的上传会话，跳过秒传检测直接续传")
            return ("pending", file_path, None)

        # 🚀 极小文件直传：0 API 消耗，直接交阶段二
        if current_size <= DIRECT_UPLOAD_THRESHOLD:
            logger.info(f"   ⏩ [极速放行] 极小文件 ({current_size/1024:.1f}KB) 0 API消耗，移交阶段二直传: {file_path.name}")
            return ("pending", file_path, None)

        # 🚀 FORCE_UPLOAD=1：强制跳过秒传，直接进入阶段二物理上传
        if FORCE_UPLOAD == 1:
            logger.info(f"   🔧 [强制上传] FORCE_UPLOAD 已启用，跳过秒传检测，直接转入物理上传: {file_path.name}")
            target_pid = get_target_pid(client, file_path)
            state.update(file_path, size=current_size, status='pending', target_pid=target_pid)
            return ("pending", file_path, None)

        has_cached_sha1 = cached_sha1 and cached_size is not None and int(cached_size) == current_size
        if not has_cached_sha1:
            global_sha1 = state.find_sha1_by_name_and_size(file_path.name, current_size)
            if global_sha1:
                has_cached_sha1 = True
                cached_sha1 = str(global_sha1)

        sha1_to_use = ""
        if has_cached_sha1:
            sha1_to_use = str(cached_sha1)
            logger.info(f"   ⚡ [缓存命中] 直接使用 SHA1: {sha1_to_use[:8]}...")
        else:
            logger.info(f"   🔄 [无缓存] SHA1 将由秒传接口内部自动计算（合并IO，消除阻塞）: {file_path.name}")

        smart_sleep()
        target_pid = get_target_pid(client, file_path)

        # 🚀 动态超时：有缓存SHA1时纯网络请求(120s)，无缓存时需读文件算SHA1(按10MB/s估算)
        if sha1_to_use:
            init_timeout = 120
        else:
            init_timeout = max(120, int(current_size / (10 * 1024 * 1024)) + 60)

        # 🚀 sig invalid (702) 自动重试 + 超限降级：指数退避，最多 RAPID_MAX_RETRIES 次后转普通上传
        sig_retry = 0
        resp = None
        while True:
            resp = safe_api_call(
                init_timeout, oss_upload_init,
                file=str(file_path),
                pid=target_pid,
                filename=file_path.name,
                filesha1=sha1_to_use,
                filesize=current_size,
                user_id=client.user_id,
                user_key=client.user_key,
            )

            if not resp.get("state") and _is_sig_invalid(resp):
                sig_retry += 1
                if sig_retry >= RAPID_MAX_RETRIES:
                    logger.warning(
                        f"   ⚠️ [秒传降级] sig invalid 连续失败 {sig_retry} 次（已达上限 {RAPID_MAX_RETRIES}），"
                        f"自动降级为普通物理上传: {file_path.name}"
                    )
                    print(f"   ⚠️ 秒传 sig invalid x{sig_retry}，自动降级普通上传: {file_path.name}")
                    state.update(file_path, size=current_size, status='pending', target_pid=target_pid)
                    return ("pending", file_path, None)
                wait_sec = 2 ** sig_retry  # 指数退避：2s → 4s → 8s
                logger.warning(
                    f"   🔁 [秒传重试] sig invalid (第{sig_retry}/{RAPID_MAX_RETRIES}次)，"
                    f"{wait_sec}s 后重试: {file_path.name}"
                )
                time.sleep(wait_sec)
                continue  # 重试本次请求

            break  # 正常响应（成功或其他错误），退出重试循环

        resp_data = resp.get("data", {})
        computed_sha1 = resp_data.get("filesha1", sha1_to_use)
        state.update(file_path, sha1=computed_sha1, size=current_size, status='pending', target_pid=target_pid)

        if resp.get("reuse"):
            move_to_success(file_path, state)
            with STATS_LOCK: STATS['rapid_success'] += 1
            reset_waf_counter()
            logger.info(f"   ✨ [秒传结果] 成功! 文件已移至归档目录")
            return ("success", file_path, None)

        if not resp.get("state"):
            raise Exception(f"秒传初始化失败: {resp}")

        logger.warning(f"   🐢 [秒传结果] 失败，已拿到凭证，转入传输流程")
        uploader = P115MultipartUpload(
            url=resp_data["url"],
            path=str(file_path),
            callback=resp_data["callback"],
        )
        try:
            state.update(file_path, upload_id=uploader.upload_id, target_pid=target_pid,
                         oss_url=resp_data.get("url"), oss_callback=resp_data.get("callback"))
        except: pass
        return ("pending", file_path, uploader)

    except Exception as e:
        # 🌐 网络错误：设置全局标志，主循环会中断当前轮并重新登录
        if is_network_error(e):
            NETWORK_ERROR.set()
            logger.warning(f"   🌐 [网络中断] {file_path.name}: {e}，已触发全局中断")
            return ("failed", file_path, f"网络中断: {e}")
        with STATS_LOCK: STATS['failed'] += 1
        logger.error(f"   🚨 [任务异常] {file_path.name}: {e}")
        handle_possible_waf(e)
        return ("failed", file_path, str(e))

def upload_small_file(file_path, client, state, target_pid):
    """🚀 核心优化：专门处理 Phase 1 放行的极小文件，干净利落直接推流"""
    print(f"📦 [直传] 上传: {file_path.name} -> PID {target_pid}")
    logger.info(f"开始极小文件直传: {file_path.name}")
    try:
        res = safe_api_call(
            120, client.upload_file,
            str(file_path), pid=target_pid, filename=file_path.name
        )
        if isinstance(res, dict) and res.get('state'):
            print(f"   ✅ 成功: {file_path.name}")
            logger.info(f"上传成功: {file_path.name}")
            move_to_success(file_path, state)
            with STATS_LOCK: STATS['regular_success'] += 1
            reset_waf_counter() 
            return True
        else:
            print(f"   ❌ 失败: {res}")
            logger.error(f"小文件上传失败 {file_path.name}: {res}")
            raise Exception(str(res))
    except Exception as e:
        print(f"   ❌ 异常: {e}")
        logger.error(f"小文件上传异常 {file_path.name}: {e}")
        with STATS_LOCK: STATS['failed'] += 1
        handle_possible_waf(e) 
        return False

def upload_large_file_manual(file_path, client, state, session, pre_uploader, target_pid):
    file_str = str(file_path)
    file_state = state.get(file_path) or {}
    sha1 = file_state.get('sha1')
    size = file_state.get('size') or getsize(file_path)

    effective_part_size = PART_SIZE
    if size and size > 0:
        min_required = math.ceil(size / 999)
        if min_required > effective_part_size:
            effective_part_size = min_required
            logger.info(f"⚠️ 文件过大，自动调整分块大小: {PART_SIZE/1024/1024:.1f}MB -> {effective_part_size/1024/1024:.1f}MB (确保 ≤999 块)")

    print(f"🐢 [分块] 上传: {file_path.name} -> PID {target_pid}")
    logger.info(f"开始分块/常规续传: {file_path.name} (大小: {size} bytes, 分块: {effective_part_size/1024/1024:.1f}MB)")

    for attempt in range(MAX_RETRIES):
        try:
            saved_id = file_state.get('upload_id')
            saved_url = file_state.get('oss_url')
            saved_cb = file_state.get('oss_callback')

            current_uploader = None
            
            # 🚀 优化：优先使用 Phase 1 传过来的 pre_uploader，绝不浪费凭证
            if pre_uploader:
                current_uploader = pre_uploader
            elif saved_id and saved_url and saved_cb:
                try:
                    current_uploader = P115MultipartUpload(url=saved_url, path=file_str, callback=saved_cb, upload_id=saved_id)
                    safe_api_call(30, current_uploader.list_parts)
                except Exception:
                    current_uploader = None

            if not current_uploader:
                logger.info(f"正在向 115 申请初始上传凭证 (第{attempt+1}次)...")
                smart_sleep()
                current_uploader = safe_api_call(
                    60, P115MultipartUpload.from_path,
                    file_str, pid=target_pid, user_id=client.user_id,
                    user_key=client.user_key, filesha1=sha1, filesize=size, filename=file_path.name
                )

                if isinstance(current_uploader, dict):
                    print("   ⚡ 补检秒传成功！")
                    logger.info(f"延迟秒传成功: {file_path.name}")
                    move_to_success(file_path, state)
                    with STATS_LOCK: STATS['rapid_success'] += 1
                    reset_waf_counter()
                    return

                state.update(file_path, upload_id=current_uploader.upload_id, target_pid=target_pid,
                             oss_url=getattr(current_uploader, 'url', None) or getattr(current_uploader, 'bucket_url', None),
                             oss_callback=getattr(current_uploader, 'callback', None))

            def hot_swap_token():
                logger.info("🔄 正在通过官方 gettoken 接口获取新鲜 STS 凭证...")
                token_res = safe_api_call(60, client.upload_gettoken)
                token_data = token_res.get('data', token_res) if isinstance(token_res, dict) else {}
                if token_data and ('AccessKeyId' in token_data or 'SecurityToken' in token_data):
                    _UPLOAD_TOKEN.update(token_data)
                    current_uploader.callback.update(token_data)
                    state.update(file_path, oss_callback=current_uploader.callback)
                    return True
                return False

            try:
                parts = safe_api_call(60, current_uploader.list_parts)
            except Exception:
                if hot_swap_token():
                    parts = safe_api_call(60, current_uploader.list_parts)
                else:
                    raise

            initial_bytes = sum(p.get("Size", 0) for p in (parts or []))
            if initial_bytes > 0:
                logger.info(f"恢复上传进度 {file_path.name}: {initial_bytes/1024/1024:.2f} MB")
                print(f"   🔄 恢复上传进度: {initial_bytes/1024/1024:.2f} MB")

            last_token_time = time.time()
            uploaded_bytes = 0
            first_hook = True

            with tqdm(total=size, initial=initial_bytes, unit="B", unit_scale=True, desc=f"   🚀 传输", leave=True, file=sys.stdout) as pbar:
                def _reporthook(n):
                    nonlocal first_hook
                    if first_hook:
                        first_hook = False
                        return 
                    pbar.update(n)

                for part_info in current_uploader.iter_upload(partsize=effective_part_size, reporthook=_reporthook):
                    uploaded_bytes += part_info.get("Size", 0)

                    if time.time() - last_token_time > TOKEN_REFRESH_INTERVAL:
                        logger.info(f"⏳ Token 已使用 {TOKEN_REFRESH_INTERVAL/60:.0f} 分钟，执行无感热更新...")
                        if hot_swap_token():
                            last_token_time = time.time()
                            logger.info("✅ 凭证热更新完成，继续丝滑满速续传！")

            logger.info("分块全部传输完毕，开始发送最终合并请求...")
            logger.info("正在获取最终合并专属安全凭证...")
            hot_swap_token()

            res = safe_api_call(120, current_uploader.complete)

            if res.get('state'):
                print("   ✅ 成功")
                logger.info(f"大文件上传成功: {file_path.name}")
                move_to_success(file_path, state)
                with STATS_LOCK: STATS['regular_success'] += 1
                reset_waf_counter()
                return
            else:
                print(f"   ❌ 合并失败: {res}")
                logger.error(f"大文件合并失败 {file_path.name}: {res}")
                raise Exception("Combine failed")

        except KeyboardInterrupt: raise
        except Exception as e:
            handle_possible_waf(e)
            pre_uploader = None  # 🚀 修复 #1：凭证已失效，下次循环走 DB 恢复或重新申请

            err_str = str(e).lower()
            if "403" in err_str or "forbidden" in err_str or "signaturedoesnotmatch" in err_str:
                logger.warning(f"由于授权过期导致中断，将在下一次重试中自动复活...")
                current_uploader = None

            if attempt >= MAX_RETRIES - 1:
                print(f"   ❌ 放弃: {e}")
                logger.error(f"大文件上传最终失败 {file_path.name}: {e}")
                with STATS_LOCK: STATS['failed'] += 1
                state.clear_session(file_path)
                return

            logger.warning(f"上传中断，稍后重试 {file_path.name} (第{attempt+1}次): {e}")
            time.sleep(3)

_API_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-call")

def safe_api_call(timeout_sec, func, *args, **kwargs):
    future = _API_EXECUTOR.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=timeout_sec)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise Exception(f"api_timeout_{func.__name__}")
    except Exception as e:
        raise e

def main():
    logger.info("=== 🚀 启动 p115up 批量上传任务 ===")
    print("🔐 登录 115...")
    try: client = P115Client(COOKIE_PATH)
    except Exception as e: 
        print(f"❌ 登录失败: {e}")
        logger.error(f"115 网盘登录初始化失败: {e}")
        return None
    
    if not client.login_status(): 
        print("⚠️ Cookie 失效")
        logger.error("Cookie 失效，需重新登录")
        return None
    
    logger.info(f"登录成功，当前用户ID: {client.user_id}")
    
    if not LOCAL_FOLDER.exists(): 
        logger.error(f"本地文件夹不存在: {LOCAL_FOLDER}")
        return None
    
    all_files = LOCAL_FOLDER.rglob("*")
    file_list = []
    ignored_count = 0
    
    for f in all_files:
        if f.is_file():
            if is_valid_file(f):
                file_list.append(f)
            else:
                ignored_count += 1
    
    with STATS_LOCK:
        STATS['total'] = len(file_list)
        STATS['ignored'] = ignored_count
    
    logger.info(f"文件扫描完毕，待处理: {len(file_list)}，已过滤: {ignored_count}")
    
    state = StateHandler(STATE_FILE)
    pending_large_files = []
    
    print(f"\n🚀 阶段一：扫描结构 & 秒传检测 (并发: {MAX_WORKERS})")
    print("=" * 50)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(check_rapid_upload_task, f, client, state): f for f in file_list}
        with tqdm(total=len(file_list), desc="🔍 进度", file=sys.stdout) as pbar:
            for future in as_completed(future_to_file):
                f_path = future_to_file[future]
                try:
                    res_type, f_path, payload = future.result()
                    if res_type == "success":
                        pbar.write(f"⚡ [秒传] {f_path.name}")
                    elif res_type == "skipped":
                        pbar.write(f"⏭️ [跳过] {f_path.name}")
                    elif res_type == "failed":
                        pbar.write(f"❌ [失败] {f_path.name}: {payload}")
                    elif res_type == "pending":
                        file_state = state.get(f_path) or {}
                        pid = file_state.get('target_pid') or TARGET_DIR_ID
                        status = file_state.get('status')
                        
                        # 🚀 修复 #2：用文件大小+upload_id判断，而非 payload
                        # 断点续传文件 payload 也是 None，不能用 payload 区分
                        f_size = getsize(f_path)
                        has_upload_session = bool(file_state.get('upload_id'))
                        if f_size <= DIRECT_UPLOAD_THRESHOLD and not has_upload_session:
                            pending_large_files.append((f_path, None, "small", pid, status))
                        elif f_size <= SIMPLE_UPLOAD_LIMIT and not has_upload_session:
                            # 中等文件（5MB-500MB）：秒传失败后走常规 upload_file
                            pending_large_files.append((f_path, payload, "small", pid, status))
                        else:
                            # 大文件或有断点续传会话：走 iter_upload 分块上传
                            pending_large_files.append((f_path, payload, "large", pid, status))
                except Exception as exc:
                    pbar.write(f"❌ [异常] {f_path.name}: {exc}")
                    logger.error(f"线程执行异常 {f_path.name}: {exc}")
                finally:
                    pbar.update(1)

        # 🌐 网络中断检测：如果 Phase 1 期间发生网络错误，中断当前轮，返回特殊信号
        if NETWORK_ERROR.is_set():
            logger.warning("🌐 Phase 1 检测到网络中断，中断当前轮，等待网络恢复后重新登录...")
            print("\n🌐 网络中断，当前轮已中止，等待恢复后将重新登录并重试...")
            return "network_error"

    if pending_large_files:
        if RAPID_ONLY == 1:
            print(f"\n⏭️ [全量秒传模式] 发现 {len(pending_large_files)} 个无法秒传的文件，已跳过物理上传。")
            logger.info(f"开启了 RAPID_ONLY 模式，跳过 {len(pending_large_files)} 个文件的物理上传。")
        else:
            print(f"\n🐢 阶段二：普通上传 (共 {len(pending_large_files)} 个)")
            print("="*50)
            
            pending_large_files.sort(key=lambda x: (
                x[4] not in ['pending', 'failed'],
                x[2] == "large"                    
            ))
            
            with requests.Session() as session:
                for i, (f_path, uploader, f_type, pid, status) in enumerate(pending_large_files, 1):
                    if not f_path.exists(): continue
                    
                    # 🚀 修复 #7：小文件跳过外层 sleep，get_target_pid 内部已有防护
                    if f_type != "small":
                        smart_sleep()
                    
                    status_flag = "🔙 断点/重试" if status in ['pending', 'failed'] else "🆕 新文件"
                    print(f"\n[{i}/{len(pending_large_files)}] 任务启动 ({status_flag})...")
                    
                    if f_type == "small":
                        pid = get_target_pid(client, f_path)
                        upload_small_file(f_path, client, state, pid)
                    else:
                        upload_large_file_manual(f_path, client, state, session, uploader, pid)
    
    cleanup_empty_dirs(LOCAL_FOLDER)
    print("\n✨ 全部完成！")
    return state

if __name__ == "__main__":
    SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", 1800))
    
    try:
        while True:
            # 🌐 每轮开始前清除网络错误标志
            NETWORK_ERROR.clear()
            
            with STATS_LOCK:
                STATS = {
                    "total": 0, "rapid_success": 0, "regular_success": 0,
                    "skipped": 0, "failed": 0, "ignored": 0, "total_size": 0
                }
            
            result = main()
            
            # 🌐 网络中断处理：等待网络恢复 → 重新登录 → 立即重试
            if result == "network_error":
                print_summary()
                if wait_for_network():
                    logger.info("🔄 网络已恢复，重新登录 115 并立即重试...")
                    print("🔄 重新登录并立即重试...\n")
                    time.sleep(5)  # 等几秒让网络稳定
                    continue  # 跳过 SCAN_INTERVAL，立即重新执行 main()
                else:
                    logger.error("🌐 网络未恢复，等待下一轮扫描")
            else:
                print_summary()
            
            # 正常完成或网络未恢复，执行 checkpoint 后休眠
            if isinstance(result, StateHandler):
                result.checkpoint()
            
            print(f"\n💤 本轮结束，休眠 {SCAN_INTERVAL} 秒等待新文件...\n")
            time.sleep(SCAN_INTERVAL)
            
    except Exception as e:
        logger.error(f"守护进程发生严重异常: {e}")
        archive_log()
