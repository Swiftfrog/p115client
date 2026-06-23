import json
import sqlite3
import time
import hashlib
import math
import shutil
import requests
import atexit
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
from p115oss import _UPLOAD_TOKEN, oss_upload_init

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

PART_SIZE           = parse_size(os.environ.get("PART_SIZE", "100M"))                # 绝对不能小于 10M
SIMPLE_UPLOAD_LIMIT = parse_size(os.environ.get("SIMPLE_UPLOAD_LIMIT", "500M"))      # 普通直传的文件大小上限。

MAX_RETRIES         = int(os.environ.get("MAX_RETRIES", 5))
MAX_WORKERS         = int(os.environ.get("MAX_WORKERS", 1))
RAPID_ONLY          = int(os.environ.get("RAPID_ONLY", 0)) 
SKIP_UPLOADED       = int(os.environ.get("SKIP_UPLOADED", 1))

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

# 🚀 修复漏洞 1：新增 API 全局延迟锁，强制排队请求
API_DELAY_LOCK = threading.Lock()

# ==================== 🛡️ 新增：防风控 405 拦截状态管理器 ====================
WAF_LOCK = threading.Lock()
WAF_405_COUNT = 0
WAF_RESUME_TIME = 0  # 🚀 新增：记录解封的时间戳

def reset_waf_counter():
    """成功操作后重置 WAF 计数器"""
    global WAF_405_COUNT
    with WAF_LOCK:
        WAF_405_COUNT = 0

def check_waf_block():
    """每个线程在发请求前必须调用，如果在封控期则原地睡到解封"""
    global WAF_RESUME_TIME
    while True:
        now = time.time()
        if now < WAF_RESUME_TIME:
            sleep_sec = WAF_RESUME_TIME - now
            # 避免一睡睡死，每次最多睡 60 秒，醒来再看一眼，方便响应 Ctrl+C 退出信号
            time.sleep(min(sleep_sec, 60)) 
        else:
            break

def handle_possible_waf(error_msg):
    """检测并处理 115 频控拦截机制"""
    global WAF_405_COUNT, WAF_RESUME_TIME
    err_str = str(error_msg).lower()
    
    # 匹配 405 状态码、Method Not Allowed 或 115 WAF 专属拦截 JS 特征词
    if "405" in err_str or "method not allowed" in err_str or "block_url_tips" in err_str:
        with WAF_LOCK:
            WAF_405_COUNT += 1
            current_count = WAF_405_COUNT
            
            # 如果已经触发了冷却，就不重复触发了
            if current_count == 10:
                logger.error("🚨 连续检测到 10 次 405/WAF拦截错误！触发防风控机制，全局挂起 35 分钟...")
                print(f"\n🛑 [风控告警] 触发 115 频率限制，全局挂起 35 分钟以保平安...\n")
                
                # 🚀 核心：设置未来解封的时间戳，然后立刻释放锁，绝不在这里 sleep！
                WAF_RESUME_TIME = time.time() + (35 * 60)
                WAF_405_COUNT = 0   # 重置计数，等待下一轮
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
        # 🚀 优化 #6：使用持久连接 + WAL 模式，减少连接开销，提升并发读性能
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
    # 忽略以点开头和以 ~$ 开头的文件
    if name.startswith('.'): return False 
    if name.startswith('~$'): return False
    
    # 忽略系统隐藏垃圾文件
    ignored_names = {'thumbs.db', 'desktop.ini', 'icon\r', '$recycle.bin', 'system volume information'}
    if name.lower() in ignored_names: return False
    
    # 忽略特殊目录
    for part in path.parts:
        if part in ['__MACOSX', '$RECYCLE.BIN']: return False
        
    # 解析并验证自定义后缀
    exts = [e.strip().lower() for e in IGNORED_EXTS_ENV.split(',') if e.strip()]
    lower_name = name.lower()
    for suffix in exts:
        # 确保后缀带点，例如用户填了 "nfo"，自动补为 ".nfo"
        if not suffix.startswith('.'):
            suffix = '.' + suffix
        if lower_name.endswith(suffix): 
            return False
            
    return True

def calculate_sha1(file_path, cached_sha1=None, cached_size=None):
    current_size = getsize(file_path)
    if cached_sha1 and cached_size is not None and int(cached_size) == current_size:
        logger.info(f"   ⚡ [物理跳过] 缓存验证通过，不读硬盘: {file_path.name}")
        return str(cached_sha1), current_size

    logger.warning(f"   ⚠️ [物理读取] 缓存失效或不存在，开始读取硬盘: {file_path.name}")
    sha1 = hashlib.sha1()
    read_size = 8 * 1024 * 1024  # 🚀 优化：8MB 缓冲区提升大文件读取吞吐

    # 🚀 优化：大文件显示进度条，消除 SHA1 计算的「空白等待期」
    if current_size > 50 * 1024 * 1024:
        with open(file_path, 'rb') as f, tqdm(
            total=current_size, unit="B", unit_scale=True,
            desc=f"   🔑 SHA1", leave=False
        ) as pbar:
            while chunk := f.read(read_size):
                sha1.update(chunk)
                pbar.update(len(chunk))
    else:
        with open(file_path, 'rb') as f:
            while chunk := f.read(read_size):
                sha1.update(chunk)
    return sha1.hexdigest(), current_size

def move_to_success(src_path: Path, state: StateHandler):
    try:
        f_size = getsize(src_path)
        rel_path = src_path.relative_to(LOCAL_FOLDER)
        dst_path = SUCCESS_FOLDER / rel_path
        if not dst_path.parent.exists(): dst_path.parent.mkdir(parents=True, exist_ok=True)
        if dst_path.exists():
            # 🚀 修复 #7：原代码 unpack 赋值数量不匹配会 crash
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
    max_pages = 10  # 🚀 新增：最多只翻 10 页（防止 115 分页接口死循环导致卡死）
    
    while page <= max_pages:
        if page > 1:
            smart_sleep() # 确保翻页时也强制排队延迟
            
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
        
        # 如果当前页返回的数据少于 page_size，说明到底了
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

    # 🚀 优化 #3：使用 fs_makedirs_app 一次性创建整个目录树（N 次 API 调用 → 1 次）
    with DIR_LOCK:
        # 双重检查：等锁期间可能已被其他线程创建
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

    # ========== 回退：逐级查找 + 创建（原始逻辑，仅在 fs_makedirs_app 失败时触发）==========
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

    # 🚀 定义“可以被当作空文件夹处理”的垃圾文件白名单
    def is_garbage_file(fname):
        fname_lower = fname.lower()
        return (
            fname.startswith('._') or         # Mac 资源分支文件
            fname == '.DS_Store' or           # Mac 目录设置文件
            fname_lower in ['thumbs.db', 'desktop.ini']  # Win 缩略图和目录设置
        )

    for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
        if '__MACOSX' in dirpath: continue
        
        # 绝对不删根目录
        if str(Path(dirpath).resolve()) == root_path_str:
            continue

        try:
            # 检查当前目录下的所有文件是否【全部】都是垃圾文件
            # 如果 filename 为空（[]），all() 也会返回 True，完美兼容真正的空目录
            all_garbage = all(is_garbage_file(f) for f in filenames)
            
            # 如果没有子目录了，且剩下的文件全是系统垃圾
            if not dirnames and all_garbage:
                
                # 1. 先把这些垃圾残留文件物理删除
                for f in filenames:
                    file_to_del = os.path.join(dirpath, f)
                    os.remove(file_to_del)
                
                # 2. 现在目录是真正的物理空置了，安全删除！
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
    """执行安全随机延迟 & 检查 WAF 封控状态"""
    
    # 1. 先查水表：如果处于 35 分钟大休眠期，所有调用的线程都会卡在这里挂起
    check_waf_block() 
    
    # 2. 正常呼吸：强制排队，打散并发洪峰，修复瞬间请求的漏洞
    with API_DELAY_LOCK:
        if MAX_DELAY > 0:
            actual_min = min(MIN_DELAY, MAX_DELAY)
            sleep_time = random.uniform(actual_min, MAX_DELAY)
            time.sleep(sleep_time)

def check_rapid_upload_task(file_path, client, state):
    try:
        logger.info(f"🔍 [读取文件名] 准备处理: {file_path.name}")
        file_state = state.get(file_path) or {}
        cached_sha1 = file_state.get('sha1')
        cached_size = file_state.get('size')
        cached_status = file_state.get('status')
        current_size = getsize(file_path)

        # 🚀 终极跳过逻辑：开关开启 + 标记为已成功 + 大小没变
        if SKIP_UPLOADED == 1 and cached_status == 'success' and cached_size == current_size:
            logger.info(f"   ⏭️ [已传跳过] 数据库标记为已完成，无需重复处理: {file_path.name}")
            with STATS_LOCK: STATS['skipped'] += 1
            return ("skipped", file_path, None)

        # 🚀 断点续传保护：如果数据库中存在未完成的上传会话，直接跳到 Phase 2 续传
        saved_upload_id = file_state.get('upload_id')
        if cached_status == 'pending' and saved_upload_id and cached_size == current_size:
            logger.info(f"   🔄 [断点续传] 发现未完成的上传会话 "
                        f"(文件: {current_size/1024/1024:.1f}MB, upload_id: {saved_upload_id[:8]}...)，"
                        f"跳过秒传检测直接续传")
            return ("pending", file_path, None)

        # 🚀 小文件极速放行：0 API 消耗，直接移交阶段二
        # upload_file 内部会自动完成：SHA1计算 + 秒传检测 + 上传，一步到位
        if current_size <= SIMPLE_UPLOAD_LIMIT:
            logger.info(f"   ⏩ [极速放行] 小文件 0 API 消耗，直接移交阶段二: {file_path.name}")
            return ("pending", file_path, None)

        # 到达此处必定是大文件（> SIMPLE_UPLOAD_LIMIT）
        # 查找缓存 SHA1，有则尝试秒传，无则由 oss_upload_init 内部计算
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

        logger.info(f"   🚀 [尝试秒传] 发送请求中: {file_path.name}")
        smart_sleep()

        target_pid = get_target_pid(client, file_path)

        # 🚀 直接调用 oss_upload_init：SHA1计算 + 秒传检测 + 二次验证，一步完成
        resp = oss_upload_init(
            file=str(file_path),
            pid=target_pid,
            filename=file_path.name,
            filesha1=sha1_to_use,
            filesize=current_size,
            user_id=client.user_id,
            user_key=client.user_key,
        )

        # 从响应中提取库计算好的 SHA1，回写数据库作为未来的缓存
        resp_data = resp.get("data", {})
        computed_sha1 = resp_data.get("filesha1", sha1_to_use)
        state.update(file_path, sha1=computed_sha1, size=current_size, status='pending')

        if resp.get("reuse"):
            move_to_success(file_path, state)
            with STATS_LOCK: STATS['rapid_success'] += 1
            reset_waf_counter()
            logger.info(f"   ✨ [秒传结果] 成功! 文件已移至归档目录")
            return ("success", file_path, None)

        if not resp.get("state"):
            raise Exception(f"秒传初始化失败: {resp}")

        logger.warning(f"   🐢 [秒传结果] 失败，转入普通上传流程")
        # 直接用响应数据构建分块上传对象（不再经过 from_path 二次封装）
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
        with STATS_LOCK: STATS['failed'] += 1
        logger.error(f"   🚨 [任务异常] {file_path.name}: {e}")
        handle_possible_waf(e)
        return ("failed", file_path, str(e))

def upload_small_file(file_path, client, state, target_pid):
    print(f"📦 [普通] 上传: {file_path.name} -> PID {target_pid}")
    logger.info(f"开始小文件上传: {file_path.name}")
    try:
        # 🚀 优化 #5：传入已缓存的 SHA1，避免库内部二次计算
        file_state = state.get(file_path) or {}
        cached_sha1 = file_state.get('sha1', '')
        cached_size = file_state.get('size', -1)
        res = client.upload_file(
            str(file_path), pid=target_pid,
            filesha1=cached_sha1, filesize=cached_size, filename=file_path.name
        )
        if isinstance(res, dict) and res.get('state'):
            print(f"   ✅ 成功: {file_path.name}")
            logger.info(f"上传成功: {file_path.name}")
            move_to_success(file_path, state)
            with STATS_LOCK: STATS['regular_success'] += 1
            reset_waf_counter() # 🛡️ 成功即清零拦截计数
            return True
        else:
            print(f"   ❌ 失败: {res}")
            logger.error(f"小文件上传失败 {file_path.name}: {res}")
            raise Exception(str(res))
    except Exception as e:
        print(f"   ❌ 异常: {e}")
        logger.error(f"小文件上传异常 {file_path.name}: {e}")
        with STATS_LOCK: STATS['failed'] += 1
        handle_possible_waf(e) # 🛡️ 检测是否被 115 拦截
        return False

def upload_large_file_manual(file_path, client, state, session, pre_uploader, target_pid):
    file_str = str(file_path)
    file_state = state.get(file_path) or {}
    sha1 = file_state.get('sha1')
    size = file_state.get('size') or getsize(file_path)

    # 🛡️ 防御 p115oss 库的 OSS 签名分页 Bug：当分块超过 1000 个时，
    # list_parts 分页会把 part-number-marker 参数错误地编入签名，导致 403。
    # 动态提升分块大小，确保总分块数不超过 999，从根源规避分页。
    effective_part_size = PART_SIZE
    if size and size > 0:
        min_required = math.ceil(size / 999)
        if min_required > effective_part_size:
            effective_part_size = min_required
            logger.info(f"⚠️ 文件过大，自动调整分块大小: {PART_SIZE/1024/1024:.1f}MB -> {effective_part_size/1024/1024:.1f}MB (确保 ≤999 块)")

    print(f"🐢 [分块] 上传: {file_path.name} -> PID {target_pid}")
    logger.info(f"开始大文件分块上传: {file_path.name} (大小: {size} bytes, 分块: {effective_part_size/1024/1024:.1f}MB)")

    for attempt in range(MAX_RETRIES):
        try:
            # 1. 初始化 / 恢复 Uploader
            saved_id = file_state.get('upload_id')
            saved_url = file_state.get('oss_url')
            saved_cb = file_state.get('oss_callback')

            current_uploader = None
            if saved_id and saved_url and saved_cb:
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

            # 🌟 Token 热更新机制（保留原有核心逻辑）
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

            # 凭证预检 + 获取已上传进度
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

            # 🚀 优化 #2：使用库内置 iter_upload 替代手写分块上传
            # iter_upload 自动处理：断点续传、list_parts 跳过已传分块、文件定位、分块管理
            last_token_time = time.time()
            uploaded_bytes = 0
            first_hook = True

            with tqdm(total=size, initial=initial_bytes, unit="B", unit_scale=True, desc=f"   🚀 传输", leave=True) as pbar:
                def _reporthook(n):
                    """iter_upload 首次回调是 skipsize（已跳过字节），跳过以避免与 tqdm initial 重复计数"""
                    nonlocal first_hook
                    if first_hook:
                        first_hook = False
                        return  # 首次回调是 skipsize，已通过 initial 设置，不重复 update
                    pbar.update(n)

                for part_info in current_uploader.iter_upload(partsize=effective_part_size, reporthook=_reporthook):
                    uploaded_bytes += part_info.get("Size", 0)

                    # 🌟 定期热更新 Token，防止超时 403
                    if time.time() - last_token_time > TOKEN_REFRESH_INTERVAL:
                        logger.info(f"⏳ Token 已使用 {TOKEN_REFRESH_INTERVAL/60:.0f} 分钟，执行无感热更新...")
                        if hot_swap_token():
                            last_token_time = time.time()
                            logger.info("✅ 凭证热更新完成，继续丝滑满速续传！")

            logger.info("分块全部传输完毕，开始发送最终合并请求...")

            # 🌟 最终防线：合并前再做一次热更新
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

# 🚀 优化 #4：使用共享线程池，避免每次调用都创建/销毁线程池
_API_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-call")

def safe_api_call(timeout_sec, func, *args, **kwargs):
    """防弹级网络调用：强制设定最长等待时间，超时直接斩断僵尸连接"""
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
        return
    
    if not client.login_status(): 
        print("⚠️ Cookie 失效")
        logger.error("Cookie 失效，需重新登录")
        return
    
    logger.info(f"登录成功，当前用户ID: {client.user_id}")
    
    if not LOCAL_FOLDER.exists(): 
        logger.error(f"本地文件夹不存在: {LOCAL_FOLDER}")
        return
    
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
        with tqdm(total=len(file_list), desc="🔍 进度") as pbar:
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
                        if getsize(f_path) < SIMPLE_UPLOAD_LIMIT:
                            pending_large_files.append((f_path, None, "small", pid, status)) 
                        else:
                            pending_large_files.append((f_path, payload, "large", pid, status))
                except Exception as exc:
                    pbar.write(f"❌ [异常] {f_path.name}: {exc}")
                    logger.error(f"线程执行异常 {f_path.name}: {exc}")
                finally:
                    pbar.update(1)

    # 🚀 阶段二：进行 RAPID_ONLY 拦截判断
    if pending_large_files:
        if RAPID_ONLY == 1:
            print(f"\n⏭️ [全量秒传模式] 发现 {len(pending_large_files)} 个无法秒传的文件，已跳过物理上传。")
            logger.info(f"开启了 RAPID_ONLY 模式，跳过 {len(pending_large_files)} 个文件的物理上传。")
        else:
            print(f"\n🐢 阶段二：普通上传 (共 {len(pending_large_files)} 个)")
            print("="*50)
            
            # 🚀 核心排序逻辑：失败/中断优先，小文件优先，最后大文件
            pending_large_files.sort(key=lambda x: (
                x[4] not in ['pending', 'failed'], # 优先处理失败或中断的 (False 在 True 前面)
                x[2] == "large"                    # small 优先于 large
            ))
            
            with requests.Session() as session:
                for i, (f_path, uploader, f_type, pid, status) in enumerate(pending_large_files, 1):
                    if not f_path.exists(): continue
                    
                    smart_sleep()
                    
                    status_flag = "🔙 断点/重试" if status in ['pending', 'failed'] else "🆕 新文件"
                    print(f"\n[{i}/{len(pending_large_files)}] 任务启动 ({status_flag})...")
                    
                    if f_type == "small":
                        # 小文件在 Phase 2 解析目标目录（Phase 1 零 API 消耗放行）
                        pid = get_target_pid(client, f_path)
                        upload_small_file(f_path, client, state, pid)
                    else:
                        upload_large_file_manual(f_path, client, state, session, uploader, pid)
    
    cleanup_empty_dirs(LOCAL_FOLDER)
    print("\n✨ 全部完成！")

if __name__ == "__main__":
    SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", 1800))
    
    try:
        while True:
            with STATS_LOCK:
                STATS = {
                    "total": 0, "rapid_success": 0, "regular_success": 0,
                    "skipped": 0, "failed": 0, "ignored": 0, "total_size": 0
                }
            
            main()
            print_summary() 
            
            print(f"\n💤 本轮结束，休眠 {SCAN_INTERVAL} 秒等待新文件...\n")
            time.sleep(SCAN_INTERVAL)
            
    except Exception as e:
        logger.error(f"守护进程发生严重异常: {e}")
        archive_log()