#!/usr/bin/env python3
import argparse
import logging
import os
import queue
import re
import shutil
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Any, Set, Optional, Tuple, List
from urllib.parse import quote, urlparse, urlunparse

import requests

# ===== 默认配置常量 =====
DEFAULT_CONFIG: Dict[str, Any] = {
    "p115_db_path": "temp.db",
    "previous_db": "p115file.db",
    "output_dir": Path("output"),
    "log_file": Path("generate_strm.log"),
    "generated_db_path": Path("generated.db"),
    "video_exts": {".mp4", ".mkv", ".avi", ".ts", ".mov", ".wmv", ".iso"},
    "subtitle_exts": {".ass", ".srt", ".ssa", ".sub", ".smi", ".idx", ".sup"},
    "play_url_template": "http://aaa.bbb.com:8000/<url/{name}?pickcode={pickcode}&id={id}&sha1={sha1}<encode>",
    "download_subtitle": True,
    "num_workers": 8,
    "queue_size": 5000,
    "log_level": "info",
    "download_timeout": 30,
    "download_retries": 3,
    "download_delay": 1.0,
    "db_insert_buffer_size": 1000,
    "regenerate_all": False,
}

# 日志级别映射
LOG_LEVELS: Dict[str, int] = {"info": logging.INFO, "debug": logging.DEBUG}

# 正则表达式
SHA1_REGEX = re.compile(r'^[a-fA-F0-9]{40}$')

# Sentinel object to signal the end of the queue
DB_WRITER_SENTINEL = None


class StrmGenerator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        self.output_dir: Path = Path(self.config["output_dir"])
        self.generated_db_path: Path = Path(self.config["generated_db_path"])
        self.p115_db_path: Path = Path(self.config["p115_db_path"])
        self.log_file: Path = Path(self.config["log_file"])
        self.previous_db_path: Optional[Path] = Path(self.config["previous_db"]) if self.config.get("previous_db") else None

        self.logger = self._setup_logger()
        self.display_mode()

        self.video_exts: Set[str] = self.config["video_exts"]
        self.subtitle_exts: Set[str] = self.config["subtitle_exts"]
        self.num_workers: int = self.config["num_workers"]
        self.download_subtitle: bool = self.config["download_subtitle"]

        self.task_queue: queue.Queue = queue.Queue(maxsize=self.config["queue_size"])
        self.db_writer_queue: queue.Queue = queue.Queue(maxsize=self.config["queue_size"])

        self.producer_done_event = threading.Event()
        self.lock = threading.Lock()

        self.count_total = 0
        self.count_success = 0
        self.count_skipped_ext = 0
        self.count_skipped_no_sha1 = 0
        self.count_error = 0
        self.count_subtitle_downloaded = 0
        self.count_subtitle_skipped = 0
        
        self.last_download_time = 0
        self.download_lock = threading.Lock()

    def _setup_logger(self) -> logging.Logger:
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        log_level_name = self.config.get("log_level", "info").lower()
        log_level = LOG_LEVELS.get(log_level_name, logging.INFO)

        log_format = '%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s'
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(self.log_file, encoding='utf-8')
            ]
        )
        return logging.getLogger(__name__)

    def display_mode(self):
        log_level = self.logger.getEffectiveLevel()
        mode_desc = "详细模式" if log_level == logging.DEBUG else "简单模式"
        self.logger.info(f"以 {mode_desc} 运行，日志级别: {logging.getLevelName(log_level)}")

    @staticmethod
    def cleanup_sqlite_temp_files(db_path: Optional[Path]):
        if not db_path:
            return
        try:
            for ext in ["-shm", "-wal"]:
                temp_file = db_path.with_suffix(db_path.suffix + ext)
                if temp_file.exists():
                    temp_file.unlink()
                    logging.debug(f"已清理临时文件: {temp_file}")
        except Exception as e:
            logging.warning(f"清理临时文件 {db_path} 时出现警告: {e}")

    def init_generated_db(self):
        try:
            with sqlite3.connect(self.generated_db_path) as conn:
                cur = conn.cursor()
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS generated (
                        sha1 TEXT PRIMARY KEY,
                        id INTEGER NOT NULL,
                        path TEXT NOT NULL,
                        generated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sha1 ON generated(sha1)")
                conn.commit()
            self.logger.info(f"成功初始化或连接已生成记录数据库: {self.generated_db_path}")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"初始化数据库失败: {e}")
            return False

    def get_files_to_process(self) -> List[Tuple]:
        """获取需要处理的文件列表。支持增量、全量以及强制修复模式。"""
        if self.config.get("regenerate_all"):
            # --- 全量重新生成模式 ---
            self.logger.info("检测到 --regenerate-all 参数，将从历史数据库重新生成所有文件...")
            if not self.previous_db_path or not self.previous_db_path.exists():
                self.logger.error(f"历史数据库不存在: {self.previous_db_path}，无法执行全量生成。")
                return []
            
            try:
                with sqlite3.connect(f"file:{self.previous_db_path}?mode=ro", uri=True) as conn:
                    query = """
                        SELECT d.sha1, d.pickcode, d.id, d.name, d.size, json_extract(e.fs, '$.dst_path') AS path
                        FROM data AS d JOIN event AS e ON d.id = e.id WHERE d.is_dir = 0
                    """
                    results = conn.execute(query).fetchall()
                    self.logger.info(f"从历史数据库中查询到 {len(results)} 个文件需要重新生成。")
                    return results
            except sqlite3.Error as e:
                self.logger.error(f"从历史数据库查询文件失败: {e}")
                return []
        
        # --- 增量模式 & 强制修复模式逻辑 ---
        results = []
        try:
            if not self.p115_db_path.exists():
                if self.previous_db_path and self.previous_db_path.exists():
                    self.logger.info("未发现临时数据库文件，本次无文件需要处理。")
                    return []
                else:
                    self.logger.error(f"数据库文件不存在: {self.p115_db_path}")
                    sys.exit(1)

            # 1. 正常计算增量
            with sqlite3.connect(f"file:{self.p115_db_path}?mode=ro", uri=True) as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM data WHERE is_dir = 0")
                current_ids = {row[0] for row in cur.fetchall()}
                
                previous_ids: Set[int] = set()
                if self.previous_db_path and self.previous_db_path.exists():
                    cur.execute("ATTACH DATABASE ? AS previous", (str(self.previous_db_path),))
                    cur.execute("SELECT id FROM previous.data WHERE is_dir = 0")
                    previous_ids = {row[0] for row in cur.fetchall()}
                    cur.execute("DETACH DATABASE previous")

                incremental_ids = current_ids - previous_ids
                
                if incremental_ids:
                    self.logger.info(f"发现 {len(incremental_ids)} 个新增或变更的文件记录。")
                    cur.execute("ATTACH DATABASE ? AS generated", (str(self.generated_db_path),))
                    id_placeholders = ','.join('?' for _ in incremental_ids)
                    query = f"""
                        SELECT d.sha1, d.pickcode, d.id, d.name, d.size, json_extract(e.fs, '$.dst_path') AS path
                        FROM data AS d JOIN event AS e ON d.id = e.id
                        LEFT JOIN generated.generated AS g ON d.sha1 = g.sha1
                        WHERE d.id IN ({id_placeholders}) AND d.is_dir = 0 AND g.sha1 IS NULL
                    """
                    cur.execute(query, list(incremental_ids))
                    results.extend(cur.fetchall())
                    self.logger.info(f"过滤后，有 {len(results)} 个常规增量文件需要处理。")

            # 2. 强制修复逻辑 (穿透双层防御)
            force_repair_keyword = self.config.get("force_repair")
            if force_repair_keyword:
                self.logger.warning(f"🔨 启动强制修复模式，关键词: '{force_repair_keyword}'")
                with sqlite3.connect(f"file:{self.p115_db_path}?mode=ro", uri=True) as conn:
                    cur = conn.cursor()
                    query = """
                        SELECT d.sha1, d.pickcode, d.id, d.name, d.size, json_extract(e.fs, '$.dst_path') AS path
                        FROM data AS d JOIN event AS e ON d.id = e.id
                        WHERE d.is_dir = 0 AND json_extract(e.fs, '$.dst_path') LIKE ?
                    """
                    like_pattern = f"%{force_repair_keyword}%"
                    cur.execute(query, (like_pattern,))
                    repair_results = cur.fetchall()

                if repair_results:
                    # 将需要修复的文件从 generated.db 中删除，确保护航生成
                    with sqlite3.connect(self.generated_db_path) as g_conn:
                        g_cur = g_conn.cursor()
                        sha1s_to_delete = [r[0] for r in repair_results]
                        id_placeholders = ','.join('?' for _ in sha1s_to_delete)
                        g_cur.execute(f"DELETE FROM generated WHERE sha1 IN ({id_placeholders})", sha1s_to_delete)
                        g_conn.commit()
                        self.logger.debug(f"已清理 {g_cur.rowcount} 条旧记录，准备重新生成。")

                    # 将修复结果合并到总任务列表，并去重
                    existing_sha1s = {row[0] for row in results}
                    repair_added = 0
                    for row in repair_results:
                        if row[0] not in existing_sha1s:
                            results.append(row)
                            repair_added += 1
                    self.logger.info(f"成功将 {repair_added} 个修复文件加入生成队列。")
                else:
                    self.logger.warning(f"未找到路径中包含 '{force_repair_keyword}' 的文件。")

            if not results:
                self.logger.info("没有发现需要处理的新文件或修复文件。")
                return []
                
            return results

        except sqlite3.Error as e:
            self.logger.error(f"获取待处理文件失败: {e}")
            return []

    def producer(self):
        try:
            files_to_process = self.get_files_to_process()
            self.logger.info(f"开始将 {len(files_to_process)} 个任务放入队列。")

            for row in files_to_process:
                sha1, pickcode, file_id, name, size, path = row
                
                with self.lock:
                    self.count_total += 1
                
                ext = Path(name).suffix.lower()
                file_type = None
                if ext in self.video_exts:
                    file_type = "video"
                elif ext in self.subtitle_exts:
                    file_type = "subtitle"
                else:
                    with self.lock:
                        self.count_skipped_ext += 1
                    self.logger.debug(f"后缀不匹配，跳过 → {name}")
                    continue

                if not sha1 or not SHA1_REGEX.match(sha1):
                    with self.lock:
                        self.count_skipped_no_sha1 += 1
                    self.logger.debug(f"SHA1无效，跳过 → {name}")
                    continue

                self.task_queue.put((sha1, pickcode, file_id, name, size, path, file_type))
            
            self.logger.info("所有文件任务已成功放入队列。")
        except Exception as e:
            self.logger.error(f"生产者线程发生未知错误: {e}", exc_info=True)
        finally:
            self.producer_done_event.set()

    def db_writer(self):
        buffer: List[Tuple] = []
        buffer_size: int = self.config["db_insert_buffer_size"]
        
        while True:
            try:
                record = self.db_writer_queue.get()
                if record is DB_WRITER_SENTINEL:
                    if buffer:
                        self._write_batch_to_db(buffer)
                    break
                
                buffer.append(record)
                if len(buffer) >= buffer_size:
                    self._write_batch_to_db(buffer)
                    buffer.clear()
            except Exception as e:
                self.logger.error(f"数据库写入线程发生错误: {e}")

    def _write_batch_to_db(self, records: List[Tuple]):
        try:
            with sqlite3.connect(self.generated_db_path) as conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO generated (sha1, id, path) VALUES (?, ?, ?)",
                    records
                )
                conn.commit()
                self.logger.debug(f"批量写入 {len(records)} 条记录到数据库成功。")
        except sqlite3.Error as e:
            self.logger.error(f"批量写入数据库失败: {e}")

    def _generate_play_url(self, template: str, data: Dict[str, Any]) -> str:
        """根据模板和数据生成播放URL，并处理编码"""
        raw_url = template
        for key, value in data.items():
            placeholder = f"{{{key}}}"
            raw_url = raw_url.replace(placeholder, str(value))

        should_encode = '<encode>' in raw_url
        if should_encode:
            raw_url = raw_url.replace('<encode>', '', 1)
        
        if not should_encode:
            return raw_url
        
        parsed = urlparse(raw_url)
        encoded_path = quote(parsed.path, safe='/:')
        return urlunparse(
            (parsed.scheme, parsed.netloc, encoded_path, parsed.params, parsed.query, parsed.fragment)
        )

    def consumer(self):
        """消费者：处理单个文件任务（生成strm或下载字幕）"""
        while not (self.producer_done_event.is_set() and self.task_queue.empty()):
            try:
                sha1, pickcode, file_id, name, size, path, file_type = self.task_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            
            if not path:
                self.logger.warning(f"文件路径为空，无法处理 → {name}")
                with self.lock: self.count_error += 1
                self.task_queue.task_done()
                continue
            
            try:
                relative_dir = Path(path.strip("/")).parent
                target_dir = self.output_dir / relative_dir
                target_dir.mkdir(parents=True, exist_ok=True)

                url_data = {"name": name, "path": path, "pickcode": pickcode, "id": file_id, "sha1": sha1}
                url_template = self.config["play_url_template"]
                
                if file_type == "video":
                    file_stem = Path(name).stem
                    strm_file = target_dir / f"{file_stem}.strm"
                    play_url = self._generate_play_url(url_template, url_data)
                    
                    strm_file.write_text(play_url, encoding='utf-8')
                    self.logger.debug(f"已生成 → {strm_file.relative_to(self.output_dir)}")
                    
                    with self.lock: self.count_success += 1
                    self.db_writer_queue.put((sha1, file_id, path))

                elif file_type == "subtitle" and self.download_subtitle:
                    subtitle_file = target_dir / name
                    subtitle_url = self._generate_play_url(url_template, url_data)

                    if self.download_file(subtitle_url, subtitle_file):
                        self.logger.debug(f"已下载字幕 → {subtitle_file.relative_to(self.output_dir)}")
                        with self.lock:
                            self.count_subtitle_downloaded += 1
                            self.count_success += 1
                        self.db_writer_queue.put((sha1, file_id, path))
                    else:
                        with self.lock:
                            self.count_subtitle_skipped += 1
                            self.count_error += 1

                self.task_queue.task_done()

            except Exception as e:
                self.logger.error(f"处理文件 {name} 时发生未知错误: {e}")
                with self.lock: self.count_error += 1
                self.task_queue.task_done()

    def download_file(self, url: str, destination: Path) -> bool:
        """带重试机制的文件下载"""
        retries = self.config["download_retries"]
        timeout = self.config["download_timeout"]
        delay = self.config["download_delay"]

        for attempt in range(retries):
            try:
                with self.download_lock:
                    elapsed = time.time() - self.last_download_time
                    if elapsed < delay:
                        time.sleep(delay - elapsed)
                    self.last_download_time = time.time()

                with requests.get(url, timeout=timeout, stream=True) as r:
                    r.raise_for_status()
                    with destination.open('wb') as f:
                        shutil.copyfileobj(r.raw, f)
                    return True
            except requests.RequestException as e:
                msg = f"下载失败 (尝试 {attempt + 1}/{retries}) → {destination.name}: {e}"
                if attempt < retries - 1:
                    self.logger.warning(msg)
                    time.sleep(1)
                else:
                    self.logger.error(msg)
        return False
        
    def print_summary(self):
        """输出任务总结"""
        self.logger.info("="*20 + " 任务完成 " + "="*20)
        self.logger.info(f"总计文件数 (新增):  {self.count_total}")
        self.logger.info(f"成功处理:           {self.count_success}")
        self.logger.info(f"  - 成功生成STRM:   {self.count_success - self.count_subtitle_downloaded}")
        self.logger.info(f"  - 成功下载字幕:   {self.count_subtitle_downloaded}")
        self.logger.info(f"跳过 (后缀不匹配):  {self.count_skipped_ext}")
        self.logger.info(f"跳过 (无 SHA1):     {self.count_skipped_no_sha1}")
        self.logger.info(f"失败 (字幕下载):    {self.count_subtitle_skipped}")
        self.logger.info(f"失败 (其他错误):    {self.count_error - self.count_subtitle_skipped}")
        self.logger.info("="*52)

    def update_database_file(self):
        """处理完成后，更新数据库文件"""
        if self.config.get("regenerate_all"):
            self.logger.info("全量重新生成模式下，不更新历史数据库。")
            return
        
        if not self.p115_db_path.exists():
            return

        if not self.previous_db_path:
            self.logger.warning("未配置历史数据库路径 (previous_db)，跳过数据库更新。")
            return

        try:
            if not self.previous_db_path.exists():
                self.logger.info("未发现历史数据库，将当前数据库重命名为历史库...")
                self.p115_db_path.rename(self.previous_db_path)
                self.logger.info(f"数据库已初始化: {self.previous_db_path}")
            else:
                self.logger.info(f"开始将 {self.p115_db_path} 合并到 {self.previous_db_path}...")
                
                with sqlite3.connect(self.previous_db_path) as conn:
                    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, sha1 TEXT, size INTEGER, pickcode TEXT, type INTEGER, ctime INTEGER, mtime INTEGER, is_dir INTEGER, is_collect INTEGER, is_alive INTEGER, extra BLOB, updated_at DATETIME, _triggered INTEGER)")
                    conn.execute("CREATE TABLE IF NOT EXISTS event (_id INTEGER PRIMARY KEY, id INTEGER, old JSON, diff JSON, fs JSON, created_at DATETIME)")
                    
                    cur = conn.cursor()
                    cur.execute("ATTACH DATABASE ? AS temp_db", (str(self.p115_db_path),))
                    cur.execute("INSERT OR IGNORE INTO main.data SELECT * FROM temp_db.data")
                    cur.execute("INSERT OR IGNORE INTO main.event SELECT * FROM temp_db.event")
                    conn.commit()
                    cur.execute("DETACH DATABASE temp_db")

                self.logger.info("数据库合并完成。")
                self.p115_db_path.unlink()
                self.logger.info(f"已删除临时数据库: {self.p115_db_path}")

        except sqlite3.Error as e:
            self.logger.error(f"更新或合并数据库文件失败: {e}")
        except Exception as e:
            self.logger.error(f"更新数据库文件时发生未知错误: {e}")

    def run(self):
        """主执行函数"""
        start_time = time.time()
        self.logger.info("开始执行 .strm 生成和字幕下载任务...")
        
        self.cleanup_sqlite_temp_files(self.previous_db_path)
        self.cleanup_sqlite_temp_files(self.p115_db_path)

        if not self.init_generated_db():
            return

        producer_thread = threading.Thread(target=self.producer, name="Producer")
        db_writer_thread = threading.Thread(target=self.db_writer, name="DBWriter")
        
        producer_thread.start()
        db_writer_thread.start()

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix="Consumer") as executor:
            producer_thread.join()
            self.logger.info("生产者线程已完成，等待消费者处理剩余任务...")

            consumer_futures = [executor.submit(self.consumer) for _ in range(self.num_workers)]
            for future in consumer_futures:
                future.result()

        self.logger.info("所有消费者线程已完成。")
        
        self.db_writer_queue.put(DB_WRITER_SENTINEL)
        db_writer_thread.join()
        self.logger.info("数据库写入线程已完成。")

        self.update_database_file()
        self.cleanup_sqlite_temp_files(self.previous_db_path)
        
        self.print_summary()
        elapsed_time = time.time() - start_time
        self.logger.info(f"总耗时: {elapsed_time:.2f} 秒")


def get_config() -> Dict[str, Any]:
    """加载配置：优先级为 命令行 > 环境变量 > 默认值"""
    config = DEFAULT_CONFIG.copy()

    # 1. 从环境变量加载
    env_mapping = {
        "P115_DB_PATH": "p115_db_path", "PREVIOUS_DB": "previous_db", "OUTPUT_DIR": "output_dir",
        "LOG_FILE": "log_file", "GENERATED_DB_PATH": "generated_db_path", "PLAY_URL_TEMPLATE": "play_url_template",
        "VIDEO_EXTS": "video_exts", "SUBTITLE_EXTS": "subtitle_exts", "NUM_WORKERS": "num_workers",
        "QUEUE_SIZE": "queue_size", "LOG_LEVEL": "log_level", "DOWNLOAD_SUBTITLE": "download_subtitle",
        "DOWNLOAD_TIMEOUT": "download_timeout", "DOWNLOAD_RETRIES": "download_retries", "DOWNLOAD_DELAY": "download_delay",
        "REGENERATE_ALL": "regenerate_all","DB_INSERT_BUFFER_SIZE": "db_insert_buffer_size",
    }
    for env_key, config_key in env_mapping.items():
        value = os.getenv(env_key)
        if value is None:
            continue
        
        if config_key in {"num_workers", "queue_size", "download_timeout", "download_retries", "db_insert_buffer_size"}:
            try: config[config_key] = int(value)
            except (ValueError, TypeError): pass
        elif config_key == "download_delay":
            try: config[config_key] = float(value)
            except (ValueError, TypeError): pass
        elif config_key in {"download_subtitle", "regenerate_all"}:
            config[config_key] = value.lower() in ['true', '1', 'yes', 'on']
        elif config_key in {"video_exts", "subtitle_exts"}:
             config[config_key] = {f".{ext.strip().lstrip('.')}" for ext in value.split(",")}
        else:
            config[config_key] = value

    # 2. 从命令行参数加载
    parser = argparse.ArgumentParser(description="根据 p115 数据库增量生成 .strm 文件和下载字幕。")
    parser.add_argument("--db", help=f"当前数据库路径 (temp.db)")
    parser.add_argument("--previous-db", help=f"历史数据库路径 (p115file.db)")
    parser.add_argument("--output", help=f"输出目录")
    parser.add_argument("--exts", help="支持的视频后缀,逗号分隔")
    parser.add_argument("--subtitle-exts", help="支持的字幕后缀,逗号分隔")
    parser.add_argument("--log", help=f"日志文件路径")
    parser.add_argument("--generated-db", help=f"记录已生成文件的数据库")
    parser.add_argument("--url-template", help="播放链接模板")
    parser.add_argument("--workers", type=int, help=f"工作线程数量")
    parser.add_argument("--queue-size", type=int, help=f"工作队列大小")
    parser.add_argument("--db-buffer-size", type=int, help=f"数据库批量写入缓冲区大小")
    parser.add_argument("--log-level", choices=["info", "debug"], help=f"日志级别")
    parser.add_argument("--download-subtitle", action="store_true", default=None, help="强制下载字幕文件")
    parser.add_argument("--no-download-subtitle", action="store_false", dest="download_subtitle", help="强制不下载字幕文件")
    parser.add_argument("--download-timeout", type=int, help="下载超时时间（秒）")
    parser.add_argument("--download-retries", type=int, help="下载重试次数")
    parser.add_argument("--download-delay", type=float, help="下载间隔时间（秒）")
    parser.add_argument("--regenerate-all", action="store_true", help="忽略增量，根据历史数据库重新生成所有STRM文件")
    parser.add_argument("--force-repair", type=str, default="", help="强制重新生成路径中包含该关键词的 strm 文件（穿透增量防御）") # <-- 增加这一行

    args = parser.parse_args()

    arg_map = {
        "db": "p115_db_path", "previous_db": "previous_db", "output": "output_dir",
        "log": "log_file", "generated_db": "generated_db_path", "url_template": "play_url_template",
        "workers": "num_workers", "queue_size": "queue_size", "log_level": "log_level","db_buffer_size": "db_insert_buffer_size",
        "download_subtitle": "download_subtitle", "download_timeout": "download_timeout",
        "download_retries": "download_retries", "download_delay": "download_delay",
        "regenerate_all": "regenerate_all",
        "force_repair": "force_repair",
    }
    for arg_key, config_key in arg_map.items():
        value = getattr(args, arg_key)
        if value is not None:
            config[config_key] = value

    if args.exts:
        config["video_exts"] = {f".{ext.strip().lstrip('.')}" for ext in args.exts.split(",")}
    if args.subtitle_exts:
        config["subtitle_exts"] = {f".{ext.strip().lstrip('.')}" for ext in args.subtitle_exts.split(",")}

    return config


def main():
    try:
        config = get_config()
        
        # 在全量模式下，temp.db 不是必须的
        if not config.get("regenerate_all"):
            p115_db_path = Path(config["p115_db_path"])
            if not p115_db_path.exists():
                if config.get("previous_db") and Path(config["previous_db"]).exists():
                    print("未发现临时数据库文件 (temp.db)，认为本次无增量更新，正常退出。")
                    sys.exit(0)
                else:
                    print(f"错误: 数据库文件不存在: {p115_db_path}")
                    sys.exit(1)

        generator = StrmGenerator(config)
        generator.run()

    except KeyboardInterrupt:
        print("\n任务被用户中断。")
        sys.exit(130)
    except Exception as e:
        logging.getLogger(__name__).error(f"发生未捕获的异常: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
