#!/bin/bash

# --- 脚本安全设置 ---
# set -e: 命令失败时立即退出脚本
# set -u: 使用未定义的变量时报错
# set -o pipefail: 管道命令中任何一步失败，都算作整个管道失败
set -euo pipefail

# --- 带颜色的日志函数 ---
# 提供标准化的、易于阅读的控制台输出
log_info() { echo -e "\e[34m[信息]\e[0m $*"; }
log_success() { echo -e "\e[32m[成功]\e[0m $*"; }
log_warn() { echo -e "\e[33m[警告]\e[0m $*"; }
log_error() { echo -e "\e[31m[错误]\e[0m $*"; exit 1; }

# ==============================================================================
# --- 配置加载 ---
# 所有配置均从环境变量加载，并提供合理的默认值
# ==============================================================================

# --- 通用工作流控制 ---
RUN_UPDATE_DB=${RUN_UPDATE_DB:-true}          # 控制是否执行数据库更新步骤
RUN_GENERATE_STRM=${RUN_GENERATE_STRM:-true}  # 控制是否执行STRM文件生成步骤
EMBY_ENABLE_SCAN=${EMBY_ENABLE_SCAN:-false}   # 控制是否在流程结束后触发Emby扫描
REGENERATE_ALL=${REGENERATE_ALL:-false}       # 控制是否根据历史库全量重新生成

# --- p115updatedb 相关参数 ---
P115_COOKIES_PATH="${P115_COOKIES_PATH:-}"    # 115 Cookies 文件在容器内的路径
P115_DB_PATH="${P115_DB_PATH:-/data/115file.db}" # 历史数据库的最终存储路径
P115_INTERVAL="${P115_INTERVAL:-30}"         # p115updatedb 的任务间隔时间
# 注意: 我们会将115网盘数据导出到一个临时文件，以保证操作的原子性
TEMP_DB_PATH="${P115_DB_PATH}.temp"
# 需要扫描的115网盘目录ID或路径，多个路径用空格分隔 (例如 "0" 或 "/电影 /电视剧")
P115_TARGET_DIR="${P115_TARGET_DIR:-0}"

# --- generate_strm.py 相关参数 ---
OUTPUT_DIR="${OUTPUT_DIR:-/output}"            # STRM 文件的输出目录
LOG_FILE="${LOG_FILE:-/data/logs/strm.log}"    # generate_strm.py 的日志文件路径
GENERATED_DB_PATH="${GENERATED_DB_PATH:-/data/generated.db}" # 已生成记录的数据库路径
DEFAULT_PLAY_URL="http://localhost:8000/{name2}?id={id}&pickcode={pickcode}&sha1={sha1}&name={name}&path={path}"
PLAY_URL_TEMPLATE="${PLAY_URL_TEMPLATE:-$DEFAULT_PLAY_URL}"
VIDEO_EXTS="${VIDEO_EXTS:-.mp4,.mkv,.avi,.wmv,.ts,.iso,.mov}"
SUBTITLE_EXTS="${SUBTITLE_EXTS:-.ass,.srt,.ssa,.sub,.idx,.sup}"
NUM_WORKERS="${NUM_WORKERS:-8}"                # generate_strm.py 的工作线程数
LOG_LEVEL="${LOG_LEVEL:-info}"                 # generate_strm.py 的日志级别
DOWNLOAD_SUBTITLE="${DOWNLOAD_SUBTITLE:-true}" # 是否下载字幕
DOWNLOAD_TIMEOUT="${DOWNLOAD_TIMEOUT:-30}"     # 字幕下载超时时间（秒）
DOWNLOAD_RETRIES="${DOWNLOAD_RETRIES:-3}"      # 字幕下载重试次数
DOWNLOAD_DELAY="${DOWNLOAD_DELAY:-1}"          # 字幕下载间隔（秒）
FORCE_REPAIR="${FORCE_REPAIR:-}"               # 强制修复包含特定关键词的路径

# --- Emby 扫描相关参数 ---
EMBY_SERVER_URL="${EMBY_SERVER_URL:-}"         # Emby 服务器地址
EMBY_API_KEY="${EMBY_API_KEY:-}"               # Emby API Key
# 需要扫描的媒体库ID，多个ID用逗号分隔
EMBY_LIBRARY_IDS="${EMBY_LIBRARY_IDS:-}"
EMBY_SCAN_TIMEOUT="${EMBY_SCAN_TIMEOUT:-30}"   # Emby API 请求超时时间（秒）


# ==============================================================================
# --- 各个工作流步骤的辅助函数 ---
# ==============================================================================

# 打印最终生效的配置信息
print_config() {
    log_info "使用以下配置进行初始化:"
    echo "  - 是否执行DB更新: $RUN_UPDATE_DB"
    echo "  - 是否生成STRM文件: $RUN_GENERATE_STRM"
    echo "  - 是否扫描Emby: $EMBY_ENABLE_SCAN"
    echo "  - 全量重新生成: $REGENERATE_ALL"
    echo "---"
    echo "  - 历史DB路径: $P115_DB_PATH"
    echo "  - 临时DB路径: $TEMP_DB_PATH"
    echo "  - 115扫描目标: $P115_TARGET_DIR"
    echo "---"
    echo "  - STRM输出目录: $OUTPUT_DIR"
    echo "  - 是否下载字幕: $DOWNLOAD_SUBTITLE"
    echo "---"
    echo "  - Emby服务器地址: $EMBY_SERVER_URL"
    echo "  - Emby媒体库ID: $EMBY_LIBRARY_IDS"
}

# 步骤1: 运行 p115updatedb
run_update_db() {
    if [ "$RUN_UPDATE_DB" != "true" ]; then
        log_info "根据 RUN_UPDATE_DB 的设置，跳过数据库更新步骤。"
        return
    fi

    log_info "正在运行 p115updatedb..."

    if [ -z "$P115_COOKIES_PATH" ]; then
        log_warn "环境变量 P115_COOKIES_PATH 未设置，跳过 p115updatedb 执行。"
        return
    fi
    if [ ! -f "$P115_COOKIES_PATH" ]; then
        log_error "Cookies 文件未找到: $P115_COOKIES_PATH"
    fi

    local CMD=("p115updatedb")
    CMD+=("--cookies-path" "$P115_COOKIES_PATH")
    CMD+=("--dbfile" "$TEMP_DB_PATH") # 始终写入到临时文件
    CMD+=("--interval" "$P115_INTERVAL")

    # 将 P115_TARGET_DIR 字符串分割成数组，以支持带空格的路径
    read -r -a dirs <<< "$P115_TARGET_DIR"
    if [ ${#dirs[@]} -gt 0 ]; then
        CMD+=("${dirs[@]}")
    fi

    log_info "执行命令: ${CMD[*]}"
    "${CMD[@]}" # 如果此命令失败，`set -e` 会自动处理退出
    log_success "p115updatedb 执行完成。"
}

# 步骤2: 运行 generate_strm.py
run_generate_strm() {
    if [ "$RUN_GENERATE_STRM" != "true" ]; then
        log_info "根据 RUN_GENERATE_STRM 的设置，跳过STRM文件生成步骤。"
        return
    fi
    
    # 在全量模式下，temp.db 不是必须的
    if [ "$REGENERATE_ALL" != "true" ] && [ ! -f "$TEMP_DB_PATH" ]; then
        log_warn "未找到临时数据库文件 ($TEMP_DB_PATH) 且未开启全量生成模式，本次可能无增量文件，跳过STRM生成。"
        return
    fi

    log_info "正在运行 generate_strm.py..."

    local CMD=("python3" "/app/generate_strm.py")
    CMD+=("--db" "$TEMP_DB_PATH")
    CMD+=("--previous-db" "$P115_DB_PATH")
    CMD+=("--output" "$OUTPUT_DIR")
    CMD+=("--log" "$LOG_FILE")
    CMD+=("--generated-db" "$GENERATED_DB_PATH")
    CMD+=("--url-template" "$PLAY_URL_TEMPLATE")
    CMD+=("--exts" "$VIDEO_EXTS")
    CMD+=("--subtitle-exts" "$SUBTITLE_EXTS")
    CMD+=("--workers" "$NUM_WORKERS")
    CMD+=("--log-level" "$LOG_LEVEL")
    CMD+=("--download-timeout" "$DOWNLOAD_TIMEOUT")
    CMD+=("--download-retries" "$DOWNLOAD_RETRIES")
    CMD+=("--download-delay" "$DOWNLOAD_DELAY")

    if [ "$REGENERATE_ALL" = "true" ]; then
        CMD+=("--regenerate-all")
        log_warn "已启用全量重新生成模式！将忽略增量更新。"
    fi

    if [ "$DOWNLOAD_SUBTITLE" = "true" ]; then
        CMD+=("--download-subtitle")
    else
        CMD+=("--no-download-subtitle")
    fi

	if [ -n "$FORCE_REPAIR" ]; then
        CMD+=("--force-repair" "$FORCE_REPAIR")
        log_warn "已启用强制修复模式！关键词: $FORCE_REPAIR"
    fi

    log_info "执行命令: python3 /app/generate_strm.py ..."
    "${CMD[@]}" # 如果此命令失败，`set -e` 会自动处理退出

    # 成功运行后，清理临时数据库文件及其关联文件
    rm -f "$TEMP_DB_PATH" "${TEMP_DB_PATH}-shm" "${TEMP_DB_PATH}-wal"
    log_success "generate_strm.py 执行完成，并已清理临时数据库。"
}

# 步骤3: 触发 Emby 扫描
trigger_emby_scan() {
    # 仅当 STRM 生成步骤也执行时，才考虑执行扫描
    if [ "$RUN_GENERATE_STRM" != "true" ] || [ "$EMBY_ENABLE_SCAN" != "true" ]; then
        log_info "跳过 Emby 扫描。"
        return
    fi

    log_info "正在触发 Emby 媒体库扫描..."

    if [ -z "$EMBY_SERVER_URL" ] || [ -z "$EMBY_API_KEY" ]; then
        log_error "EMBY_SERVER_URL 或 EMBY_API_KEY 未设置，无法触发扫描。"
    fi

    # 确保 URL 末尾没有斜杠
    local server_url="${EMBY_SERVER_URL%/}"

    # 定义一个扫描单个媒体库的内部函数
    scan_library() {
        local library_id="$1"
        local url="$server_url/emby/Items/$library_id/Refresh?Recursive=true"
        log_info "正在扫描媒体库 ID: $library_id..."
        local response
        response=$(curl -s -o /dev/null -w "%{http_code}" \
            --connect-timeout 10 --max-time "$EMBY_SCAN_TIMEOUT" \
            -X POST "$url" -H "X-Emby-Token: $EMBY_API_KEY")

        if [ "$response" = "204" ]; then
            log_success "已成功触发媒体库 $library_id 的扫描任务。"
        else
            log_warn "触发媒体库 $library_id 扫描失败 (HTTP 状态码: $response)。"
        fi
    }

    if [ -n "$EMBY_LIBRARY_IDS" ]; then
        # 按逗号分割字符串，并依次扫描每个媒体库
        IFS=',' read -r -a ids <<< "$EMBY_LIBRARY_IDS"
        for id in "${ids[@]}"; do
            scan_library "$id"
        done
    else
        log_info "未指定特定媒体库ID，将扫描所有媒体库..."
        local url="$server_url/Library/Refresh"
        local response
        response=$(curl -s -o /dev/null -w "%{http_code}" \
            --connect-timeout 10 --max-time "$EMBY_SCAN_TIMEOUT" \
            -X POST "$url" -H "X-Emby-Token: $EMBY_API_KEY")

        if [ "$response" = "204" ]; then
            log_success "已成功触发所有媒体库的扫描任务。"
        else
            log_warn "触发所有媒体库扫描失败 (HTTP 状态码: $response)。"
        fi
    fi
}

# ==============================================================================
# --- 主执行逻辑 ---
# ==============================================================================

# 这是一个命令路由器。
# 如果用户在 `docker run` 或 `docker compose run` 后面提供了自定义命令 (如 "bash"),
# 脚本会直接执行该命令，而不是运行下面的自动化工作流。
if [ $# -gt 0 ] && ! [[ "$1" =~ ^- ]]; then
    log_info "接收到自定义命令，将直接执行: $*"
    exec "$@"
fi

# --- 默认的自动化工作流 ---
main() {
    # 创建必要的目录，以防万一
    mkdir -p /data/logs "$OUTPUT_DIR" "$(dirname "$P115_DB_PATH")" "$(dirname "$TEMP_DB_PATH")"
    
    print_config
    run_update_db
    run_generate_strm
    trigger_emby_scan
    log_success "所有任务已完成！🎉"
}

# 运行主工作流
main
