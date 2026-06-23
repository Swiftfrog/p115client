# --- Stage 1: Base Image ---
# 使用一个官方的、轻量级的 Python 3.11 镜像作为基础
FROM python:3.13-slim

# --- Metadata ---
LABEL maintainer="albert"
LABEL description="A Docker image to run p115updatedb and generate STRM files for Emby."

# --- Environment Variables ---
# 设置工作目录
WORKDIR /app

# 防止 Python 写入 .pyc 文件，并设置无缓冲输出，便于在 Docker 日志中查看
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# --- System Dependencies ---
# 安装 curl (用于 Emby 扫描) 和其他可能需要的工具
# 清理 apt 缓存以保持镜像体积小
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# --- Python Dependencies ---
# 先安装 generate_strm.py 的依赖（利用缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Application Code ---
# 复制整个仓库的代码到工作目录中，以便本地编译和运行
COPY . /app/

# 确保所有复制的文件和目录对所有用户可读，目录可遍历（修复非 root 用户的 PermissionError 报错）
RUN chmod -R a+rX /app

# 安装项目本身的本地代码 (p115client、各个本地模块和 p115updatedb)
RUN pip install --no-cache-dir \
    /app/modules/p115cipher/ \
    /app/modules/p115pickcode/ \
    /app/modules/p115oss/ \
    /app/ \
    /app/modules/p115updatedb/

# 清理源码目录，避免 shadowing 导入冲突并减少镜像体积
RUN rm -rf /app/p115client /app/modules

# 赋予 entrypoint.sh 执行权限
RUN chmod +x ./entrypoint.sh

# --- Entrypoint ---
# 设置容器的入口点为我们的脚本
ENTRYPOINT ["./entrypoint.sh"]

# --- Default Command ---
# 当容器运行时，默认执行 entrypoint.sh 的主工作流
# 如果在 `docker run` 或 `compose.yaml` 中提供了其他命令，则会覆盖它
CMD []
