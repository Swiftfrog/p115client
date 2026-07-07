# p115updatedb 405 排障与修复记录

记录时间：2026-07-03 20:59 CST

## 背景

在 `/home/albert/p115mg` 中运行：

```bash
python3 01_export_115_db.py test2 -cp ./config/115-cookies.txt -f database/fe.db -i 3.0
```

出现：

```text
Error during metadata sync: HTTP Error 405: Method Not Allowed
```

该脚本通过 `p115updatedb` 生成 SQLite DB，再读取 DB 生成 STRM。

## 对照实验

Docker 镜像 `dobirdcker/p115dbstrm:latest` 可正常运行同类流程：

```text
p115updatedb --cookies-path /data/cookies/115-cookies.txt --dbfile /data/database/p115fe.db.temp --interval 3 /Media
[GOOD] ... upsert: 47732, remove: 0
```

镜像内版本：

```text
p115updatedb (0, 0, 12)
p115client   (0, 0, 8)
```

宿主 `/home/albert/p115mg/venv` 版本：

```text
p115updatedb (0, 0, 12)
p115client   (0, 0, 9)
```

## 根因

`p115updatedb` 的核心同步逻辑基本一致，差异主要来自底层 `p115client`：

1. `p115client 0.0.9` 请求层会向 query 参数注入 `app_ver=99.99.99.99`。
2. 浏览器验证 `https://proapi.115.com/app/chrome/downfolders?pickcode=...&page=1&per_page=10` 可用，但同一请求一旦带上 `app_ver=99.99.99.99` 就会触发 405。
3. `/{app}/ufile/downfolders` 变体（例如 `qipad`、`115ipad`、`os_windows`）在当前测试中返回 404 或 405，不适合作为 `p115updatedb` 目录节点快路径。

因此，405 并非单纯的 cookie 失效，也不是 `p115updatedb` 算法整体坏掉，而是新版 `p115client` 的接口选择和请求参数导致 115 返回 405。

## 已修复内容

修复已提交并推送到 GitHub：

```text
repo:   git@github.com:Swiftfrog/p115client.git
branch: main
commit: 5e08114 fix: avoid 115 updatedb 405 regressions
```

修改文件：

```text
modules/p115updatedb/p115updatedb/updatedb.py
```

主要改动：

1. `download_folders_app()` 对请求加兼容 wrapper，移除 `p115client 0.0.9` 注入的 `app_ver=99.99.99.99`。
2. 在全量树同步中，目录节点拉取显式调用：

```python
iter_download_nodes(client, id, files=False, max_workers=None, app="chrome", **request_kwargs)
```

3. 将兼容请求参数覆盖到这些入口：

```text
updatedb
updatedb_one
updatedb_tree
updatedb_life_iter
```

4. `updatedb()` 中路径转 id 的 `get_id_to_path(...)` 也传入兼容后的 `request_kwargs`。

## 模块回归测试

使用仓库源码直接测试 `modules/p115updatedb`：

```bash
cd /home/albert/p115mg
PYTHONPATH=/home/albert/p115client/modules/p115updatedb \
PYTHONDONTWRITEBYTECODE=1 \
venv/bin/python -m p115updatedb /XYZ \
  -cp /home/albert/docker/p115db/115-cookies.txt \
  -f /tmp/p115_updatedb_module_xyz.db \
  -i 3.0
```

结果：

```text
[GOOD] 3450437724109138222, upsert: 2, remove: 0
```

DB 校验：

```sql
select count(*) as rows, sum(is_dir) as dirs, sum(not is_dir) as files from data;
```

结果：

```text
7|5|2
```

## 01_export_115_db.py 临时兼容

在 `/home/albert/p115mg/01_export_115_db.py` 中曾加入 monkey patch：

1. `iter_download_nodes(..., files=False)` 默认指定 app。
2. 移除 `app_ver=99.99.99.99`。

最初用 `android` 测试通过，后来按要求改为 `qipad` 并再次测试通过。

`qipad` 测试命令：

```bash
cd /home/albert/p115mg
PYTHONDONTWRITEBYTECODE=1 \
venv/bin/python 01_export_115_db.py /XYZ \
  -cp /home/albert/docker/p115db/115-cookies.txt \
  -f /tmp/p115_xyz_qipad_test.db \
  -i 3.0
```

结果：

```text
[GOOD] 3450437724109138222, upsert: 2, remove: 0
Metadata sync finished successfully!
```

DB 校验：

```text
7|5|2
```

## requirements.txt 调整

`/home/albert/p115mg/requirements.txt` 原先锁定旧提交：

```text
p115client @ git+https://github.com/Swiftfrog/p115client.git@7e08b4d92c999b21ff1d679ef51ecc22d409db5a
p115updatedb @ git+https://github.com/Swiftfrog/p115client.git@7e08b4d92c999b21ff1d679ef51ecc22d409db5a#subdirectory=modules/p115updatedb
```

这样不会安装到已推送的修复提交。现已改为跟随 `main`：

```text
p115client @ git+https://github.com/Swiftfrog/p115client.git@main
p115updatedb @ git+https://github.com/Swiftfrog/p115client.git@main#subdirectory=modules/p115updatedb
```

注意：跟随 `main` 可以自动拿最新修复，但可复现性弱于固定 commit。若后续需要生产稳定性，可以改为固定到已验证的新提交：

```text
5e08114d37aed6dc837ac4ba7f09593c8c7d7bd7
```

## 当前状态

- `/home/albert/p115client`：修复已提交并推送，`main` 与 `origin/main` 同步。
- `/home/albert/p115mg/01_export_115_db.py`：monkey patch 已改为 `qipad`，测试通过。
- `/home/albert/p115mg/requirements.txt`：已改为安装 GitHub `main` 最新代码。
- `/home/albert/p115mg` 是另一个工作区，存在大量既有本地改动，未在该目录执行提交。
