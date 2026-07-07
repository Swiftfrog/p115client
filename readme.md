# p115client

`p115client` is a Python client for 115 cloud storage. It wraps 115 web, app, and open APIs with sync and async call styles.

This fork keeps the upstream API shape, with extra fixes and debugging notes for `p115updatedb` metadata sync.

## Install

From this fork:

```console
pip install -U git+https://github.com/Swiftfrog/p115client.git@main
pip install -U "p115updatedb @ git+https://github.com/Swiftfrog/p115client.git@main#subdirectory=modules/p115updatedb"
```

From PyPI:

```console
pip install -U p115client
```

## Basic Usage

Create a client from a cookie file:

```python
from pathlib import Path
from p115client import P115Client

client = P115Client(Path("~/115-cookies.txt").expanduser())
resp = client.fs_files({"cid": 0, "limit": 20, "show_dir": 1})
```

Call APIs synchronously or asynchronously:

```python
resp = client.fs_files({"cid": 0})
resp = await client.fs_files({"cid": 0}, async_=True)
```

## Metadata Sync

`p115updatedb` exports 115 metadata into SQLite:

```console
python -m p115updatedb /TWO \
  -cp ./config/115-cookies.txt \
  -f ./database/TWO_UPDATE.db \
  -i 3.0
```

For a local checkout without reinstalling:

```console
PYTHONPATH=/home/albert/p115client/modules/p115updatedb:/home/albert/p115client \
  /home/albert/p115mg/venv/bin/python /home/albert/p115mg/01_export_115_db.py /TWO \
  -cp /home/albert/p115mg/config/115-cookies.txt \
  -f /tmp/TWO_updatedb_test.db \
  -i 3.0
```

Check the generated database:

```console
sqlite3 /tmp/TWO_updatedb_test.db \
  'select count(*) as rows, sum(is_dir) as dirs, sum(not is_dir) as files from data;'
```

## Development

Useful checks:

```console
python -m py_compile p115client/client.py modules/p115updatedb/p115updatedb/updatedb.py
git diff -- p115client/client.py modules/p115updatedb/p115updatedb/updatedb.py
```

Run a focused `downfolders` probe:

```python
from pathlib import Path
from p115client import P115Client

client = P115Client(Path("/home/albert/p115mg/config/115-cookies.txt"))
resp = client.download_folders_app(
    {"pickcode": "fedbo6dvewzu6c3wei", "page": 1, "per_page": 10},
    app="chrome",
)
print(resp["state"], resp["errno"], resp["data"]["count"])
```

Run a full sync regression:

```console
PYTHONPATH=/home/albert/p115client/modules/p115updatedb:/home/albert/p115client \
  /home/albert/p115mg/venv/bin/python /home/albert/p115mg/01_export_115_db.py /TWO \
  -cp /home/albert/p115mg/config/115-cookies.txt \
  -f /tmp/TWO_updatedb_appver_fix.db \
  -i 3.0
```

Expected result:

```text
[GOOD] 3459850788520716428, upsert: 2223, remove: 0
Metadata sync finished successfully!
```

## Debug Notes

### `downfolders` 405

`P115Client.get_request()` injects this query parameter into dict params:

```text
app_ver=99.99.99.99
```

The browser request below succeeds:

```text
https://proapi.115.com/app/chrome/downfolders?pickcode=...&page=1&per_page=10
```

The same request can fail with `HTTP 405` when `app_ver=99.99.99.99` is appended. `download_folders_app()` now wraps its request function and removes that synthetic `app_ver` for this endpoint.

### App Path

For directory nodes, `p115updatedb` should use:

```python
iter_download_nodes(client, id, files=False, max_workers=None, app="chrome", ...)
```

That maps to:

```text
https://proapi.115.com/app/chrome/downfolders
```

The `/{app}/ufile/downfolders` variants such as `qipad`, `115ipad`, and `os_windows` may return `404` or `405`.

### Fallback Path

When `downfolders` is unavailable, directory walking can fall back to:

```text
https://webapi.115.com/files
```

`webapi /files` returns enough information for tree walking: `cid`, `pid`, `n`, `pc`, `path`, and `count`.

### Browser Comparison

When an endpoint works in the browser but fails in Python:

1. Export the exact Network request, preferably "Copy as cURL".
2. Compare query params first; for `downfolders`, ensure `app_ver=99.99.99.99` is absent.
3. Test with `curl`, `urllib.request`, and `P115Client.request` separately.
4. Do not commit cookies, HAR files, or local database outputs.

## Upstream

Original project:

```text
https://github.com/ChenyangGao/p115client
```
