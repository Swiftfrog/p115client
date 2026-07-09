#!/usr/bin/env python3
from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from urllib.error import HTTPError

from p115client import P115Client


def show(label, fn) -> bool:
    try:
        resp = fn()
    except HTTPError as e:
        print(f"{label}: HTTP {e.code} {e.reason}")
        return False
    except Exception as e:
        print(f"{label}: {type(e).__name__}: {e}")
        return False
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, dict):
        print(
            f"{label}: state={resp.get('state')} errno={resp.get('errno')} "
            f"count={data.get('count')} next={data.get('has_next_page')}"
        )
    else:
        print(f"{label}: {resp!r}")
    return bool(resp.get("state"))


def main() -> int:
    parser = ArgumentParser(description="Probe 115 downfolders/downfiles endpoints.")
    parser.add_argument(
        "-cp",
        "--cookies-path",
        default="/home/albert/p115mg/config/115-cookies.txt",
        help="115 cookies file path",
    )
    parser.add_argument(
        "--pickcode",
        default="fedbo6dvewzu6c3wei",
        help="directory pickcode to test, defaults to /TWO in the local test account",
    )
    args = parser.parse_args()

    client = P115Client(Path(args.cookies_path))
    payload = {"pickcode": args.pickcode, "page": 1, "per_page": 10}
    ok_folders = show(
        "downfolders",
        lambda: client.download_folders_app(payload, app="chrome"),
    )
    ok_files = show(
        "downfiles",
        lambda: client.download_files_app(payload, app="chrome"),
    )
    return 0 if ok_folders and ok_files else 1


if __name__ == "__main__":
    raise SystemExit(main())
