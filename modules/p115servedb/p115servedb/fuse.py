#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["main"]
__doc__ = """\
    🌍 115 数据库 FUSE 服务 🪩

⏰ 由于网盘对多线程访问的限制，请停用挂载目录的显示图标预览

1. Linux 要安装 libfuse：  https://github.com/libfuse/libfuse
2. MacOSX 要安装 MacFUSE： https://github.com/osxfuse/osxfuse
3. Windows 要安装 WinFsp： https://github.com/winfsp/winfsp
"""

from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from pathlib import Path

if __name__ == "__main__":  
    from sys import path

    path[0] = str(Path(__file__).parents[1])
    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
else:
    from .init import subparsers

    parser = subparsers.add_parser("fuse", description=__doc__, formatter_class=RawTextHelpFormatter)


def parse_args(argv: None | list[str] = None, /) -> Namespace:
    args = parser.parse_args(argv)
    if args.version:
        from p115servedb import __version__
        print(".".join(map(str, __version__)))
        raise SystemExit(0)
    return args


def main(argv: None | list[str] | Namespace = None, /):
    if isinstance(argv, Namespace):
        args = argv
    else:
        args = parse_args(argv)

    import logging
    import re

    from p115servedb.component.fuser import ServedbFuseOperations
    from p115servedb.component.log import logger
    from path_predicate import make_predicate

    mount_point = args.mount_point
    if not mount_point:
        from uuid import uuid4
        mount_point = str(uuid4())

    options = {
        "mountpoint": mount_point, 
        "allow_other": True, 
        "foreground": True, 
        "max_readahead": 0, 
        "noauto_cache": True, 
        "ro": True, 
    }
    if fuse_options := args.fuse_options:
        for option in fuse_options:
            if "=" in option:
                name, value = option.split("=", 1)
                if value:
                    options[name] = value
                else:
                    options.pop(name, None)
            else:
                options[option] = True

    log_level = args.log_level
    if log_level.isascii() and log_level.isdecimal():
        log_level = int(log_level)
    else:
        log_level = getattr(logging, log_level.upper(), logging.NOTSET)
    logger.setLevel(log_level)

    if args.fast_strm:
        predicate = make_predicate("""(
    path.is_dir() or
    path.media_type.startswith("image/") or
    path.suffix.lower() in (".nfo", ".ass", ".ssa", ".srt", ".idx", ".sub", ".txt", ".vtt", ".smi")
)""", type="expr")
    elif predicate := args.predicate or None:
        predicate = make_predicate(predicate, {"re": re}, type=args.predicate_type)
    if args.fast_strm:
        strm_predicate = make_predicate("""(
    path.media_type.startswith(("video/", "audio/")) and
    path.suffix.lower() != ".ass" or
    path.suffix.lower() in (".divx", ".iso", ".m2ts", ".swf", ".xvid")
)""", type="expr")
    elif strm_predicate := args.strm_predicate or None:
        strm_predicate = make_predicate(strm_predicate, {"re": re}, type=args.strm_predicate_type)

    from os.path import exists, abspath

    print(f"""
        👋 Welcome to use servedb fuse 👏

    mounted at: {abspath(mount_point)!r}
    FUSE options: {options!r}
    """)

    if not exists(mount_point):
        import atexit
        from os import makedirs, removedirs
        makedirs(mount_point)
        def remove_mount_point():
            try:
                removedirs(mount_point)
            except:
                pass
        atexit.register(remove_mount_point)

    # https://code.google.com/archive/p/macfuse/wikis/OPTIONS.wiki
    ServedbFuseOperations(
        args.dbfile, 
        args.cookies_path, 
        predicate=predicate, 
        strm_predicate=strm_predicate, 
        strm_origin=args.strm_origin, 
    ).run(**options)


parser.add_argument("mount_point", nargs="?", help="挂载路径")
parser.add_argument("-f", "--dbfile", required=True, help="数据库文件路径")
parser.add_argument("-cp", "--cookies-path", default="", help="cookies cookies 文件保存路径，默认为当前工作目录下的 115-cookies.txt（如果 115-cookies.txt 不存在，则使用 -o/--strm-origin 所指定的服务进行下载）")
parser.add_argument("-o", "--strm-origin", default="http://localhost:8000", help="strm 所用的 302 服务地址，默认为 'http://localhost:8000'")
parser.add_argument("-p1", "--predicate", help="断言，当断言的结果为 True 时，文件或目录会被显示")
parser.add_argument(
    "-t1", "--predicate-type", default="ignore", 
    choices=("ignore", "ignore-file", "expr", "lambda", "stmt", "module", "file", "re"), 
    help="""断言类型，默认值为 'ignore'
    - ignore       （默认值）gitignore 配置文本（有多个时用空格隔开），在文件路径上执行模式匹配，匹配成功则断言为 False
                   NOTE: https://git-scm.com/docs/gitignore#_pattern_format
    - ignore-file  接受一个文件路径，包含 gitignore 的配置文本（一行一个），在文件路径上执行模式匹配，匹配成功则断言为 False
                   NOTE: https://git-scm.com/docs/gitignore#_pattern_format
    - expr         表达式，会注入一个名为 path 的类 pathlib.Path 对象
    - lambda       lambda 函数，接受一个类 pathlib.Path 对象作为参数
    - stmt         语句，当且仅当不抛出异常，则视为 True，会注入一个名为 path 的类 pathlib.Path 对象
    - module       模块，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个类 pathlib.Path 对象作为参数
    - file         文件路径，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个类 pathlib.Path 对象作为参数
    - re           正则表达式，模式匹配，如果文件的名字匹配此模式，则断言为 True
""")
parser.add_argument("-p2", "--strm-predicate", help="strm 断言（优先级高于 -p1/--predicate），当断言的结果为 True 时，文件会被显示为带有 .strm 后缀的文本文件，打开后是链接")
parser.add_argument(
    "-t2", "--strm-predicate-type", default="filter", 
    choices=("filter", "filter-file", "expr", "lambda", "stmt", "module", "file", "re"), 
    help="""断言类型，默认值为 'filter'
    - filter       （默认值）gitignore 配置文本（有多个时用空格隔开），在文件路径上执行模式匹配，匹配成功则断言为 True
                   请参考：https://git-scm.com/docs/gitignore#_pattern_format
    - filter-file  接受一个文件路径，包含 gitignore 的配置文本（一行一个），在文件路径上执行模式匹配，匹配成功则断言为 True
                   请参考：https://git-scm.com/docs/gitignore#_pattern_format
    - expr         表达式，会注入一个名为 path 的类 pathlib.Path 对象
    - lambda       lambda 函数，接受一个类 pathlib.Path 对象作为参数
    - stmt         语句，当且仅当不抛出异常，则视为 True，会注入一个名为 path 的类 pathlib.Path 对象
    - module       模块，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个类 pathlib.Path 对象作为参数
    - file         文件路径，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个类 pathlib.Path 对象作为参数
    - re           正则表达式，模式匹配，如果文件的名字匹配此模式，则断言为 True
""")
parser.add_argument("-fs", "--fast-strm", action="store_true", help="""快速实现 媒体筛选 和 虚拟 strm，此命令优先级较高，相当于命令行指定

    --strm-predicate-type expr \\
    --strm-predicate '(
        path.media_type.startswith(("video/", "audio/")) and
        path.suffix.lower() != ".ass" or
        path.suffix.lower() in (".divx", ".iso", ".m2ts", ".swf", ".xvid")
    )' \\
    --predicate-type expr \\
    --predicate '(
        path.is_dir() or
        path.media_type.startswith("image/") or
        path.suffix.lower() in (".nfo", ".ass", ".ssa", ".srt", ".idx", ".sub", ".txt", ".vtt", ".smi")
    )'
""")
parser.add_argument(
    "-fo", "--fuse-option", dest="fuse_options", metavar="option", nargs="+", 
    help="""fuse 挂载选项，支持如下几种格式：
    - name         设置 name 选项
    - name=        取消 name 选项
    - name=value   设置 name 选项，值为 value
参考资料：
    - https://man7.org/linux/man-pages/man8/mount.fuse3.8.html
    - https://code.google.com/archive/p/macfuse/wikis/OPTIONS.wiki
""")
parser.add_argument("-ll", "--log-level", default="ERROR", help=f"指定日志级别，可以是数字或名称，不传此参数则不输出日志，默认值: 'ERROR'")
parser.add_argument("-l", "--license", action="store_true", help="输出授权信息")
parser.add_argument("-v", "--version", action="store_true", help="输出版本号")
parser.set_defaults(func=main)


if __name__ == "__main__":
    main()
