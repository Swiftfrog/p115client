#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["main"]
__doc__ = """\
    🛸 115 数据库 WebDAV 服务 ✈️
"""

from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from pathlib import Path

if __name__ == "__main__":  
    from sys import path

    path[0] = str(Path(__file__).parents[1])
    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
else:
    from .init import subparsers

    parser = subparsers.add_parser("dav", description=__doc__, formatter_class=RawTextHelpFormatter)


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

    import uvicorn

    from orjson import loads as json_loads
    from tomllib import load as toml_load
    from p115servedb.component.dav import make_application
    from path_predicate import make_predicate
    from yaml import load as yaml_load, Loader

    if args.fast_strm:
        predicate = make_predicate("""(
        path.is_dir() or
        path.media_type.startswith("image/") or
        path.suffix.lower() in (".nfo", ".ass", ".ssa", ".srt", ".idx", ".sub", ".txt", ".vtt", ".smi")
)""", type="expr")
    elif predicate := args.predicate or None:
        predicate = make_predicate(predicate, {"re": __import__("re")}, type=args.predicate_type)
    if args.fast_strm:
        strm_predicate = make_predicate("""(
        path.media_type.startswith(("video/", "audio/")) and
        path.suffix.lower() != ".ass" or
        path.suffix.lower() in (".divx", ".iso", ".m2ts", ".swf", ".xvid")
)""", type="expr")
    elif strm_predicate := args.strm_predicate or None:
        strm_predicate = make_predicate(strm_predicate, {"re": __import__("re")}, type=args.strm_predicate_type)

    if wsgidav_config_path := args.wsgidav_config_path:
        file = open(wsgidav_config_path, "rb")
        match suffix := Path(wsgidav_config_path).suffix.lower():
            case ".yml" | ".yaml":
                wsgidav_config = yaml_load(file, Loader=Loader)
            case ".toml":
                wsgidav_config = toml_load(file)
            case _:
                wsgidav_config = json_loads(file.read())
    else:
        wsgidav_config = {}

    uvicorn_run_config_path = args.uvicorn_run_config_path
    if uvicorn_run_config_path:
        file = open(uvicorn_run_config_path, "rb")
        match suffix := Path(uvicorn_run_config_path).suffix.lower():
            case ".yml" | ".yaml":
                run_config = yaml_load(file, Loader=Loader)
            case ".toml":
                run_config = toml_load(file)
            case _:
                run_config = json_loads(file.read())
    else:
        run_config = {}

    if args.host:
        run_config["host"] = args.host
    else:
        run_config.setdefault("host", "0.0.0.0")
    if args.port:
        run_config["port"] = args.port
    elif not run_config.get("port"):
        from socket import create_connection

        def get_available_ip(start: int = 1024, stop: int = 65536) -> int:
            for port in range(start, stop):
                try:
                    with create_connection(("127.0.0.1", port), timeout=1):
                        pass
                except OSError:
                    return port
            raise RuntimeError("no available ports")

        run_config["port"] = get_available_ip()

    run_config.setdefault("proxy_headers", True)
    run_config.setdefault("server_header", False)
    run_config.setdefault("forwarded_allow_ips", "*")
    run_config.setdefault("timeout_graceful_shutdown", 1)
    run_config.setdefault("access_log", False)

    app = make_application(
        dbfile=args.dbfile, 
        cookies_path=args.cookies_path, 
        strm_origin=args.strm_origin, 
        predicate=predicate, 
        strm_predicate=strm_predicate, 
        load_libass=args.load_libass, 
        debug=args.debug, 
        wsgidav_config=wsgidav_config, 
    )
    uvicorn.run(app, **run_config)


parser.add_argument("-f", "--dbfile", required=True, help="数据库文件路径")
parser.add_argument("-cp", "--cookies-path", default="", help="cookies 文件保存路径，默认为当前工作目录下的 115-cookies.txt（如果 115-cookies.txt 不存在，则使用 -o/--strm-origin 所指定的服务进行下载）")
parser.add_argument("-o", "--strm-origin", default="", help="strm 所用的 302 服务地址，如果不传，则自动确定")
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
parser.add_argument("-H", "--host", default="0.0.0.0", help="ip 或 hostname，默认值：'0.0.0.0'")
parser.add_argument("-P", "--port", default=8000, type=int, help="端口号，默认值：8000")
parser.add_argument("-d", "--debug", action="store_true", help="启用 debug 模式（会输出更详细的信息）")
parser.add_argument("-ass", "--load-libass", action="store_true", help="加载 libass.js，实现 ass/ssa 字幕特效")
parser.add_argument("-uc", "--uvicorn-run-config-path", help="uvicorn 启动时的配置文件路径，会作为关键字参数传给 `uvicorn.run`，支持 JSON、YAML 或 TOML 格式，会根据扩展名确定，不能确定时视为 JSON")
parser.add_argument("-wc", "--wsgidav-config-path", help="""WsgiDAV 启动时的配置文件路径，支持 JSON、YAML 或 TOML 格式，会根据扩展名确定，不能确定时视为 JSON
如需样板文件，请阅读：

    https://wsgidav.readthedocs.io/en/latest/user_guide_configure.html#sample-wsgidav-yaml

""")
parser.add_argument("-l", "--license", action="store_true", help="输出授权信息")
parser.add_argument("-v", "--version", action="store_true", help="输出版本号")
parser.set_defaults(func=main)


if __name__ == "__main__":
    main()
