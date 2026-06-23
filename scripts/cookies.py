import os
from pathlib import Path
from p115client import P115Client

# 从环境变量读取配置，提供默认值兜底
# CONFIG_DIR: 你希望保存 Cookie 文件的目标目录
# APP_NAME: 伪装的客户端身份
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/cookies")
APP_NAME = os.environ.get("APP_NAME", "qandriod")

# 确保目标目录存在 (避免因挂载错误导致路径不存在而报错)
config_path = Path(CONFIG_DIR).expanduser()
config_path.mkdir(parents=True, exist_ok=True)

# 动态拼接完整的 Cookie 文件路径
cookies_file = config_path / f"115-cookies-{APP_NAME}.txt"

print("=" * 50)
print(f"🚀 115 凭证生成工具启动")
print(f"📁 输出目录: {config_path}")
print(f"🪪 伪装身份: {APP_NAME}")
print(f"📄 凭证路径: {cookies_file}")
print("=" * 50)

try:
    client = P115Client(
        cookies=cookies_file,
        check_for_relogin=True,
        ensure_cookies=True,
        app=APP_NAME,
        console_qrcode=True
    )
    print("\n" + "=" * 50)
    print(f"✅ 客户端初始化成功！")
    print(f"💾 Cookie 已安全保存至: {cookies_file}")
    print("=" * 50)
    
except Exception as e:
    print(f"\n❌ 客户端初始化失败: {e}")
