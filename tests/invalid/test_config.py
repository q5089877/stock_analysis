import sys
import os

# 把 src 資料夾加入 sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "src"))

from utils.helpers import load_config

def test_config():
    config = load_config()
    print("✅ 成功讀取 config.yaml")
    print("資料庫連線字串：", config["database"]["url"])
    print("TWSE URL：", config["twse"]["url_template"])
    print("TPEx URL：", config["tpex"]["url_template"])
    print("資料儲存位置：", config["paths"]["raw_data"])

if __name__ == "__main__":
    test_config()
