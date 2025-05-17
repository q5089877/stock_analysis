import sys
import os

# 加入 src
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.downloader import TWSEDownloader
from utils.helpers import load_config

def test_twse_download():
    config = load_config()
    twse = TWSEDownloader(
        url_template=config["twse"]["url_template"],
        save_dir=os.path.join(config["paths"]["raw_data"], "twse")
    )
    twse.download("20250509")  # 你可改為其他測試日

if __name__ == "__main__":
    test_twse_download()
