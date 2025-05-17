import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipeline.downloader import TPExDownloader
from src.utils.helpers import load_config

def test_tpex_download():
    config = load_config()
    downloader = TPExDownloader(
        url_template=config["tpex"]["url_template"],
        save_dir=config["paths"]["raw_data"]
    )

    # 測試日期請確認是有交易日
    test_date = "20250509"
    downloader.download(test_date)

if __name__ == "__main__":
    test_tpex_download()
