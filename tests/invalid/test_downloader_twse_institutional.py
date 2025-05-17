import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipeline.downloader import InstitutionalTWSEDownloader
from src.utils.helpers import load_config

def test_institutional_twse_download():
    config = load_config()

    # ✅ 使用 save_root（不是 save_dir）
    downloader = InstitutionalTWSEDownloader(
        url_template=config["twse_institutional"]["url_template"],
        save_root=config["paths"]["raw_data"]
    )

    test_date = "20250509"  # 確保是有交易資料的一天
    downloader.download(test_date)

if __name__ == "__main__":
    test_institutional_twse_download()
