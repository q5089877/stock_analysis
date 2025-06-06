import os
import pytest
import pandas as pd
from src.pipeline.downloader import TPExInstitutionalDownloader


@pytest.fixture
def downloader():
    return TPExInstitutionalDownloader(save_dir="data/raw/tpex_institutional")


def test_download_tpex_institutional(downloader):
    test_date = "114/05/08"

    df = downloader.download(test_date)

    # 驗證 df 為合法資料
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, "DataFrame 應該不為空"
    assert '證券代號' in df.columns
    assert '證券名稱' in df.columns
    assert '外資_買進' in df.columns

    # 驗證檔案是否正確儲存
    file_name = f"tpex_institutional_{test_date.replace('/', '')}.csv"
    file_path = os.path.join("data/raw/tpex_institutional", file_name)
    assert os.path.exists(file_path)
