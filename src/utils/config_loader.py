# src/utils/config_loader.py

import yaml
import os


def load_config(config_path="config/config.yaml"):
    """
    讀取專案根目錄底下的 config/config.yaml，回傳 dict。
    """

    # 取得目前檔案（config_loader.py）所在資料夾的路徑
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 往上兩層到專案根目錄
    project_root = os.path.normpath(
        os.path.join(current_dir, os.pardir, os.pardir))
    # 將傳入的相對路徑（config/config.yaml）轉成從專案根目錄開始的完整路徑
    full_config_path = os.path.join(project_root, config_path)

    if not os.path.exists(full_config_path):
        raise FileNotFoundError(f"找不到設定檔: {full_config_path}")

    with open(full_config_path, "r", encoding="utf-8") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise RuntimeError(f"讀取 YAML 設定檔時發生錯誤：{e}")

    return config
