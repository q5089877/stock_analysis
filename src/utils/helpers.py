import yaml
import os

def load_config(config_path="config/config.yaml"):
    """
    讀取 YAML 設定檔，回傳 dict。
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"找不到設定檔: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config
