# list_files.py

import os


def main():
    # 從目前這個資料夾開始
    root_dir = "."

    # 開啟 dir_files.txt，準備把結果寫進去
    with open("dir_files.txt", "w", encoding="utf-8") as f:
        for current_path, dir_names, file_names in os.walk(root_dir):
            # 如果發現有 .git 這個子資料夾，就把它從 dir_names 裡移除
            # 這樣 os.walk 就不會進去 .git，也不會把 .git 裡的東西輸出
            if ".git" in dir_names:
                dir_names.remove(".git")

            # 計算這個資料夾層級有幾層（用來做縮排）
            level = current_path.count(os.sep)
            indent = "    " * level  # 每一層就多四個空格

            # 取出這個 current_path 的資料夾名字，如果是根目錄就顯示 "."
            folder_name = os.path.basename(current_path) or current_path
            f.write(f"{indent}{folder_name}/\n")

            # 檢查這個資料夾裡的所有檔案
            for filename in file_names:
                # 只輸出副檔名為 .py、.db，或是檔名剛好叫 stock_id.csv 的檔案
                if filename.endswith(".py") or filename.endswith(".db") or filename == "stock_id.csv":
                    # 再多一層縮排，把檔案名稱寫下來
                    f.write(f"{indent}    {filename}\n")


if __name__ == "__main__":
    main()
