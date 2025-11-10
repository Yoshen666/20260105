import os
import subprocess


def test_sqluldr_connection_formats():
    """測試不同的連接字符串格式"""

    print("=== 測試 sqluldr264 連接格式 ===")

    # 不同的連接格式
    connection_formats = [
        # 格式1: 標準格式
        "uosche/uosche@10.52.192.100:1521/uosche",

        # 格式2: 使用括號
        "uosche/uosche@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.52.192.100)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=uosche)))",

        # 格式3: 簡化IP格式
        "uosche/uosche@10.52.192.100/uosche",

        # 格式4: 使用SID而非SERVICE_NAME
        "uosche/uosche@10.52.192.100:1521:uosche",
    ]

    test_query = "SELECT SYSDATE FROM DUAL"
    test_file = "C:/temp/conn_test.csv"

    for i, conn_str in enumerate(connection_formats, 1):
        print(f"\n{i}. 測試連接格式: {conn_str}")

        # 刪除之前的測試文件
        if os.path.exists(test_file):
            os.remove(test_file)

        cmd = f'sqluldr264 user={conn_str} query="{test_query}" field="|" head=yes file={test_file}'

        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)

            print(f"   返回碼: {result.returncode}")
            if result.stdout:
                print(f"   輸出: {result.stdout.strip()}")
            if result.stderr:
                print(f"   錯誤: {result.stderr.strip()}")

            if os.path.exists(test_file):
                file_size = os.path.getsize(test_file)
                print(f"   ✓ 成功! 文件大小: {file_size} bytes")

                with open(test_file, 'r') as f:
                    content = f.read().strip()
                    print(f"   內容: {content}")
                return conn_str  # 返回成功的連接格式
            else:
                print(f"   ✗ 失敗 - 文件未生成")

        except subprocess.TimeoutExpired:
            print(f"   ✗ 超時")
        except Exception as e:
            print(f"   ✗ 異常: {e}")

    return None


def test_sqluldr_with_verbose():
    """使用詳細模式測試 sqluldr264"""
    print("\n=== 詳細模式測試 ===")

    # 嘗試獲取詳細錯誤信息
    cmd = 'sqluldr264 help'
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        print(f"help 命令返回碼: {result.returncode}")
        print(f"help 輸出: {result.stdout}")
        if result.stderr:
            print(f"help 錯誤: {result.stderr}")
    except Exception as e:
        print(f"help 命令失敗: {e}")


def create_pandas_replacement():
    """創建 pandas 替代方案"""

    replacement_code = '''
def exec_view_to_dat_pandas(file_name, query_sql, split_char="[@@]", is_append_header=True):
    """
    使用 pandas 替代 sqluldr264 的函數
    """
    import pandas as pd
    import os
    from xinxiang.util import my_oracle

    print(f"使用pandas導出CSV: {file_name}")
    print(f"查詢: {query_sql[:100]}...")

    conn = None
    try:
        # 獲取連接
        conn = my_oracle.oracle_get_connection()

        # 讀取數據
        df = pd.read_sql(query_sql, conn)
        print(f"讀取到 {len(df)} 行數據")

        if len(df) == 0:
            print("警告: 查詢沒有返回數據")
            return False

        # 確保目標目錄存在
        target_dir = os.path.dirname(file_name)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            print(f"創建目錄: {target_dir}")

        # 導出CSV
        df.to_csv(file_name, sep=split_char, index=False, encoding='utf-8')

        # 驗證文件
        if os.path.exists(file_name):
            file_size = os.path.getsize(file_name)
            print(f"✓ 文件生成成功: {file_name}, 大小: {file_size} bytes")
            return True
        else:
            print("✗ 文件生成失敗")
            return False

    except Exception as e:
        print(f"✗ pandas 導出失敗: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()

# 使用方法：
# 在您的 my_cmder.py 中：
# 1. 先嘗試原來的 sqluldr264
# 2. 如果失敗，調用這個函數

def exec_view_to_dat_with_fallback(file_name, query_sql, split_char="[@@]", is_append_header=True):
    """
    帶有失敗回退的導出函數
    """
    import subprocess
    import os

    # 嘗試 sqluldr264
    try:
        conn_str = "uosche/uosche@10.52.192.100:1521/uosche"
        cmd = f'sqluldr264 user={conn_str} query="{query_sql}" field="{split_char}" head=yes file={file_name} charset=AL32UTF8'

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)

        if result.returncode == 0 and os.path.exists(file_name) and os.path.getsize(file_name) > 0:
            print("✓ sqluldr264 導出成功")
            return True
        else:
            print(f"sqluldr264 失敗 (返回碼: {result.returncode})")
            if result.stderr:
                print(f"錯誤: {result.stderr}")
    except Exception as e:
        print(f"sqluldr264 異常: {e}")

    # 回退到 pandas
    print("回退到 pandas 方案...")
    return exec_view_to_dat_pandas(file_name, query_sql, split_char, is_append_header)
'''

    print("\n=== pandas 替代方案代碼 ===")
    print(replacement_code)

    # 保存到文件
    with open("C:/temp/pandas_replacement.py", "w", encoding="utf-8") as f:
        f.write(replacement_code)
    print("\n替代代碼已保存到: C:/temp/pandas_replacement.py")


if __name__ == "__main__":
    # 測試不同連接格式
    working_format = test_sqluldr_connection_formats()

    if working_format:
        print(f"\n✓ 找到可用的連接格式: {working_format}")
    else:
        print(f"\n✗ 所有連接格式都失敗")

        # 測試詳細模式
        test_sqluldr_with_verbose()

        # 提供替代方案
        create_pandas_replacement()

        print("\n建議:")
        print("1. sqluldr264 工具可能與 Oracle 19c 不兼容")
        print("2. 使用 pandas 替代方案")
        print("3. 或者檢查 Oracle 客戶端配置")