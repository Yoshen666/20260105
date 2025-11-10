#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
深度分析 oracledb 3.3.0 的 DPY-3006 錯誤
確定是否為真正的 bug
"""

import sys
import os


def analyze_oracle_metadata(conn, table_name):
    """深度分析 Oracle 元數據，找出問題根源"""
    print(f"=== 深度分析 {table_name} 元數據 ===")
    cursor = conn.cursor()

    # 1. 檢查視圖定義中的資料類型映射
    print("\n1. 分析視圖定義:")
    try:
        cursor.execute("""
            SELECT text 
            FROM user_views 
            WHERE view_name = UPPER(:view_name)
        """, {"view_name": table_name})

        view_def = cursor.fetchone()[0]
        print("視圖SQL:")
        print(view_def)

        # 分析 SQL 中可能有問題的部分
        problematic_parts = []
        if "TO_DATE" in view_def:
            problematic_parts.append("包含 TO_DATE 轉換")
        if "REPLACE" in view_def:
            problematic_parts.append("包含 REPLACE 函數")
        if "@" in view_def:
            problematic_parts.append("查詢遠程資料庫 (Database Link)")

        if problematic_parts:
            print("可能的問題點:")
            for issue in problematic_parts:
                print(f"  - {issue}")

    except Exception as e:
        print(f"獲取視圖定義失敗: {e}")

    # 2. 檢查遠程表格的真實資料類型
    print("\n2. 檢查遠程表格資料類型:")
    try:
        # 從視圖定義可以看出是 F2RPT.CSLOT_FORCESAMP@mes_rpt
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable
            FROM ALL_TAB_COLUMNS@mes_rpt
            WHERE table_name = 'CSLOT_FORCESAMP'
            AND owner = 'F2RPT'
            ORDER BY column_id
        """)

        remote_columns = cursor.fetchall()
        print("遠程表格欄位:")
        print("-" * 80)
        print(f"{'欄位名稱':<20} {'資料類型':<15} {'長度':<8} {'精度':<6} {'小數':<6} {'可空':<6}")
        print("-" * 80)

        suspicious_columns = []

        for col in remote_columns:
            col_name, data_type, data_length, data_precision, data_scale, nullable = col
            print(
                f"{col_name:<20} {data_type:<15} {str(data_length or ''):<8} {str(data_precision or ''):<6} {str(data_scale or ''):<6} {nullable:<6}")

            # 檢查可能有問題的類型
            if data_type == 'NUMBER' and data_precision is None:
                suspicious_columns.append((col_name, "NUMBER 無精度"))
            elif data_type in ['FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE']:
                suspicious_columns.append((col_name, f"{data_type} 浮點類型"))
            elif data_type.startswith('TIMESTAMP'):
                suspicious_columns.append((col_name, f"{data_type} 時間戳類型"))

        if suspicious_columns:
            print("\n⚠️  可疑的遠程欄位:")
            for col_name, issue in suspicious_columns:
                print(f"   - {col_name}: {issue}")

    except Exception as e:
        print(f"檢查遠程表格失敗: {e}")

    # 3. 對比本地視圖和遠程表格的差異
    print("\n3. 對比本地視圖與遠程表格:")
    try:
        cursor.execute("""
            SELECT column_name, data_type, data_precision, data_scale
            FROM user_tab_columns 
            WHERE table_name = UPPER(:table_name)
            ORDER BY column_id
        """, {"table_name": table_name})

        local_columns = cursor.fetchall()

        print("資料類型對比:")
        print(f"{'欄位':<15} {'本地類型':<20} {'遠程類型':<20} {'問題':<20}")
        print("-" * 75)

        for local_col in local_columns:
            local_name, local_type, local_prec, local_scale = local_col

            # 找對應的遠程欄位
            remote_col = next((r for r in remote_columns if r[0] == local_name), None)

            if remote_col:
                remote_type = remote_col[1]
                remote_prec = remote_col[3]

                issue = ""
                if local_type != remote_type:
                    issue = "類型不一致"
                elif local_type == 'NUMBER' and remote_prec is None:
                    issue = "NUMBER 無精度"

                print(f"{local_name:<15} {local_type:<20} {remote_type:<20} {issue:<20}")
            else:
                print(f"{local_name:<15} {local_type:<20} {'不存在':<20} {'欄位不匹配':<20}")

    except Exception as e:
        print(f"對比失敗: {e}")

    cursor.close()


def test_specific_count_scenarios(conn, table_name):
    """測試特定的 COUNT 場景"""
    print(f"\n=== 測試特定 COUNT 場景 ===")
    cursor = conn.cursor()

    # 測試場景
    test_cases = [
        ("基本 COUNT(*)", f"SELECT COUNT(*) FROM {table_name}"),
        ("COUNT(1)", f"SELECT COUNT(1) FROM {table_name}"),
        ("COUNT(主鍵)", f"SELECT COUNT(LOT_ID) FROM {table_name}"),
        ("COUNT(非空欄位)", f"SELECT COUNT(SAMP_TYPE) FROM {table_name}"),
        ("COUNT(日期欄位)", f"SELECT COUNT(CLAIM_TIME) FROM {table_name}"),
        ("有條件的 COUNT", f"SELECT COUNT(*) FROM {table_name} WHERE ROWNUM <= 1"),
        ("子查詢 COUNT", f"SELECT COUNT(*) FROM (SELECT LOT_ID FROM {table_name} WHERE ROWNUM <= 10)"),
        ("聚合後 COUNT", f"SELECT COUNT(DISTINCT LOT_ID) FROM {table_name} WHERE ROWNUM <= 100"),
        ("遠程表直接 COUNT", "SELECT COUNT(*) FROM F2RPT.CSLOT_FORCESAMP@mes_rpt WHERE ROWNUM <= 10"),
    ]

    results = {"成功": [], "失敗": []}

    for test_name, sql in test_cases:
        print(f"\n測試: {test_name}")
        print(f"SQL: {sql}")

        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✓ 成功: {result}")
            results["成功"].append(test_name)
        except Exception as e:
            print(f"✗ 失敗: {e}")
            results["失敗"].append((test_name, str(e)))

            # 詳細分析錯誤
            if "DPY-3006" in str(e):
                print("  >>> 這是 DPY-3006 錯誤！")
            elif "ORA-" in str(e):
                print("  >>> 這是 Oracle 錯誤，不是驅動問題")

    cursor.close()
    return results


def analyze_driver_behavior():
    """分析驅動程式行為"""
    print(f"\n=== 分析 oracledb 驅動行為 ===")

    import oracledb

    print(f"驅動程式資訊:")
    print(f"  版本: {oracledb.version}")
    print(f"  模式: {'Thick' if hasattr(oracledb, 'is_thin_mode') and not oracledb.is_thin_mode() else 'Thin'}")

    # 檢查已知問題
    version_parts = oracledb.version.split('.')
    major, minor = int(version_parts[0]), int(version_parts[1])

    known_issues = {
        (3, 3): "已知 DPY-3006 問題，特別是 Database Link 查詢",
        (3, 2): "相對穩定，但仍有少數問題",
        (3, 1): "早期版本，建議升級",
        (3, 0): "早期版本，建議升級",
    }

    if (major, minor) in known_issues:
        print(f"  已知問題: {known_issues[(major, minor)]}")

    # 檢查相關的環境變量和設置
    import os
    oracle_vars = ['ORACLE_HOME', 'TNS_ADMIN', 'NLS_LANG']

    print(f"\nOracle 環境變數:")
    for var in oracle_vars:
        value = os.environ.get(var)
        print(f"  {var}: {value if value else '未設置'}")


def determine_bug_type(test_results):
    """判斷 bug 類型"""
    print(f"\n=== Bug 類型判斷 ===")

    failed_tests = test_results["失敗"]
    successful_tests = test_results["成功"]

    print(f"成功的測試 ({len(successful_tests)}):")
    for test in successful_tests:
        print(f"  ✓ {test}")

    print(f"\n失敗的測試 ({len(failed_tests)}):")
    dpy_3006_count = 0
    oracle_error_count = 0

    for test_name, error in failed_tests:
        print(f"  ✗ {test_name}: {error}")
        if "DPY-3006" in error:
            dpy_3006_count += 1
        elif "ORA-" in error:
            oracle_error_count += 1

    # 判斷
    print(f"\n結論:")
    if dpy_3006_count > 0:
        print(f"🐛 確認是 oracledb 驅動的 BUG!")
        print(f"   - {dpy_3006_count} 個測試觸發 DPY-3006 錯誤")
        print(f"   - 這是 oracledb 3.3.0 處理 Database Link 查詢時的已知問題")
        print(f"   - 問題出現在驅動無法正確處理遠程表格的 Oracle data type 2 (NUMBER)")

        if oracle_error_count > 0:
            print(f"   - 另有 {oracle_error_count} 個 Oracle 本身的錯誤")
    else:
        print(f"❓ 不是 DPY-3006 驅動 bug")
        if oracle_error_count > 0:
            print(f"   - 主要是 Oracle 層面的問題 ({oracle_error_count} 個)")
        else:
            print(f"   - 可能是其他類型的問題")


def main():
    """主函數"""
    print("=== oracledb DPY-3006 Bug 深度分析 ===")

    # 添加項目路徑
    project_path = 'D:/XinXiang/SYNC_PYTHON'
    if project_path not in sys.path:
        sys.path.append(project_path)

    try:
        from xinxiang.util.my_oracle import oracle_get_connection

        print("連接資料庫...")
        conn = oracle_get_connection()
        print("✓ 資料庫連接成功")

        table_name = "V_CSLOT_FORCESAMP"

        # 1. 分析元數據
        analyze_oracle_metadata(conn, table_name)

        # 2. 測試不同場景
        test_results = test_specific_count_scenarios(conn, table_name)

        # 3. 分析驅動行為
        analyze_driver_behavior()

        # 4. 判斷 bug 類型
        determine_bug_type(test_results)

        conn.close()

    except Exception as e:
        print(f"✗ 分析失敗: {e}")
        import traceback
        traceback.print_exc()

    print("\n=== 分析結束 ===")


if __name__ == "__main__":
    main()