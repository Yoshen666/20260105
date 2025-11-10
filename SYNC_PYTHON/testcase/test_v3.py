#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
精確測試是什麼條件讓 COUNT 查詢成功
"""

import sys
import os


def test_precise_conditions(conn):
    """精確測試各種條件的影響"""
    print("=== 精確測試條件影響 ===")

    cursor = conn.cursor()

    # 測試各種不同類型的條件
    condition_tests = [
        # 基本條件
        ("無條件", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP"),
        ("1=1", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE 1=1"),
        ("'A'='A'", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE 'A'='A'"),

        # 欄位條件
        ("LOT_ID IS NOT NULL", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE LOT_ID IS NOT NULL"),
        ("LOT_ID LIKE '%'", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE LOT_ID LIKE '%'"),
        ("LENGTH(LOT_ID) > 0", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE LENGTH(LOT_ID) > 0"),

        # 日期條件 - 關鍵測試
        ("CLAIM_TIME IS NOT NULL", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CLAIM_TIME IS NOT NULL"),
        ("CLAIM_TIME > TO_DATE('1900-01-01', 'YYYY-MM-DD')",
         "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CLAIM_TIME > TO_DATE('1900-01-01', 'YYYY-MM-DD')"),
        ("CLAIM_TIME < TO_DATE('2030-01-01', 'YYYY-MM-DD')",
         "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CLAIM_TIME < TO_DATE('2030-01-01', 'YYYY-MM-DD')"),
        ("CLAIM_TIME = CLAIM_TIME", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CLAIM_TIME = CLAIM_TIME"),

        # TO_DATE 函數測試
        (
        "TO_DATE 函數存在", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE TO_DATE('2025-01-01', 'YYYY-MM-DD') IS NOT NULL"),
        ("簡單日期比較", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SYSDATE > TO_DATE('1900-01-01', 'YYYY-MM-DD')"),

        # ROWNUM 測試
        ("ROWNUM > 0", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE ROWNUM > 0"),
        ("ROWNUM <= 999999", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE ROWNUM <= 999999"),

        # 函數相關
        ("使用 TRUNC", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE TRUNC(CLAIM_TIME) IS NOT NULL"),
        ("使用 TO_CHAR", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE TO_CHAR(CLAIM_TIME, 'YYYY') IS NOT NULL"),
    ]

    successful_conditions = []
    failed_conditions = []

    for desc, sql in condition_tests:
        print(f"\n測試: {desc}")
        print(f"SQL: {sql}")

        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✓ 成功: {result}")
            successful_conditions.append((desc, result))

        except Exception as e:
            if "DPY-3006" in str(e):
                print("✗ DPY-3006 錯誤")
                failed_conditions.append(desc)
            else:
                print(f"✗ 其他錯誤: {e}")

    print(f"\n" + "=" * 50)
    print("📊 **測試結果統計**")
    print("=" * 50)

    print(f"\n✅ 成功的條件 ({len(successful_conditions)}):")
    for desc, count in successful_conditions:
        print(f"   - {desc}: {count}")

    print(f"\n❌ 失敗的條件 ({len(failed_conditions)}):")
    for desc in failed_conditions:
        print(f"   - {desc}")

    cursor.close()
    return successful_conditions, failed_conditions


def test_date_function_impact(conn):
    """專門測試日期函數的影響"""
    print(f"\n=== 專門測試日期函數影響 ===")

    cursor = conn.cursor()

    date_function_tests = [
        # 不同的日期函數
        ("SYSDATE", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SYSDATE IS NOT NULL"),
        ("SYSTIMESTAMP", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SYSTIMESTAMP IS NOT NULL"),
        ("CURRENT_DATE", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CURRENT_DATE IS NOT NULL"),

        # TO_DATE 的不同用法
        ("TO_DATE 簡單", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE TO_DATE('2025-01-01', 'YYYY-MM-DD') IS NOT NULL"),
        ("TO_DATE 複雜",
         "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CLAIM_TIME < TO_DATE('2025/09/05 18:00:00', 'YYYY/MM/DD HH24:MI:SS')"),

        # 日期運算
        ("日期加法", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SYSDATE + 1 IS NOT NULL"),
        ("日期減法", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SYSDATE - 1 IS NOT NULL"),

        # 日期格式化
        ("TO_CHAR 日期", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE TO_CHAR(SYSDATE, 'YYYY') IS NOT NULL"),
        ("EXTRACT", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE EXTRACT(YEAR FROM SYSDATE) IS NOT NULL"),
    ]

    for desc, sql in date_function_tests:
        print(f"\n{desc}:")
        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✓ 成功: {result}")
        except Exception as e:
            if "DPY-3006" in str(e):
                print("✗ DPY-3006 錯誤")
            else:
                print(f"✗ 其他錯誤: {e}")

    cursor.close()


def test_query_complexity_threshold(conn):
    """測試查詢複雜度的閾值"""
    print(f"\n=== 測試查詢複雜度閾值 ===")

    cursor = conn.cursor()

    complexity_tests = [
        # 從簡單到複雜
        ("純COUNT", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP"),
        ("加法運算", "SELECT COUNT(*) + 0 FROM V_CSLOT_FORCESAMP"),
        ("字串函數", "SELECT LENGTH('A') + COUNT(*) - 1 FROM V_CSLOT_FORCESAMP"),
        ("日期函數", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE EXTRACT(YEAR FROM SYSDATE) > 2000"),
        ("子查詢", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE EXISTS (SELECT 1 FROM DUAL)"),
        ("CASE 語句", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE CASE WHEN 1=1 THEN 1 ELSE 0 END = 1"),
    ]

    print("查找從簡單到複雜，哪個點開始正常工作:")

    for desc, sql in complexity_tests:
        print(f"\n{desc}:")
        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✓ 成功: {result}")
            print("   >>> 這個複雜度已經可以避免 DPY-3006!")
        except Exception as e:
            if "DPY-3006" in str(e):
                print("✗ 仍然 DPY-3006")
            else:
                print(f"✗ 其他錯誤: {e}")

    cursor.close()


def find_minimal_working_condition(conn):
    """找出最小的有效條件"""
    print(f"\n=== 尋找最小有效條件 ===")

    cursor = conn.cursor()

    # 基於前面的發現，測試各種最小條件
    minimal_tests = [
        # 最簡單的有效條件
        ("任何函數調用", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE ABS(1) = 1"),
        ("任何運算", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE 1 + 0 = 1"),
        ("字串操作", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SUBSTR('A', 1, 1) = 'A'"),
        ("數學函數", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE SQRT(4) = 2"),
        ("DUAL 表", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE (SELECT 1 FROM DUAL) = 1"),

        # 涉及目標表的條件
        ("欄位函數", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE UPPER(LOT_ID) IS NOT NULL"),
        ("欄位運算", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE LENGTH(LOT_ID) >= 0"),
        ("欄位比較", "SELECT COUNT(*) FROM V_CSLOT_FORCESAMP WHERE LOT_ID = LOT_ID"),
    ]

    working_solutions = []

    for desc, sql in minimal_tests:
        print(f"\n{desc}:")
        print(f"SQL: {sql}")

        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✓ 成功: {result}")
            working_solutions.append((desc, sql))
        except Exception as e:
            if "DPY-3006" in str(e):
                print("✗ 仍然 DPY-3006")
            else:
                print(f"✗ 其他錯誤: {e}")

    if working_solutions:
        print(f"\n🎯 **找到最小有效條件**:")
        for desc, sql in working_solutions:
            print(f"   - {desc}")
            print(f"     {sql}")

    cursor.close()
    return working_solutions


def main():
    """主函數"""
    print("🔬 **精確定位條件差異**")
    print("=" * 60)

    # 添加項目路徑
    project_path = 'D:/XinXiang/SYNC_PYTHON'
    if project_path not in sys.path:
        sys.path.append(project_path)

    try:
        from xinxiang.util.my_oracle import oracle_get_connection

        print("連接資料庫...")
        conn = oracle_get_connection()
        print("✓ 資料庫連接成功")

        # 執行精確測試
        successful, failed = test_precise_conditions(conn)
        test_date_function_impact(conn)
        test_query_complexity_threshold(conn)
        working_solutions = find_minimal_working_condition(conn)

        # 最終分析
        print(f"\n" + "=" * 60)
        print("🎯 **最終分析結論**")
        print("=" * 60)

        if working_solutions:
            print("🎉 找到了最小的解決方案!")
            print("✅ 最簡單的修復方法:")

            simplest = working_solutions[0]
            print(f"   條件: {simplest[0]}")
            print(f"   範例: {simplest[1]}")

            print(f"\n💡 **實用的修復函數**:")
            print(f"```python")
            print(f"def get_row_count_in_oracle_fixed(conn, tableName):")
            print(f"    cursor = conn.cursor()")
            print(f"    try:")
            print(f"        # 使用最小有效條件避免 DPY-3006")
            print(f"        sql = f\"SELECT COUNT(*) FROM {{tableName}} WHERE ABS(1) = 1\"")
            print(f"        cursor.execute(sql)")
            print(f"        count = cursor.fetchone()[0]")
            print(f"        cursor.close()")
            print(f"        return count")
            print(f"    except Exception as e:")
            print(f"        cursor.close()")
            print(f"        raise e")
            print(f"```")
        else:
            print("⚠️  沒有找到簡單的解決方案，建議使用 TO_NUMBER() 方法")

        conn.close()

    except Exception as e:
        print(f"✗ 測試失敗: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n🎊 **精確測試完成!**")


if __name__ == "__main__":
    main()