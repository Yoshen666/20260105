import os

import duckdb

from xinxiang import config
from xinxiang.util import my_postgres
import pandas as pd

def diff(table_name,prod_time, test_time):
    prod_pg = my_postgres.postgres_get_connection()
    prod_cursor = prod_pg.cursor()
    test_pg = my_postgres.postgres_get_test_db_connection()
    test_cursor = test_pg.cursor()
    #-------------------------得到最後的時間戳
    # query_max_ver_timekey_sql = """
    #     select ver_timekey from yth.{table_name} order by ver_timekey desc limit 1
    # """.format(table_name=table_name)
    # prod_cursor.execute(query_max_ver_timekey_sql)
    # prod_ver_timekey = prod_cursor.fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')
    # print(table_name, "prod_ver_timekey", prod_ver_timekey)
    #
    # test_cursor.execute(query_max_ver_timekey_sql)
    # test_ver_timekey = test_cursor.fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')
    # print(table_name, "test_ver_timekey", test_ver_timekey)
    prod_ver_timekey = prod_time
    test_ver_timekey = test_time
    # ------------------------
    columns_search_sql = "select * from yth.{table_name} limit 1".format(table_name=table_name)
    prod_temp = pd.read_sql(columns_search_sql, prod_pg)
    prod_temp.columns = prod_temp.columns.str.lower()

    columns = " DESC, ".join([col for col in prod_temp.columns])
    _order_condition = " ORDER BY " + columns
    print(_order_condition)


    prod_query_content = """
        select * from yth.{table_name} where to_char(ver_timekey, 'YYYY-MM-DD HH24:MI:SS') = '{ver_timekey}' 
    """.format(table_name=table_name, ver_timekey= prod_ver_timekey)
    prod_result = pd.read_sql(prod_query_content + _order_condition, prod_pg)
    print(prod_result)


    test_query_content = """
            select * from yth.{table_name} where to_char(ver_timekey, 'YYYY-MM-DD HH24:MI:SS') = '{ver_timekey}' 
        """.format(table_name=table_name, ver_timekey=test_ver_timekey)
    test_result = pd.read_sql(test_query_content + _order_condition, test_pg)
    print(test_result)


    # -----------------------------
    diff_result_file = os.path.join(config.g_debug_file_path, table_name + "_PG_PROD_AND_TEST_DIFF.db")
    if not os.path.exists(config.g_debug_file_path):
        os.makedirs(config.g_debug_file_path)
    if os.path.exists(diff_result_file):
        os.remove(diff_result_file)



    diff_cols = []
    for _col in prod_temp.columns:
        if _col.lower() != "parentid" and _col.lower() != "ver_timekey":
            diff_cols.append(_col)
    prod_result[diff_cols] = prod_result[diff_cols].astype(str).apply(lambda x: x.str.strip())
    test_result[diff_cols] = test_result[diff_cols].astype(str).apply(lambda x: x.str.strip())

    merge_result = prod_result[diff_cols].eq(test_result[diff_cols])
    mask = merge_result.any(axis=1)
    merge_result = merge_result.applymap(lambda x: 'N' if not x else 'Y')
    out_df = pd.concat([prod_result.add_prefix('P_'), test_result[diff_cols].add_prefix('T_')], axis=1)
    # out_df = pd.concat([out_df, merge_result], axis=1)
    out_df = out_df[mask]
    out_df = pd.concat([out_df, merge_result], axis=1, join='inner')

    try:
        duck_db = duckdb.connect(diff_result_file)
        duck_db.sql("create table {} as select * from out_df".format(table_name))

    except AttributeError as eee:
        print(eee)
    finally:
        duck_db.close()

    prod_cursor.close()
    test_cursor.close()
    prod_pg.close()
    test_pg.close()


if __name__ == '__main__':
    # table_name: 表名
    # prod_time： 生產環境中的 ver_timekey 格式：2023-11-08 14:00:30
    # test_time： 測試環境中的 ver_timekey 格式：2023-11-08 14:00:30

    diff(table_name="etl_tool", prod_time="", test_time="")
