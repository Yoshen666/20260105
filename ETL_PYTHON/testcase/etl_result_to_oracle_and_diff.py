import csv
import logging
import os
import shutil
import sys
import traceback
import warnings

import cx_Oracle
import duckdb
import pandas
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from xinxiang import config

sys.path.insert(0, sys.path[0])
sys.path.insert(0, os.path.join(sys.path[0], '..'))
sys.path.insert(0, os.path.join(sys.path[0], '..', 'xinxiang'))

from xinxiang.util import my_duck, my_oracle, my_date

import openpyxl
from openpyxl.styles import PatternFill


def HandlingVerControl(conn, uuid, TableName):
    dbcursor = None
    try:
        timeStr = my_date.date_time_second_short_str()
        dbcursor = conn.cursor()
        sql = """delete from aps_etl_ver_control where TABLE_NAME='{}'""".format(TableName)
        dbcursor.execute(sql)

        sql = """
                INSERT INTO aps_etl_ver_control
                ( ID, MODULE_ID, TOOLG_ID, TABLE_NAME, UPDATE_TIME, UPDATE_USER )
                VALUES('{}', '' , '', '{}', '{}', '{}') 
                """.format(
            uuid,
            TableName,
            timeStr,
            "CIM",
        )

        dbcursor.execute(sql)
    except Exception as e:
        logging.exception("处理出错: %s", e)
        raise e
    finally:
        conn.commit()
        if dbcursor:
            dbcursor.close()


def get_row_count_in_oracle(conn, tableName=""):
    sql = """select count(1) from {tableName}
                where 1 = 1
                """.format(tableName=tableName)
    dbcursor = conn.cursor()
    dbcursor.execute(sql)
    result = dbcursor.fetchone()
    return result[0]


def get_test_oracle_connection():
    '''得到oracle的连接'''
    # conn = jaydebeapi.connect(config.g_driver_driver, config.g_oracle_url,
    #                               [config.g_oracle_user, config.g_oracle_password], config.g_driver_jarFile)
    conn = cx_Oracle.connect(config.local_oracle_user, config.local_oracle_password, config.local_oracle_dsn, encoding="UTF-8")
    # conn = create_engine('oracle+cx_oracle://phsche:phsche@10.52.192.99:1521/?service_name=uosche')
    return conn


def GetNextPartCodeData(conn, TableName):
    # next = None
    # curr_partcode = my_oracle.GetLastPartCodeData(conn, TableName)
    # if curr_partcode == '':
    #     next = '2000'
    # elif curr_partcode == '2000':
    #     next = '4000'
    # elif curr_partcode == '4000':
    #     next = '6000'
    # elif curr_partcode == '6000':
    #     next = '2000'
    # return next
    curr_partcode = my_oracle.GetLastPartCodeData(conn, TableName)
    return curr_partcode


def _export_duckdb_to_oracle(duck_db, oracle_conn, table_name, truncate_tables):
    folder = r"D:\workspace\temp"
    if not os.path.exists(folder):
        os.makedirs(folder)
    uuid = my_oracle.UUID()
    csv_file_name = os.path.join(folder, table_name + '_' + uuid + '.csv')
    current_time_short = my_date.date_time_second_str()

    # 删除oracle中的旧数据
    run_insert = True
    try:
        if (table_name.find("_VIEW")) != -1:
            table_name1 = table_name.split('_VIEW')[0]
            delete_sql = """truncate table {table_name}""".format(table_name=table_name1)
        else :
            delete_sql = """truncate table {table_name}""".format(table_name=table_name)
        dbcursor = oracle_conn.cursor()
        dbcursor.execute(delete_sql)
    except Exception as e:
        run_insert = False
        pass

    if run_insert:
        if (table_name.find("_VIEW")) != -1:
            table_name1 = table_name.split('_VIEW')[0]
            date_type_sql = """select column_name, data_type from all_tab_columns where upper(table_name) = '{table_name1}' and upper(owner) = upper('{owner}')""".format(table_name1=table_name1, owner=config.local_oracle_user)
            # df.to_sql(table_name, oracle_conn, if_exists='replace', index=False)
        else:
            date_type_sql = """select column_name, data_type from all_tab_columns where upper(table_name) = '{table_name}' and upper(owner) = upper('{owner}')""".format(table_name=table_name, owner=config.local_oracle_user)

        print(date_type_sql)
        column_types = pd.read_sql_query(date_type_sql, oracle_conn)

        partcode = GetNextPartCodeData(oracle_conn, table_name)
        total_size = my_duck.get_row_count_in_duckdb(duck_db, tableName=table_name + "." + table_name)

        print(f"{table_name} >>> 读取{total_size}行 >>> partcode >>> {partcode}")

        batch_size = 100000

        for offset in range(0, total_size, batch_size):
            query_sql = """select * from {table_name}.{table_name} limit {batch_size} offset {offset}""".format(
                table_name=table_name, batch_size=batch_size, offset=offset)
            df = pd.read_sql_query(query_sql, duck_db)
            df.columns = df.columns.str.lower()
            df['guid'] = uuid
            df['parentid'] = uuid
            df['sync_time'] = current_time_short
            df['partkey'] = current_time_short
            df['partcode'] = partcode
            df['partkey'] = current_time_short
            # if 'prev_op_comp_time' in df.columns:
            #     df['prev_op_comp_time'].fillna('1970-01-01 00:00:00', inplace=True)
            #     df['prev_op_comp_time'] = pd.to_datetime(df['prev_op_comp_time'], format='mixed')
            # if 'process_start_time' in df.columns:
            #     df['process_start_time'].fillna('1970-01-01 00:00:00', inplace=True)
            #     df['process_start_time'] = pd.to_datetime(df['process_start_time'], format='mixed')
            #
            # if 'op_start_date_time' in df.columns:
            #     df['op_start_date_time'].fillna('1970-01-01 00:00:00', inplace=True)
            #     df['op_start_date_time'] = pd.to_datetime(df['op_start_date_time'], format='mixed')
            # if 'parentid' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('parentid')]
            #     # if aaa.shape[0] > 0:
            #     df['parentid'] = uuid
            # if 'sync_time' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('sync_time')]
            #     # if aaa.shape[0] > 0:
            #     df['sync_time'] = current_time_short
            # if 'partcode' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('partcode')]
            #     # if aaa.shape[0] > 0:
            #     df['partcode'] = partcode
            # if 'parentid' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('parentid')]
            #     # if aaa.shape[0] > 0:
            #     df['parentid'] = uuid
            # if 'sync_time' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('sync_time')]
            #     # if aaa.shape[0] > 0:
            #     df['sync_time'] = current_time_short
            # if 'partcode' not in df.columns:
            #     # aaa = column_types[column_types['COLUMN_NAME'].str.contains('partcode')]
            #     # if aaa.shape[0] > 0:
            #     df['partcode'] = partcode


            column_types['COLUMN_NAME'] = column_types['COLUMN_NAME'].str.lower()
            column_types['DATA_TYPE'] = column_types['DATA_TYPE'].str.lower()

            df = df.replace(pd.NaT, np.nan)
            df = df.replace(np.nan, '')

            for col in df.columns:
                aaa = column_types[column_types['COLUMN_NAME'].str.contains(col)]
                if aaa.shape[0] > 0:
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0] == 'date':
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except Exception as e:
                            try:
                                df[col].fillna("", inplace=True)
                            except Exception as eee:
                                try:
                                    print("---------------------------!!!!!")
                                    df[col].fillna('1970-01-01 00:00:00', inplace=True)
                                    df[col] = pd.to_datetime(df[col], format='mixed')
                                except Exception as fff:
                                    df[col].fillna("'null'", inplace=True)
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0] == 'number':
                        df[col] = pd.to_numeric(df[col])
                        df[col].fillna(0, inplace=True)
                        df[col].replace('', 0, inplace=True)
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0].startswith('varchar'):
                        df[col].fillna('', inplace=True)
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0].startswith('char'):
                        df[col].fillna('', inplace=True)
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0].startswith('varchar2'):
                        df[col].fillna('', inplace=True)
                    if column_types[column_types['COLUMN_NAME'] == col]['DATA_TYPE'].values[0].startswith('float'):
                        df[col] = pd.to_numeric(df[col])
                        df[col].fillna(0.0, inplace=True)
                        df[col].replace('', 0.0, inplace=True)
                else:
                    # Duckdb有，Oracle中不存在这列的时候
                    del df[col]
            if(table_name.find("_VIEW")) != -1 :
                table_name1 = table_name.split('_VIEW')[0]
                insert_sql = f"""insert into {table_name1} ({', '.join(df.columns)}) values( {', '.join([':' + str(i + 1) for i in range(len(df.columns))])} )"""
            # df.to_sql(table_name, oracle_conn, if_exists='replace', index=False)
            else :
                insert_sql = f"""insert into {table_name} ({', '.join(df.columns)}) values( {', '.join([':' + str(i + 1) for i in range(len(df.columns))])} )"""
            print(df.shape)
            data_list = [tuple(row) for row in df.to_numpy()]
            print("111::::", len(data_list[0]))
            print(insert_sql)
            try:
                dbcursor.executemany(insert_sql, data_list)
            except Exception as ee:
                print("====================================================")
                print(ee)
                print(data_list[0])

            start_row = offset + 1
            end_row = min(offset + batch_size, total_size)
            print(f"........读取{start_row}到{end_row}行")

            del df

    if dbcursor:
        dbcursor.close()
    if os.path.exists(csv_file_name):
        os.remove(csv_file_name)

    HandlingVerControl(conn=oracle_conn, uuid=uuid, TableName=table_name)


def export_duckdb_to_oracle(duckdb_file, truncate_tables):
    '''将DuckDB结果导入到Oracle'''
    duck_db = None
    oracle_conn = None
    try:
        duck_db = duckdb.connect(duckdb_file)
        oracle_conn = get_test_oracle_connection()


        for _t in truncate_tables:
            delete_sql = """truncate table {table_name}""".format(table_name=_t)
            dbcursor = oracle_conn.cursor()
            dbcursor.execute(delete_sql)

        sql = """ select * from APS_SRCFILE """
        duck_db.execute(sql)
        used_file_list = duck_db.fetchall()
        # 遍历使用到的所有数据文件
        for item in used_file_list:
            # 将使用的文件attach进来

            my_duck.attach_table(duck_db, item[0], item[1])

            if not os.path.exists(r"d:\debug"):
                os.makedirs(r"d:\debug")
            shutil.copy(item[1], os.path.join(r"d:\debug", item[0] + ".db"))

            _export_duckdb_to_oracle(duck_db=duck_db, oracle_conn=oracle_conn, table_name=item[0], truncate_tables=truncate_tables)
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        if duck_db:
            duck_db.close()
        if oracle_conn:
            oracle_conn.commit()
            oracle_conn.close()


def _diff_row_pandas(table_name, duck_pandas, oracle_pandas, diff_result_folder, offset):
    _i = table_name.index('.') + 1

    # duck_pandas.reset_index(inplace=True)
    # oracle_pandas.reset_index(inplace=True)
    duck_pandas.fillna("", inplace=True)
    oracle_pandas.fillna("", inplace=True)
    duck_pandas.columns = duck_pandas.columns.str.lower()
    oracle_pandas.columns = oracle_pandas.columns.str.lower()
    # duck_pandas.drop('r', axis=1, inplace=True)
    # oracle_pandas.drop('r', axis=1, inplace=True)

    if oracle_pandas.shape[0] > 0 and duck_pandas.shape[0] > 0:

        diff_cols = []
        for _col in duck_pandas.columns:
            if _col.lower() != "partcode" and _col.lower() != "update_time" and _col.lower() != "parent_id" and _col.lower() != "sync_time" and _col.lower() != "r":
                diff_cols.append(_col)
        # diff_cols.append('r')

        duck_pandas[diff_cols] = duck_pandas[diff_cols].astype(str).apply(lambda x: x.str.strip())
        oracle_pandas[diff_cols] = oracle_pandas[diff_cols].astype(str).apply(lambda x: x.str.strip())

        csv_file = os.path.join(diff_result_folder, table_name + "_" + str(offset) + ".db")
        duck_db = duckdb.connect(csv_file)

        merge_result = duck_pandas[diff_cols].eq(oracle_pandas[diff_cols])
        mask = merge_result.any(axis=1)
        merge_result = merge_result.applymap(lambda x: 'N' if not x else 'Y')
        out_df = pd.concat([duck_pandas.add_prefix('K_'), oracle_pandas[diff_cols].add_prefix('O_')], axis=1)
        # out_df = pd.concat([out_df, merge_result], axis=1)
        out_df = out_df[mask]
        out_df = pd.concat([out_df, merge_result], axis=1, join='inner')

        # out_df.to_csv(csv_file, index=False)
        try:
            duck_db.sql("create table {} as select * from out_df".format(table_name))

            # for item in out_df.columns:
            #     print(item)
            #
            # data = out_df.to_numpy()
            # insert_columns = "?,".join(["" for i in range(0, out_df.shape[1] + 1)])
            # insert_columns = insert_columns[:-1]
            # # print(insert_columns)
            # insert_sql = " insert into {table_name} values({insert_columns})".format(table_name=table_name, insert_columns=insert_columns)
            # print(insert_sql)
            # duck_db.executemany(insert_sql, data)

        except AttributeError as eee:
            print(eee)
        finally:
            duck_db.close()


def _diff_row_content(duck_db, oracle_conn, table_name, diff_result_folder, row_in_duck, row_in_oracle):
    _i = table_name.index('.') + 1
    sql_duck = "select * from {}".format(table_name)

    diff_row_count = min(row_in_duck, row_in_oracle)
    batch_size = 1000000

    # for offset in range(0, diff_row_count, batch_size):
    #     # 内容比较
    #     # ——————————————————————————————————————————————————————————————————————————————————————————————————————————
    #     result_temp = None
    #     result_temp = duck_db.execute(sql_duck)
    #     columns = " DESC, ".join([col[0] for col in result_temp.description])
    #     _order_condition = " ORDER BY " + columns
    #     _column_condition = " , ".join([col[0] for col in result_temp.description])
    #
    #     # Duck DB 检索SQL
    #     sql_duck_search = f"select ROW_NUMBER() OVER() AS r, {_column_condition} from {table_name} {_order_condition} LIMIT {batch_size} offset {offset}"
    #     print(sql_duck_search)
    #     pd_resul_duck = None
    #     pd_resul_duck = pd.read_sql_query(sql_duck_search, duck_db)
    #     print(pd_resul_duck.shape)
    #
    #     # sql_oracle = "select {colunms} from {table} where 1=1 and ROWNUM <= {limit}".format(
    #     #     colunms=_column_condition, table=table_name[_i:], limit=max_row_limit)
    #     sql_oracle = " select * from (select {colunms}, ROWNUM r from {table} where 1=1 {_order_condition}) where r > {offset} and r <= {limit}".format(colunms=_column_condition,
    #                                                                                                                                  table=table_name[_i:],
    #                                                                                                                                  offset=offset,
    #                                                                                                                                  limit=offset+batch_size,
    #                                                                                                                                 _order_condition=_order_condition)
    #     sql_oracle_search = sql_oracle + _order_condition
    #     print(sql_oracle)
    #     pd_result_oracle = None
    #     pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_conn)
    #     print(pd_result_oracle.shape)
    #
    #     _diff_row_pandas(table_name, pd_resul_duck, pd_result_oracle, diff_result_folder, offset)
    #
    #     start_row = offset + 1
    #     end_row = min(offset + batch_size, diff_row_count)
    #     print(f"........对比{start_row}到{end_row}行")

    # 内容比较
    # ——————————————————————————————————————————————————————————————————————————————————————————————————————————
    result_temp = None
    result_temp = duck_db.execute(sql_duck)
    columns = " DESC, ".join([col[0] for col in result_temp.description])
    _order_condition = " ORDER BY " + columns
    _column_condition = " , ".join([col[0] for col in result_temp.description])

    # Duck DB 检索SQL
    sql_duck_search = f"select {_column_condition} from {table_name} where 1=1 {_order_condition} "
    print(sql_duck_search)
    pd_resul_duck = None
    pd_resul_duck = pd.read_sql_query(sql_duck_search, duck_db)
    print(pd_resul_duck.shape)
    pd_result_duck_sort = []
    for _col in pd_resul_duck.columns:
        pd_result_duck_sort.append(_col)
    pd_resul_duck = pd_resul_duck.sort_values(by=pd_result_duck_sort)

    # sql_oracle = "select {colunms} from {table} where 1=1 and ROWNUM <= {limit}".format(
    #     colunms=_column_condition, table=table_name[_i:], limit=max_row_limit)
    sql_oracle = " select {colunms} from {table} where 1=1 ".format(
        colunms=_column_condition,
        table=table_name[_i:])
    sql_oracle_search = sql_oracle + _order_condition
    print(sql_oracle_search)
    pd_result_oracle = None
    pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_conn)
    print(pd_result_oracle.shape)
    pd_result_oracle_sort = []
    for _col in pd_result_oracle.columns:
        pd_result_oracle_sort.append(_col)
    pd_result_oracle = pd_result_oracle.sort_values(by=pd_result_oracle_sort)

    _diff_row_pandas(table_name, pd_resul_duck, pd_result_oracle, diff_result_folder, 0)


def _diff(duck_db, oracle_conn, table_name, diff_result_folder):
    _i = table_name.index('.') + 1
    # print(f" DuckDB({table_name}) VS Oracle({table_name[_i:]})")
    print(table_name + ">>>>对比中")

    # 件数比较
    row_in_duck = my_duck.get_row_count_in_duckdb(duck_db, table_name)
    row_in_oracle = get_row_count_in_oracle(oracle_conn, table_name[_i:])

    _count_result_file = os.path.join(diff_result_folder, '_row_count_' + table_name[_i:] + '.txt')
    count_result_file = open(_count_result_file, 'w')
    count_result_file.writelines("DUCK DB ROW|" + table_name + " | " + str(row_in_duck) + "\n")
    count_result_file.writelines("Oracle  ROW|" + table_name + " | " + str(row_in_oracle) + "\n")
    count_result_file.close()

    # diff_row_count = min(row_in_duck, row_in_oracle)
    _diff_row_content(duck_db, oracle_conn, table_name, diff_result_folder, row_in_duck,
                      row_in_oracle)


def diff(table_name, duckdb_file, duckdb_temp_file, diff_result_folder):
    duck_db = None
    oracle_conn = None
    try:
        duck_db = duckdb.connect(database=':memory:')
        oracle_conn = get_test_oracle_connection()

        need_diff_tables = []
        my_duck.attach_table(duck_db, table_name, duckdb_file)
        need_diff_tables.append(table_name + '.' + table_name)
        if duckdb_temp_file:
            my_duck.attach_table(duck_db, 'tempdb', duckdb_temp_file)
            sql = """select DISTINCT(table_name)  from tempdb.information_schema.columns"""
            duck_db.execute(sql)
            _tables = duck_db.fetchall()
            for item in _tables:
                if item[0] != "APS_SRCFILE" and item[0] != table_name:
                    need_diff_tables.append('tempdb.' + item[0])


        # print(need_diff_tables)

        for item in need_diff_tables:
            _diff(duck_db=duck_db, oracle_conn=oracle_conn, table_name=item, diff_result_folder=diff_result_folder)

    except Exception as e:
        traceback.print_exc()
    finally:
        if duck_db:
            duck_db.close()
        if oracle_conn:
            oracle_conn.close()


def del_result_files(diff_result_folder):
    # 准备结果路径
    if not os.path.exists(diff_result_folder):
        os.makedirs(diff_result_folder)
    else:
        for root, dirs, files in os.walk(diff_result_folder):
            for file in files:
                if file.endswith(".txt") or file.endswith(".csv") or file.endswith(".db") or file.endswith(".wal"):
                    print(f'Delete file:{file}')
                    os.remove(os.path.join(root, file))


if __name__ == '__main__':
    # 使用说明：
    # 1)    config.g_debug_mode = True
    # 2)    输出结果路径
    # 3)    table_name         要对比的结果表名称
    # 4)    duckdb_file        Python版本跑出的结果文件路径
    # 5)    duckdb_temp_file   针对几本大的FLOW, WIP, RLS(准备修改),会将TEMP表汇出为单独的文件
    #                          注意：只有 config.g_debug_mode = True的时候，该文件才会被保留
    # 6）   执行顺序
    #       > config.g_debug_mode = True
    #       > 执行python版本程序
    #       ------------------------
    #       > 修改下面 [diff_result_folder] [table_name] [duckdb_file] [duckdb_temp_file]的值
    #       > 注释掉 diff(table_name=table_name, duckdb_file=duckdb_file, duckdb_temp_file=duckdb_temp_file, diff_result_folder=diff_result_folder)
    #       > 执行本程序，将Duckdb的结果导出到99的phsche中去
    #       ------------------------
    #       > 修改Java程序的Oracle连接到【99的phsche】
    #       > 执行Java版本程序
    #       ------------------------
    #       > 注释掉 export_duckdb_to_oracle(duckdb_file)
    #       > 解开注释 diff(table_name=table_name, duckdb_file=duckdb_file, duckdb_temp_file=duckdb_temp_file, diff_result_folder=diff_result_folder)
    #       > 执行本程序
    #       ------------------------
    #       > 在diff_result_folder = r'd:\debug'检查结果

    diff_result_folder = r'd:\debug'

    truncate_tables = ['APS_ETL_DEMAND']


    table_name = 'APS_ETL_DEMAND'  # TODO
    duckdb_file = r'D:\workspace\ETLOutput\APS_ETL_DEMAND\APS_ETL_DEMAND_20231106080729.db'  # TODO
    duckdb_temp_file = r'R:\workspace\APS_ETL_DEMAND\inprocess\APS_ETL_DEMAND_20231106080729_temp.db'  # TODO

    warnings.filterwarnings("ignore")

    # 将Duckdb结果导出到Oracle TODO
    export_duckdb_to_oracle(duckdb_file, truncate_tables)

    del_result_files(diff_result_folder)

    # 对比结果 TODO
    # diff(table_name=table_name, duckdb_file=duckdb_file, duckdb_temp_file=duckdb_temp_file, diff_result_folder=diff_result_folder)


