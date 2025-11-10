import logging
import os
import shutil
import time
from datetime import datetime

import duckdb

from xinxiang import config
from xinxiang.util import my_file, my_date, my_oracle
from xinxiang.util.my_oracle import SaveAlarmLogDataForSync


def get_temp_table_mark():
    if config.g_debug_mode:
        return "TEMPDB."
    else:
        return ""


def get_select_column_in_duckdb(duckdb_in_memory, table_name):
    sql = """
        select COLUMN_NAME from information_schema.columns
        where upper(table_name) = upper('{table_name}')
    """.format(table_name=table_name)
    duckdb_in_memory.execute(sql)
    columns = duckdb_in_memory.fetchall()
    columns_str = ','.join(column[0] for column in columns if column[0] != 'PARTKEY')
    return columns_str


def exec_sql(oracle_conn, duck_db_memory, ETL_Proc_Name, methodName, sql, current_time, update_table=None):
    start_time = time.time()
    try:
        if config.g_print_flag:
            print(sql)
        duck_db_memory.sql(sql)
    except Exception as e:
        if config.g_print_flag:
            print(e)
        raise e
    finally:
        end_time = time.time()
        if config.g_print_flag:
            print(ETL_Proc_Name, "SQL执行时间", methodName, str(end_time - start_time) + "秒")
            # if update_table is not None:
            #     row = get_row_count_in_duckdb(duck_db_memory, tableName=update_table, conditionSql=None)
            #     print(ETL_Proc_Name, "SQL更新行数", update_table, str(row) + "行")

        my_oracle.SaveEtlMethodLog(oracleConn=oracle_conn,
                                   etlJob=ETL_Proc_Name,
                                   methodName=methodName,
                                   startTime=start_time,
                                   endTime=end_time,
                                   etlJobTime=current_time)


def get_row_count_in_duckdb(duck_db, tableName="", conditionSql=None):
    '''
    得到某表的行数
    :param duck_db:
    :param tableName:
    :param conditionSql:
    :return:
    '''
    sql = """
    select count(1) as rrr_COUNT from {tableName}
    where 1 = 1
    """.format(tableName=tableName)
    if conditionSql and conditionSql != "":
        sql = sql + " and " + conditionSql
    with duck_db.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0] if result else 0


def create_duckdb_for_temp_table(file_path, file_name):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    duck_db = duckdb.connect(file_name)
    return duck_db


def create_duckdb_in_file(file_path, file_name, target_table_sql):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    duck_db = duckdb.connect(file_name)
    sql = """
        create table APS_SRCFILE(
            table_name varchar(100),
            file_name  varchar(1000)
        )
        """
    duck_db.sql(sql)
    duck_db.sql(target_table_sql)
    return duck_db


def create_duckdb_in_momory(target_table_sql):
    '''
    在内存中创建DuckDB，但调试模式下会创建debug数据库
    :param target_table_sql:
    :return:
    '''
    duck_db = None
    if config.g_debug_mode:
        if os.path.exists(config.g_debug_file):
            os.remove(config.g_debug_file)
        duck_db = duckdb.connect(config.g_debug_file)
    else:
        # os.environ["DUCKDB_MEMORY_MODE"] = "Compact"
        duck_db = duckdb.connect(database=':memory:')
    sql = """
    create table APS_SRCFILE(
        table_name varchar(100),
        file_name  varchar(1000)
    )
    """
    duck_db.sql(sql)
    duck_db.sql(target_table_sql)
    return duck_db


def drop_table(duck_db, table_name):
    # sql = " drop table {table_name} ".format(table_name=table_name)
    # duck_db.sql(sql)
    sql = " TRUNCATE  {table_name} ".format(table_name=table_name)
    duck_db.sql(sql)
    sql = "VACUUM"
    duck_db.sql(sql)


def attach_temp_db_write_able(duck_db_memory, table_name, target_db_file):
    sql = "ATTACH '{file_name}' AS {table_name}".format(file_name=target_db_file, table_name=table_name)
    duck_db_memory.sql(sql)


def attach_table(duck_db_memory, table_name, target_db_file):
    sql = "ATTACH '{file_name}' AS {table_name} (READ_ONLY) ".format(file_name=target_db_file, table_name=table_name)
    duck_db_memory.sql(sql)


def detach_table(duck_db, db_name):
    sql = "DETACH {db_name}".format(db_name=db_name)
    duck_db.sql(sql)
    sql = "VACUUM"
    duck_db.sql(sql)


def get_all_used_table_file(conn, duck_db, used_table_list):
    used_dict = {}
    if used_table_list is not None:
        for db_name in used_table_list:
            if db_name is not None:
                file_name = my_file.get_last_db_file(conn, db_name)
                if file_name is not None:
                    used_dict[db_name] = file_name
                else:
                    raise Exception("数据不存在:{db_name}".format(db_name=db_name))
    return used_dict


def attach_used_table(conn, duck_db, used_table_list):
    '''
    将参考到的表都ATTACH进来，且在APS_SRCFILE中记录使用的记录
    :param conn:
    :param duck_db:
    :param used_table_list:
    :return:
    '''
    res_dict = {}
    if used_table_list is not None:
        for db_name in used_table_list:
            if db_name is not None:
                file_name = my_file.get_last_db_file(conn, db_name)
                if file_name is not None:
                    sql = "ATTACH '{file_name}' AS {db_name} (READ_ONLY)".format(file_name=file_name, db_name=db_name)
                    res_dict[db_name] = file_name
                    duck_db.sql(sql)
                    sql = "insert into APS_SRCFILE(table_name, file_name) values('{table_name}', '{file_name}')".format(table_name=db_name, file_name=file_name)
                    duck_db.sql(sql)
                else:
                    raise Exception("数据不存在:{db_name}".format(db_name=db_name))
    return res_dict


def detach_all_used_table(duck_db, used_dict):
    for item in used_dict.keys():
        sql = "DETACH {db_name}".format(db_name=item)
        duck_db.sql(sql)


def get_target_file_name(target_table, current_time):
    _file_name = target_table + "_" + current_time + ".db"
    return os.path.join(config.g_mem_etl_output_path, target_table, _file_name)


def export_result_duck_file_and_close_duck_db_memory2(duck_db_memory, in_process_db_file, target_table, current_time):
    _ssss = datetime.now()
    _file_name = target_table + "_" + current_time + ".db"
    target_table_path = os.path.join(config.g_mem_etl_output_path, target_table)
    if not os.path.exists(target_table_path):
        os.makedirs(target_table_path)
    target_db_file = os.path.join(config.g_mem_etl_output_path, target_table, _file_name)

    # 关闭文件
    if duck_db_memory:
        duck_db_memory.commit()
        duck_db_memory.close()
    _eeeee = datetime.now()
    print("Save File Time:", my_date.duration(_ssss, _eeeee), 'Seconds')

    _ssss = datetime.now()
    try:
        # os.rename(in_process_db_file, target_db_file)
        if os.path.exists(in_process_db_file):
            shutil.move(in_process_db_file, target_db_file)
    except Exception as eee:
        logging.exception("文件太大，等待20秒后再拷贝... %s", eee)
        time.sleep(20)
        if os.path.exists(in_process_db_file):
            shutil.move(in_process_db_file, target_db_file)

    _eeeee = datetime.now()
    print("Move File Time:", my_date.duration(_ssss, _eeeee), 'Seconds')

    return target_db_file

def export_result_duck_file_and_close_duck_db_memory(duck_db_memory, target_table, target_table_sql, current_time):
    '''
    导出结果并且关闭DuckDB数据库
    :param duck_db_memory:
    :param target_table:
    :param target_table_sql:
    :param current_time:
    :return:
    '''
    target_path = os.path.join(config.g_mem_etl_output_path, target_table, "inprocess")
    if not os.path.isdir(target_path):
        os.makedirs(target_path)
    _file_name = target_table + "_" + current_time + ".db"
    in_process_db_file = os.path.join(target_path, _file_name)
    target_db_file = os.path.join(config.g_mem_etl_output_path, target_table, _file_name)

    duck_db_file = duckdb.connect(in_process_db_file)
    # 在文件DB中创建表
    duck_db_file.sql(target_table_sql)
    duck_db_file.close()
    del duck_db_file
    duck_db_memory.sql("ATTACH '{duck_db_file}' as file_db".format(duck_db_file=in_process_db_file))

    # 结果表
    sql = """
    insert into file_db.{target_table} select * from {target_table}
    """.format(target_table=target_table)
    duck_db_memory.sql(sql)

    # 参考文件表
    sql = """
        create table file_db.APS_SRCFILE(
        table_name varchar(100),
        file_name  varchar(1000)
    )
    """
    duck_db_memory.sql(sql)
    sql = """
        insert into file_db.APS_SRCFILE select * from APS_SRCFILE
        """
    duck_db_memory.sql(sql)

    duck_db_memory.close()
    del duck_db_memory

    # 从inprocess目录移动到上层目录
    try:
        os.rename(in_process_db_file, target_db_file)
    except Exception as eee:
        logging.exception("文件太大，等待20秒后再拷贝... %s", eee)
        time.sleep(20)
        os.rename(in_process_db_file, target_db_file)

    return target_db_file

def doubleCheckView(duck_db_cursor, sql, conn, source_table):

    # 从inprocess目录移动到上层目录
    try:
        duck_db_cursor.execute(sql)
        result = duck_db_cursor.fetchone()
        # 必须在v_setting_view_nullskip外
        skipRowNum = my_oracle.GetCommonSkipViewExistTableData(conn, source_table)
        # 如果不在可放行的表里，数量为0.则直接报错
        if skipRowNum == 0 and result[0] == 0:
            SaveAlarmLogDataForSync(conn,
                                    ETLProcName="Sync " + source_table,
                                    Exception="Sync " + source_table + "查询数据0行",
                                    file_name="Sync Error",
                                    alarm_code="XETL0001")
            return False
    except Exception as eee:
        logging.exception(eee)
        raise eee

    return True