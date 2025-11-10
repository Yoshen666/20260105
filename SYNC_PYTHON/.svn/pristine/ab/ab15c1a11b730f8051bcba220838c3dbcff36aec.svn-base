import logging
import os
import subprocess

from xinxiang import config


def exec_view_to_dat(file_name, query_sql, split_char, is_append_header):
    head = "no"
    if is_append_header:
        head = "yes"
    # sqluldr264 user=%s/%s@%s query=\"%s\" field=\"%s\" head=%s file=%s charset=AL32UTF8
    cmder = """sqluldr264 user={g_oracle_user}/{g_oracle_password}@{g_oracle_dsn} query="{query_sql}" field="{split_char}" head={head} file={file_name} charset=AL32UTF8""".format(
        g_oracle_user=config.g_oracle_user,
        g_oracle_password=config.g_oracle_password,
        g_oracle_dsn=config.g_oracle_dsn,
        query_sql=query_sql,
        split_char=split_char,
        head=head,
        file_name=file_name)

    logging.info(cmder)
    result = exec(cmder)
    return result


def exec(cmder):
    print(cmder)
    logging.info(cmder)
    os.system(cmder)


if __name__ == '__main__':
    pass
    # print("aaaaaa")
    # table_name = "V_CSFLOWSAMPLINGCFG"
    # target_table = "APS_SYNC_CSFLOWSAMPLINGCFG"
    # query_sql = "select * from V_CSFLOWSAMPLINGCFG"
    # file_name = r"D:\aa.csv"
    # in_process_db_file = r"d:\aa.db"
    # exec_view_to_dat(file_name=file_name, query_sql=query_sql, split_char="0x09", is_append_header=True)
    # import duckdb
    # duckdb = duckdb.connect(in_process_db_file)
    # sql = """
    # create table {target_table} AS FROM read_csv_auto('{file_name}')
    # """.format(target_table=target_table, file_name=file_name)
    # duckdb.execute(sql)
    # duckdb.close()



