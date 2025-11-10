import logging
import os
import subprocess
from xinxiang import config

os.environ['PYTHONIOENCODING']='UTF-8'

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

def create_sqlldr_ctlfile(table_name,split_char,columns,import_file,column_split_char):
    ctrl_context = f"""options(skip=1,COLUMNARRAYROWS=10000, READSIZE=1048576, ERRORS=999999999)
    load data
    CHARACTERSET 'AL32UTF8'
    infile '{import_file}'
    append     
    into table "{table_name}"
    Fields terminated by '{split_char}'
    Optionally enclosed by '"'
    TRAILING NULLCOLS
    ({columns})"""

    # --Optionally enclosed by '{column_split_char}'

    folder = os.path.join(config.g_mem_etl_output_path,'ctrl')
    if not os.path.exists(folder):
        os.makedirs(folder)
    ctl_file=os.path.join(folder, table_name+".ctl");
    if not os.path.exists(ctl_file):
        with open(ctl_file, 'w') as w:
            w.write(ctrl_context)

    cmd = None
    if config.g_debug_mode:
        cmd = "sqlldr userid={user_name}/{password}@{dsn} control={ctl_file} direct=true log={log_name}\\sqlldr_log.log".format(
            user_name=config.local_oracle_user,
            password=config.local_oracle_password,
            dsn=config.local_oracle_dsn,
            ctl_file=ctl_file,
            log_name=config.g_log_path
        )
    else:
        cmd = "sqlldr userid={user_name}/{password}@{dsn} control={ctl_file} direct=true log={log_name}\\sqlldr_log.log".format(
            user_name=config.g_oracle_user,
            password=config.g_oracle_password,
            dsn=config.g_oracle_dsn,
            ctl_file=ctl_file,
            log_name=config.g_log_path
        )
    result = exec(cmd)
    if os.path.exists(ctl_file):
        os.remove(ctl_file)
    return result

def exec(cmder):
    print(cmder)
    logging.info(cmder)
    return os.system(cmder)


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



