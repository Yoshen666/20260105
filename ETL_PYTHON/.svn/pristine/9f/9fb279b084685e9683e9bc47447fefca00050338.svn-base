# sync 产出路径
import os
import shutil
from datetime import datetime

import duckdb
import pandas as pd

g_mem_sync_result_path = 'D:\workspace\SrcData'

target_table_name = "CSFRINHIBIT"

create_table_sql = """
CREATE TABLE {table_name}(         
    PRODUCTID VARCHAR(50),     
    ROUTEID VARCHAR(50),       
    OPENO VARCHAR(50),         
    PASS_FLG_MFG VARCHAR(50),  
    PASS_FLG_PRC VARCHAR(50),  
    EQUIPMENTID VARCHAR(50),   
    CREATETIME VARCHAR(50),    
    PRC_UPT_USER VARCHAR(50),  
    PRC_UPT_TIME VARCHAR(50),  
    PRC_MEMO VARCHAR(50),      
    MFG_UPT_USER VARCHAR(50),  
    MFG_UPT_TIME VARCHAR(50),  
    MFG_MEMO VARCHAR(50),      
    PARTNAME VARCHAR(50)       
)
""".format(table_name=target_table_name)

table_colmns = [
    'PRODUCTID',
    'ROUTEID',
    'OPENO',
    'PASS_FLG_MFG',
    'PASS_FLG_PRC',
    'EQUIPMENTID',
    'CREATETIME',
    'PRC_UPT_USER',
    'PRC_UPT_TIME',
    'PRC_MEMO',
    'MFG_UPT_USER',
    'MFG_UPT_TIME',
    'MFG_MEMO',
    'PARTNAME'
]

def date_time_second_str():
    times = datetime.now()
    str = times.strftime('%Y-%m-%d %H:%M:%S')
    return str


def date_time_second_short_str():
    times = datetime.now()
    str = times.strftime('%Y%m%d%H%M%S')
    return str


def create_duck_db(data):
    target_folder = os.path.join(g_mem_sync_result_path, target_table_name, "inprocess")
    if not os.path.isdir(target_folder):
        os.makedirs(target_folder)

    _file_name = target_table_name + "_" + date_time_second_short_str() + ".db"
    in_process_db_file = os.path.join(target_folder, _file_name)
    target_db_file = os.path.join(g_mem_sync_result_path, target_table_name, _file_name)  # 最终文件在inprocess目录上层

    duck_db_cursor = None
    try:
        # 创建DuckDB文件
        duck_db_cursor = duckdb.connect(in_process_db_file)

        # 创建表
        duck_db_cursor.sql(create_table_sql)

        # 构建Pandas
        df = pd.DataFrame(data, columns=table_colmns)

        # 插入数据
        insert_sql = "INSERT INTO {} select * from df".format(target_table_name)
        duck_db_cursor.sql(insert_sql)

    except Exception as e:
        print(e)
    finally:
        duck_db_cursor.close()

        # 从inprocess目录移动到上层目录
        shutil.move(in_process_db_file, target_db_file)


if __name__ == '__main__':
    # TODO 这里改成你们的List
    data = [['1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1']]
    create_duck_db(data)

