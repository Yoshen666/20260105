import logging
import os
import shutil
import time
import traceback
from datetime import datetime, timedelta

import duckdb

from xinxiang import config
from xinxiang.util import my_cmder


def copy(folder, target_folder, first_copy_day=None, sync_minute=60):
    current_datetime = datetime.now()
    for root, dirs, files in os.walk(folder):
        for file in files:
            try:
                file_path = os.path.join(root, file) # 原文件路徑
                temp_target_path = file_path.replace(folder.replace("\SrcData", "").replace("\ETLOutput", ""), target_folder) # 目標Temp文件路徑
                if not os.path.exists(os.path.dirname(temp_target_path)):
                    os.makedirs(os.path.dirname(temp_target_path))
                target_path = temp_target_path.replace(r"\temp", "") # 目標文件路徑
                if not os.path.exists(os.path.dirname(target_path)):
                    os.makedirs(os.path.dirname(target_path))
                if not os.path.exists(target_path):
                    if os.path.isfile(file_path) and file_path.endswith(".db"):
                        _file_name = os.path.basename(file).replace(".db", "")
                        _create_date_str = _file_name.split("_")[-1]
                        table_name = "_".join(_file_name.split("_")[:-1])
                        _create_date = datetime.strptime(_create_date_str, '%Y%m%d%H%M%S')
                        if first_copy_day is None:
                            if current_datetime - _create_date < timedelta(minutes=sync_minute):
                                print("----", file_path, _file_name, _create_date, table_name, temp_target_path, target_path)
                                if not os.path.exists(target_path):
                                    write_db(table_name=table_name, srcFile=file_path, targetTempFile=temp_target_path, target_path=target_path)
                        else:
                            if current_datetime - _create_date < timedelta(days=1):
                                print("====", file_path, _file_name, _create_date, table_name, temp_target_path, target_path)
                                if not os.path.exists(target_path):
                                    write_db(table_name=table_name, srcFile=file_path, targetTempFile=temp_target_path, target_path=target_path)
            except Exception as e:
                print(e)
                continue


def write_db(table_name, srcFile, targetTempFile, target_path):
    try:
        shutil.copy(srcFile, target_path)

        # duck_db = duckdb.connect(database=os.path.normcase(targetTempFile))
        # duck_db.close()
        # duck_db = duckdb.connect(database=':memory:')
        #
        # sql = """attach '{srcFile}' as OLD_{table_name} (READ_ONLY)""".format(srcFile=srcFile, table_name=table_name)
        # duck_db.execute(sql)
        # sql = """attach '{targetFile}' as {table_name} """.format(targetFile=targetTempFile, table_name=table_name)
        # duck_db.execute(sql)
        # sql = """create table {table_name}.{table_name} as select * from OLD_{table_name}.{table_name}""".format(
        #     table_name=table_name)
        # duck_db.execute(sql)
        # duck_db.close()
        #
        # shutil.move(targetTempFile, target_path)
    except Exception as e:
        try:
            print("try copy file {} once again".format(srcFile))
            time.sleep(10)
            shutil.copy(srcFile, target_path)
        except Exception as ee:
            try:
                print("try copy file {} once again!".format(srcFile))
                time.sleep(10)
                shutil.copy(srcFile, target_path)
            except Exception as eee:
                try:
                    print("try copy file {} once again!".format(srcFile))
                    time.sleep(10)
                    shutil.copy(srcFile, target_path)
                except Exception as eeee:
                    try:
                        print("try copy file {} once again!".format(srcFile))
                        time.sleep(10)
                        shutil.copy(srcFile, target_path)
                    except Exception as eeeee:
                        try:
                            print("try copy file {} once again!".format(srcFile))
                            time.sleep(10)
                            shutil.copy(srcFile, target_path)
                        except Exception as eerwer:
                            print(eerwer)



def execute():
    copy(folder=config.g_mem_sync_result_path, target_folder=r'X:\temp')
    copy(folder=config.g_mem_etl_output_path, target_folder=r'X:\temp')


def init():
    copy(folder=config.g_mem_sync_result_path, target_folder=r'X:\temp', first_copy_day=1)
    copy(folder=config.g_mem_etl_output_path, target_folder=r'X:\temp', first_copy_day=1)


if __name__ == '__main__':
    init()