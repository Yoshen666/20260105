import logging
import os
import shutil
from datetime import datetime, timedelta

import cx_Oracle

from xinxiang import config
from xinxiang.util import my_oracle, my_date


def handler_version(folder):
    oracle_conn = my_oracle.oracle_get_connection()

    for root, dirs, files in os.walk(folder):
        for _file in files:
            file_path = os.path.join(root, _file)
            if file_path.endswith(".db"):
                try:
                    _file_name = os.path.basename(_file).replace(".db", "")
                    _create_date_str = _file_name.split("_")[-1]
                    _table_name = "_".join(_file_name.split("_")[:-1])

                    print(_table_name, file_path)
                    my_oracle.HandlingVerControl(oracle_conn, my_oracle.UUID(), _table_name, file_path, my_date.date_time_second_short_str())
                except Exception as e:
                    print(e)
                    continue

    oracle_conn.commit()
    oracle_conn.close()

def copy_files(folder_from, folder_to, copy_hour=12):
    if not os.path.exists(folder_to):
        os.makedirs(folder_to)

    for root, dirs, files in os.walk(folder_from):
        for _file in files:
            file_path = os.path.join(root, _file)
            if file_path.endswith(".db"):
                try:
                    _file_name = os.path.basename(_file).replace(".db", "")
                    _create_date_str = _file_name.split("_")[-1]
                    _table_name = "_".join(_file_name.split("_")[:-1])

                    _create_date = datetime.strptime(_create_date_str, '%Y%m%d%H%M%S')

                    current_datetime = datetime.now()

                    if current_datetime - _create_date < timedelta(hours=copy_hour):
                        if not os.path.exists(os.path.join(folder_to, _table_name)):
                            os.makedirs(os.path.join(folder_to, _table_name))

                        _target_file_path = file_path.replace(folder_from, folder_to)

                        print(_file_name, _table_name, _create_date, file_path, ">>>", _target_file_path)

                        shutil.copy(file_path, _target_file_path)
                    else:
                        pass


                except Exception as e:
                    print(e)
                    continue

            # try:
            #     file_path = os.path.join(root, file)
            #     if os.path.isfile(file_path):
            #         print(os.path.abspath(file))
            #         # _file_name = os.path.basename(file).replace(".db", "")
            #         #
            #         # if not _file_name.__contains__(".wal"):
            #         #     _create_date_str = _file_name.split("_")[-1]
            #         #     _create_date = datetime.strptime(_create_date_str, '%Y%m%d%H%M%S')
            #         #
            #         #     if current_datetime - _create_date > timedelta(days=save_days):
            #         #         logging.info("Delete Duck File:" + file_path)
            #         #
            #         #         os.remove(file_path)
            # except Exception as e:
            #     continue


if __name__ == '__main__':
    # folder_from = r'D:\workspace\ETLOutput'
    # folder_to = r'D:\workspace_copy\ETLOutput'
    # copy_files(folder_from, folder_to)
    #
    # folder_from = r'D:\workspace\SrcData'
    # folder_to = r'D:\workspace_copy\SrcData'
    # copy_files(folder_from, folder_to, copy_hour=1)

    folder = r'D:\workspace\CSFRINHIBIT'
    handler_version(folder)