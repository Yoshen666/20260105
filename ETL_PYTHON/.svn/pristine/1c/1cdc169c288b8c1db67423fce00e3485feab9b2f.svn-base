import datetime
import logging
import os

from xinxiang import config
from xinxiang.util import my_log


def execute():
    while True:
        # my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log"))  # 初期化Logger
        # logging.info("log_file is rebuild....")
        if is_rename():
            break

def is_rename():
    file_name = os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log")
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    target_file_name = os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log." + now_date)
    try:
        if os.path.exists(file_name):
            os.rename(file_name, target_file_name)
            return True
        else:
            return True
    except Exception as e:
        return False
