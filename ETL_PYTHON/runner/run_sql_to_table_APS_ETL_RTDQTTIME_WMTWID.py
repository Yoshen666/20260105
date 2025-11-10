import sys
import warnings

import win32serviceutil
import win32service
import win32event
import os
import logging
import inspect
import servicemanager
import winerror

sys.path.insert(0, sys.path[0])
sys.path.insert(0, os.path.join(sys.path[0], ".."))
from xinxiang import main, config
from xinxiang.util import my_log
from xinxiang.jobs_etl import sql_to_table_APS_ETL_RTDQTTIME_WMTWID

if __name__ == '__main__':
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log"))  # 初期化Logger
    logging.info("service is run....")
    sql_to_table_APS_ETL_RTDQTTIME_WMTWID.execute()