import sys
import os
import logging

sys.path.insert(0, sys.path[0])
sys.path.insert(0, os.path.join(sys.path[0], ".."))
from xinxiang import main, config
from xinxiang.util import my_log
from xinxiang.jobs_copy import copy_etl_for_backup

if __name__ == '__main__':
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log"))  # 初期化Logger
    logging.info("service is run....")
    copy_etl_for_backup.execute()