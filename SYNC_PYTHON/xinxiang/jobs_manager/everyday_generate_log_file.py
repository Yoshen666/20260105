import os

from xinxiang import config
from xinxiang.util import my_log


def execute():
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_SYNC.log"))  # 初期化Logger
