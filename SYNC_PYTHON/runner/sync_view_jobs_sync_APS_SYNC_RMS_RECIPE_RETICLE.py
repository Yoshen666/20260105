import os
import sys
sys.path.insert(0, sys.path[0])
sys.path.insert(0, os.path.join(sys.path[0], ".."))
from xinxiang import main, config
from xinxiang.util import my_log
from xinxiang.jobs_sync_view import sync_view_jobs

if __name__ == '__main__':
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_SYNC.log"))  # 初期化Logger
    sync_view_jobs.sync_APS_SYNC_RMS_RECIPE_RETICLE()
                    
