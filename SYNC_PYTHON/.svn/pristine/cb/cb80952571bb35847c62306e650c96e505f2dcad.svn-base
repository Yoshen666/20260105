# encoding=utf-8
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
from xinxiang import main, config
from xinxiang.util import my_log


class ETLPythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonService_Sync"  # 服务名
    _svc_display_name_ = "Sync服务"  # 服务在windows系统中显示的名称
    _svc_description_ = "Python版本的Sync服务"  # 服务的描述

    def __init__(self, args):
        warnings.filterwarnings("ignore")
        my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_SYNC.log")) # 初期化Logger
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.run = True

    def SvcDoRun(self):
        logging.info("service is run....")
        # 执行apscheduler
        main()

    def SvcStop(self):
        logging.info("service is stop....")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.run = False


if __name__ == '__main__':
    if len(sys.argv) == 1:
        try:
            evtsrc_dll = os.path.abspath(servicemanager.__file__)
            servicemanager.PrepareToHostSingle(ETLPythonService)
            servicemanager.Initialize('PythonService_Sync', evtsrc_dll)
            servicemanager.StartServiceCtrlDispatcher()
        except win32service.error as details:
            if details[0] == winerror.ERROR_FAILED_SERVICE_CONTROLLER_CONNECT:
                win32serviceutil.usage()
    else:
        win32serviceutil.HandleCommandLine(ETLPythonService)