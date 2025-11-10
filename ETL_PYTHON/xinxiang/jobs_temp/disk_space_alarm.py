import psutil
import logging
import socket

from datetime import datetime
from xinxiang import config
from xinxiang.util import my_oracle,my_date,cons_error_code

def execute_monitor():
    monitor_disk_space()

def monitor_disk_space():
    # 需要监控的路径
    path = "R:\\"
    oracle_conn = None
    alarmMsg = None
    current_time = my_date.date_time_second_str()
    tmpName = 'DISK_SPACE_ALARM'
    try:
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()
        else:
            oracle_conn = my_oracle.oracle_get_connection()
        # 开始日志
        my_oracle.StartCleanUpAndLog(oracle_conn, tmpName, current_time)
        dist_usage = psutil.disk_usage(path)
        print(dist_usage.total)
        print(dist_usage.free)
        print(dist_usage.used)
        print(dist_usage.percent)

        host_name = socket.gethostname()
        # 如果使用率达到70%，则需要alarm出来进行告知
        if dist_usage.percent > 70:
            alarmMsg = "磁盘异常: " + host_name + ": R 盘的使用率已达到" + str(dist_usage.percent) + "%，请确认！！"
            # 写警告日志
            my_oracle.sendTempAlarm(oracle_conn, alarmMsg, cons_error_code.APS_TMP_CODE_XX_ETL)
        # 写完成日志
        my_oracle.EndCleanUpAndLog(oracle_conn, tmpName, current_time)
    except Exception as e:
        print(e)
        logging.error(e)
        msg = tmpName + "异常： " + str(e)
        # 写警告日志
        my_oracle.sendTempAlarm(oracle_conn, msg, cons_error_code.APS_TMP_CODE_XX_ETL)
        raise
    finally:
        oracle_conn.commit()
        oracle_conn.close()

# 监控R盘 空间使用情况
if __name__ == '__main__':
    execute_monitor()