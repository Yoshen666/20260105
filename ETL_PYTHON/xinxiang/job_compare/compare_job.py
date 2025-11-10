import os
import difflib
import logging
import uuid
from xinxiang import config
from xinxiang.util import my_runner,my_oracle,cons_error_code,my_date

def execute():
    LOCAL_ETL_PATH = r'D:\XinXiang\ETL_PYTHON'
    BACKUP_ETL_PATH = r'\\' + config.other_ip + '\XinXiang\ETL_PYTHON'
    LOCAL_SYNC_PATH = r'D:\XinXiang\SYNC_PYTHON'
    BACKUP_SYNC_PATH = r'\\' + config.other_ip + '\XinXiang\SYNC_PYTHON'
    ETL_Proc_Name = 'COMPARE_JOB'
    oracle_conn = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        # 只有主服务才需要触发
        if not my_runner.judge_main_server(oracle_conn):
            return
        # 如果备服务DOWN了直接不比
        if not my_runner.judge_backup_server_status(oracle_conn, config.other_ip):
            return
        # 比对主备的ETL_PYTHON
        directory_compare(LOCAL_ETL_PATH, BACKUP_ETL_PATH, ETL_Proc_Name, oracle_conn)
        # 比对主备的SYNC_PYTHON
        directory_compare(LOCAL_SYNC_PATH, BACKUP_SYNC_PATH, ETL_Proc_Name, oracle_conn)
        # 写版本号,为了做监控
        my_oracle.HandlingVerControl(oracle_conn, str(uuid.uuid4()).replace("-", ""), ETL_Proc_Name, '', my_date.date_time_second_short_str())
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        alarmMsg = ETL_Proc_Name + ': ' + str(e)
        print(alarmMsg)
        # 写警告日志
        my_oracle.sendTempAlarm(oracle_conn, alarmMsg, cons_error_code.APS_TMP_CODE_XX_ETL)
        raise e
    finally:
        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()

def file_compare(file1, file2, ETL_Proc_Name):
    diff_output = None
    try:
        with open(file1, 'r', encoding='utf-8') as file_1:
            file_1_text = file_1.readlines()
        with open(file2, 'r', encoding='utf-8') as file_2:
            file_2_text = file_2.readlines()

        diff = difflib.ndiff(file_1_text, file_2_text)
        diff_output = [line for line in diff if line.startswith('+') or line.startswith('-')]
        return diff_output
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        raise e
    return diff_output


def directory_compare(dir1, dir2, ETL_Proc_Name, oracle_conn):
    try:
        for root, dirs, files in os.walk(dir1):
            dirs[:] = [d for d in dirs if d not in "config"]
            for file_name in files:
                for suffix in [".py", ".bat", ".vbs"]:
                    if file_name.lower().endswith(suffix):
                        file_path_1 = os.path.join(root, file_name)
                        file_path_2 = file_path_1.replace(dir1, dir2)
                        if os.path.isfile(file_path_2):
                            diff = file_compare(file_path_1, file_path_2, ETL_Proc_Name)
                            if diff:
                                alarmMsg = ETL_Proc_Name + ': ' + (file_path_1) +" and "+ (file_path_2) +  " are different. please check!!"
                                print(alarmMsg)
                                # 写警告日志
                                my_oracle.sendTempAlarm(oracle_conn, alarmMsg, cons_error_code.APS_TMP_CODE_XX_ETL)
                        else:
                            alarmMsg = ETL_Proc_Name + ': ' + (file_path_2) + " does not exist. please check!!"
                            print(alarmMsg)
                            # 写警告日志
                            my_oracle.sendTempAlarm(oracle_conn, alarmMsg, cons_error_code.APS_TMP_CODE_XX_ETL)
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        raise e


if __name__ == '__main__':
    execute()

