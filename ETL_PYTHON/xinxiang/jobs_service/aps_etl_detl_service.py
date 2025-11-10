import socket
import time
from datetime import datetime

import pandas as pd
from xinxiang import config
from xinxiang.util import my_oracle, my_runner
import logging
from datetime import timedelta
import subprocess
import win32com.client


g_ETL_NAME = "APS_ETL_DETL_SERVICE"
g_ETL_START_TIME = ""
g_TABLE_DETL_SERVICE = "APS_ETL_DETL_SERVICE"
g_HOSTNAME = ""
g_CHECK_TIMES_IN_MINUTE = 2
g_SLEEP_SECONDS = 25
g_BackUp_SLEEP_SECONDS = 10
g_SECONDS_DONW_SPEC = 180
g_process_start_time = None
g_ETL_START_TIME = datetime.now().strftime('%Y%m%d %H%M%S')
g_process_start_time = datetime.now()
my_timedelta = timedelta(seconds=20)
g_process_start_time = g_process_start_time - my_timedelta

# Get host name
g_HOSTNAME = socket.gethostname()

# 主备切换执行的bat路径
bat_etl_file_path = "D:\\XinXiang\\ETL_PYTHON\\runner\\99_prod_temp_install_windows.bat"
bat_etl_dele_file_path = "D:\\XinXiang\\ETL_PYTHON\\runner\\99_prod_temp_uninstall_windows_task.bat"
bat_sync_file_path = "D:\\XinXiang\\SYNC_PYTHON\\runner\\00_install_task_in_windows.bat"
bat_sync_dele_file_path = "D:\\XinXiang\\SYNC_PYTHON\\runner\\00_delete_task_in_windows.bat"
backup_delete_file_path =[bat_etl_dele_file_path,bat_sync_dele_file_path]
main_create_file_path = [bat_etl_file_path, bat_sync_file_path]

# 使用ETL里面的oracle connection，取消service里自定义的oracle connection
# def connect():
#     try:
#         # Open connection
#         dsn_tns = cx_Oracle.makedsn('192.168.253.135', '1521', sid='ORCLCDB')
#         conn = cx_Oracle.connect(user='capasim', password='pwdcapasim', dsn=dsn_tns)
#         return conn
#
#     except Exception as e:
#         print(e)
#         return None

def get_number_of_scheduled_tasks():
    scheduler = win32com.client.Dispatch('Schedule.Service')
    scheduler.Connect()

    # 获取根文件夹
    root_folder = scheduler.GetFolder('\\')
    #遍历所有任务，并计数
    def count_task(folder):
        count = 0
        for task in folder.GetTasks(0):
            count += 1
        return count
    #获取任务总数
    total_tasks = count_task(root_folder)

    return total_tasks

def executePlanJob(oracle_con):
    cursor = None
    try:
        # 优先看当前IS_CHANGE_PLAN_JOB 是否为1.只有1才需要执行bat文档
        if not my_runner.judge_server_change_plan_job(oracle_con):
            return
        # 判断当前的Server是主是备
        # 主则新建window执行计划，备则删除window执行计划
        if my_runner.judge_main_server(oracle_con):
            for item in main_create_file_path:
                executePlanBatJob(item)
        else:
            # 先看下目前任务的数量，如果已经没有了则不需要执行
            task_count = get_number_of_scheduled_tasks()
            print(task_count)
            if task_count < 50:
                logging.info("已经没有可删的JOB，不需要重新执行删除任务的动作")
            else:
                for itemBack in backup_delete_file_path:
                    executePlanBatJob(itemBack)
        # 把执行完成的动作做标识
        sql_cmd = f"""UPDATE {g_TABLE_DETL_SERVICE} 
                       SET IS_CHANGE_PLAN_JOB ='0'
                     WHERE SRV_NAME = '{g_HOSTNAME}' 
                     """
        cursor = oracle_con.cursor()
        cursor.execute(sql_cmd)
        oracle_con.commit()
        cursor.close()
    except Exception as e:
        print(e)
        logging.error(e)
        raise

# 执行bat文档，新建window执行计划或者删除window执行计划
def executePlanBatJob(bat_file_path):
    try:
        # 使用subprocess来执行bat文件
        result = subprocess.run(bat_file_path, check=True, text=True, capture_output=True)

        # 如果bat文件执行成功，这里会打印出标准输出
        print("Batch file executed successfully:")
        print(result.stdout)

    except FileNotFoundError:
        # 如果bat文件不存在，会抛出FileNotFoundError
        print(f"Error: The batch file {bat_file_path} was not found.")
        logging.error(f"Error: The batch file {bat_file_path} was not found.")
        raise

    except PermissionError:
        # 如果没有执行bat文件的权限，会抛出PermissionError
        print(f"Error: Permission denied to execute the batch file {bat_file_path}.")
        logging.error(f"Error: Permission denied to execute the batch file {bat_file_path}.")
        raise

    except subprocess.CalledProcessError as e:
        # 如果bat文件执行返回非零退出码，会抛出CalledProcessError
        print(f"Error: The batch file {bat_file_path} failed with error code {e.returncode}.")
        logging.error(f"Error: The batch file {bat_file_path} failed with error code {e.returncode}.")
        print("Output from the batch file:")
        print(e.output)
        raise

    except Exception as e:
        # 处理其他可能的异常
        print(f"An unexpected error occurred: {e}")
        logging.error(f"An unexpected error occurred: {e}")
        raise

def sendAlarm(oracle_con, alarmMsg):
    global g_ETL_NAME, g_ETL_START_TIME

    cursor = None
    try:
        DETL_END_TIME = datetime.now().strftime('%Y%m%d %H%M%S')

        sql_cmd = " INSERT INTO ALARM_SEND_LOG(CODE, MODULE, LEVEL_ID, MSG, SEND_ALARM, DISPATCH_TIME, START_TIME, END_TIME, JOB, JOB_MSG) VALUES ('XETL00','XX_ETL','L2','ETL_SERVER异常: '||'%s', 1, TO_DATE('%s','YYYYMMDD HH24MISS'), TO_DATE('%s','YYYYMMDD HH24MISS'), TO_DATE('%s','YYYYMMDD HH24MISS'), '%s', '%s') " % (
        alarmMsg,DETL_END_TIME, g_ETL_START_TIME, DETL_END_TIME, g_ETL_NAME, alarmMsg)
        cursor = oracle_con.cursor()
        cursor.execute(sql_cmd)
        oracle_con.commit()
        cursor.close()
    except Exception as e:
        print(e)
        logging.error(e)
        raise


def updateSelfStatus(oracle_con):
    cursor = None
    try:
        # dt = datetime.fromtimestamp(int(str(datetime.timestamp(datetime.now())).split('.')[0]))
        # print(str(datetime.timestamp(datetime.now())).split('.')[0])
        sql_cmd = f"""
                    UPDATE {g_TABLE_DETL_SERVICE}
                     SET SRV_STATUS = 'RUN', LAST_ONLINE_TIME = '{str(datetime.now()).split('.')[0] }'
                   WHERE SRV_NAME = '{g_HOSTNAME}'
                   """
        cursor = oracle_con.cursor()
        cursor.execute(sql_cmd)
        oracle_con.commit()
        cursor.close()
        logging.info(f"INSER INTO {g_TABLE_DETL_SERVICE} Suceess!!!:{str(datetime.now()).split('.')[0]} SRV_NAME = {g_HOSTNAME}")
    except Exception as e:
        print(e)
        logging.error(f"INSER INTO {g_TABLE_DETL_SERVICE} FAIL!!!:{str(datetime.now()).split('.')[0] }")
        logging.error(e)
        sendAlarm(oracle_con, str(e))


def checkOtherServerState(oracle_con):
    cursor = None
    try:
        sql_cmd =f"""SELECT SRV_NAME, IS_MAIN_DISPATCH, LAST_ONLINE_TIME 
                     FROM {g_TABLE_DETL_SERVICE} 
                     WHERE SRV_NAME <> '{g_HOSTNAME}'
                        AND SRV_STATUS = 'RUN' 
                  """

        df_OtherServer = pd.read_sql(sql_cmd, oracle_con)
        # print(df_OtherServer)
        for i, srv in df_OtherServer.iterrows():
            now_struct = datetime.now()
            srv_online_time_struct= None
            if srv['LAST_ONLINE_TIME'] is None:
                srv_online_time_struct= g_process_start_time
            else:
                srv_online_time_struct = datetime.strptime(srv['LAST_ONLINE_TIME'], "%Y-%m-%d %H:%M:%S")
            seconds = (now_struct - srv_online_time_struct).total_seconds()
            # print(seconds, now_struct, srv_online_time_struct)

            # >= 50 seconds, means it is probably down
            if seconds >= g_SECONDS_DONW_SPEC:
                logging.info(f"ServerName Not Is: {g_HOSTNAME} Last Time: {srv_online_time_struct} Now Time: {now_struct} Seconds:{seconds}")
                sql_cmd = f"""UPDATE {g_TABLE_DETL_SERVICE} 
                             SET IS_MAIN_DISPATCH = 'N', SRV_STATUS = 'DOWN', IS_CHANGE_PLAN_JOB ='1'
                           WHERE SRV_NAME = '{srv['SRV_NAME']}' 
                           """
                cursor = oracle_con.cursor()
                cursor.execute(sql_cmd)
                alarm_message = srv['SRV_NAME'] + " is Down!!"

                if srv['IS_MAIN_DISPATCH'] == 'Y':
                    sql_cmd = f"""UPDATE {g_TABLE_DETL_SERVICE}
                                 SET IS_MAIN_DISPATCH = 'Y', IS_CHANGE_PLAN_JOB ='1' 
                               WHERE SRV_NAME = '{g_HOSTNAME}'
                               """
                    cursor = oracle_con.cursor()
                    cursor.execute(sql_cmd)
                    alarm_message = alarm_message + " " + g_HOSTNAME + " switch to MAIN."

                oracle_con.commit()
                cursor.close()

                sendAlarm(oracle_con, alarm_message)

    except Exception as e:
        print(e)
        logging.error(e)
        sendAlarm(oracle_con, str(e))

def execute():
    oracle_con = None
    try:
        for i in range(g_CHECK_TIMES_IN_MINUTE):
            if config.g_debug_mode:
                oracle_con = my_oracle.oracle_get_connection_local()
            else:
                oracle_con = my_oracle.oracle_get_connection()

            # 如果是备库，则延迟10s在更新，防止出现锁表的情况
            if not my_runner.judge_main_server(oracle_con):
                time.sleep(g_BackUp_SLEEP_SECONDS)
            updateSelfStatus(oracle_con)
            checkOtherServerState(oracle_con)
            executePlanJob(oracle_con)

            oracle_con.close()

            time.sleep(g_SLEEP_SECONDS)

    except Exception as e:
        logging.error(e)
        print(e)

if __name__ == '__main__':
    execute()
