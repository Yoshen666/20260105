import logging
import os
import socket
import uuid
from datetime import datetime

from xinxiang import config
from xinxiang.util import my_file, my_oracle, oracle_to_duck_his, cons_error_code, my_date, my_runner

sql_template = """INSERT INTO APS_ETL_VER_CONTROL_DUCK(ID,TABLE_NAME,UPDATE_TIME,UPDATE_USER,START_TIME,SERVER_NAME)
VALUES('{}','{}','{}','{}','{}','{}')"""

# 有些最终table很大，如果保留7天可能导致磁盘占用率超过80%，这些table单独拎出来，只保留2天数据
def delete_backup_files_by_output():
    '''
    按天执行的JOB
    需要修改的地方:表名称， save_days：保存几天
    '''
    ETL_Proc_Name = 'manager_jobs.delete_backup_files_by_output'
    now = datetime.now()
    oracle_conn = None
    output_path = []
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()
        guid = str(uuid.uuid4()).replace("-", "")
        if not my_runner.judge_main_server(oracle_conn):
            return
        # 判断如果备份服务器DOWN掉了则不需要对备服务做动作
        if not my_runner.judge_backup_server_status(oracle_conn, config.other_ip):
            output_path = [config.g_mem_etl_output_path]
        else:
            output_path = [config.g_mem_etl_output_path, config.g_mem_backup_etl_output_path.format(config.other_ip)]
        # 把清除历史backup资料也放在这里边执行
        dele_list = ['APS_ETL_RLS','APS_ETL_WIP','APS_ETL_PIRUN','APS_ETL_MONITOR']
        for dele_table_name in dele_list:
            for path_data in output_path:
                my_file.delete_backup_files(path_data+'\\'+ dele_table_name, current_datetime=now, save_days=2)

        # 插入虚拟版本信息
        sql_text = sql_template.format(guid, "APS_ETL_PURGE_2D", my_date.date_time_second_short_str(), "CIMP",
                                       my_date.date_time_second_short_str(), socket.gethostname())
        insert_version(oracle_conn, sql_text)

    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 写警告日志
        my_oracle.SaveAlarmLogDataForTmp(oracle_conn, ETL_Proc_Name,
                                   cons_error_code.APS_ETL_SYNC_TMP, str(e))  # TODO 改成你自己的CODE
    finally:
        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()

def delete_backup_files_by_day():
    '''
    按天执行的JOB
    需要修改的地方:表名称， save_days：保存几天
    '''
    ETL_Proc_Name = 'manager_jobs.delete_backup_files_by_day'
    now = datetime.now()
    oracle_conn = None
    g_mem_sync_result_path=[]
    g_mem_etl_output_path=[]
    g_mem_oracle_dat_input_path=[]
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()

        if not my_runner.judge_main_server(oracle_conn):
            return
        guid = str(uuid.uuid4()).replace("-", "")
        # 判断如果备份服务器DOWN掉了则不需要对备服务做动作
        if not my_runner.judge_backup_server_status(oracle_conn, config.other_ip):
            g_mem_sync_result_path = [config.g_mem_sync_result_path]
            g_mem_etl_output_path = [config.g_mem_etl_output_path]
            g_mem_oracle_dat_input_path = [config.g_mem_oracle_dat_input_path]
        else:
            g_mem_sync_result_path = [config.g_mem_sync_result_path, config.g_mem_backup_sync_result_path.format(config.other_ip)]
            g_mem_etl_output_path = [config.g_mem_etl_output_path, config.g_mem_backup_etl_output_path.format(config.other_ip)]
            g_mem_oracle_dat_input_path = [config.g_mem_oracle_dat_input_path, config.g_mem_backup_oracle_dat_input_path.format(config.other_ip)]

        for tmp_path in g_mem_sync_result_path:
            my_file.delete_backup_files(tmp_path, current_datetime=now, save_days=7)


        for tmp_path in g_mem_etl_output_path:
            my_file.delete_backup_files(tmp_path, current_datetime=now, save_days=7)


        for tmp_path in g_mem_oracle_dat_input_path:
            my_file.delete_backup_files(tmp_path, current_datetime=now, save_days=7)

        # 插入虚拟版本信息
        sql_text = sql_template.format(guid, "APS_ETL_PURGE_7D", my_date.date_time_second_short_str(), "CIMP",
                                       my_date.date_time_second_short_str(), socket.gethostname())
        insert_version(oracle_conn, sql_text)

    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 写警告日志
        my_oracle.SaveAlarmLogDataForTmp(oracle_conn, ETL_Proc_Name,
                                   cons_error_code.APS_ETL_SYNC_TMP, str(e))  # TODO 改成你自己的CODE
    finally:
        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()


def delete_temp_file():
    ETL_Proc_Name='manager_jobs.delete_temp_file'
    oracle_conn = None
    g_mem_sync_result_path = []
    g_mem_etl_output_path = []
    try:
        now = datetime.now()
        guid = str(uuid.uuid4()).replace("-", "")
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()

        # 删除R盘资料
        my_file.delete_temp_files(config.g_mem_speed_etl_output_path)

        if not my_runner.judge_main_server(oracle_conn):
            return
        if not my_runner.judge_backup_server_status(oracle_conn, config.other_ip):
            g_mem_sync_result_path = [config.g_mem_sync_result_path]
            g_mem_etl_output_path = [config.g_mem_etl_output_path]
        else:
            g_mem_sync_result_path = [config.g_mem_sync_result_path, config.g_mem_backup_sync_result_path.format(config.other_ip)]
            g_mem_etl_output_path = [config.g_mem_etl_output_path, config.g_mem_backup_etl_output_path.format(config.other_ip)]

        # 把清除历史backup资料也放在这里边执行
        dele_list = ['APS_MID_FHOPEHS_VIEW', 'APS_MID_FHOPEHS_RETICLE_VIEW',
                     'APS_MID_OCS_JOB_GROUP_PO_STATE_H_VIEW', 'APS_MID_RSPILOT_RUN_HS_VIEW',
                     'APS_TMP_LOTHISTORY_VIEW', 'APS_TMP_MASK_HISTORY_VIEW',
                     'APS_TMP_OCS_MAIN_H_VIEW', 'APS_MID_PH_LOTHISTORY', 'APS_ETL_LOTHISTORY',
                     'APS_ETL_LOT_OP_HIST']
        for dele_table_name in dele_list:
            # 先找出最新版本的资讯
            file_name = my_file.get_last_db_file(conn=oracle_conn, table_name=dele_table_name)
            oracle_to_duck_his.delete_over_three_version_masterandsalve(table_name=dele_table_name, curr_file_name=file_name, g_mem_sync_result_path=g_mem_sync_result_path, g_mem_etl_output_path=g_mem_etl_output_path)

        # 插入虚拟版本信息
        sql_text = sql_template.format(guid, "APS_ETL_PURGE_TEMPFILE", my_date.date_time_second_short_str(), "CIMP",
                                           my_date.date_time_second_short_str(), socket.gethostname())
        insert_version(oracle_conn, sql_text)
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 写警告日志
        my_oracle.SaveAlarmLogDataForTmp(oracle_conn, ETL_Proc_Name,
                                   cons_error_code.APS_ETL_SYNC_TMP, str(e))  # TODO 改成你自己的CODE
    finally:
        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()


def delete_oracle_db_log_by_12_hours():
    ETL_Proc_Name = 'manager_jobs.delete_oracle_db_log_by_12_hours'
    oracle_conn = None
    dbcursor = None
    try:
        guid = str(uuid.uuid4()).replace("-", "")
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()
        if not my_runner.judge_main_server(oracle_conn):
            return
        dbcursor = oracle_conn.cursor()
        # 删除表数据
        sql = """
            delete from ETL_EXEC_LOG where end_time < SYSDATE - 14
            """
        dbcursor.execute(sql)
        # 删除表数据
        sql = """
           delete from ETL_EXEC_LOG_HIST where end_time < SYSDATE - 180
           """
        dbcursor.execute(sql)
        sql = """
            delete from APS_ETL_VER_CONTROL_DUCK  where update_time < TO_CHAR(SYSDATE - 30,'YYYYMMDDHH24MISS') 
            """
        dbcursor.execute(sql)
        sql = """
            delete from APS_ETL_MONITOR_DUCK  where update_time < TO_CHAR(SYSDATE - 30,'YYYYMMDDHH24MISS') 
            """
        dbcursor.execute(sql)
        sql = """
            delete from APS_ETL_METHOD_LOG_DUCK  where end_time < SYSDATE - 7
            """
        dbcursor.execute(sql)
        sql = """
            delete from APS_ETL_SYNC_CONTROL_DUCK  where update_time < TO_CHAR(SYSDATE - 5/24,'YYYYMMDDHH24MISS') 
            """
        dbcursor.execute(sql)

        # 插入虚拟版本信息
        sql_text = sql_template.format(guid, "APS_ETL_PURGE_12H", my_date.date_time_second_short_str(), "CIMP",
                                       my_date.date_time_second_short_str(), socket.gethostname())
        insert_version(oracle_conn, sql_text)

    except Exception as e:
        logging.error(e)
        # 写警告日志
        my_oracle.SaveAlarmLogDataForTmp(oracle_conn, ETL_Proc_Name,
                                         cons_error_code.APS_ETL_SYNC_TMP, str(e))  # TODO 改成你自己的CODE
        raise e
    finally:
        if dbcursor is not None:
            dbcursor.close()

        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()


def delete_backup_files_by_6_hours():
    '''
    按6小时执行的JOB
    需要修改的地方:表名称， save_days：保存几天
    '''
    now = datetime.now()
    print("TODO")


def insert_version(conn, sql_text):
    dbcursor = None
    try:
        dbcursor = conn.cursor()
        dbcursor.execute(sql_text)
    except Exception as e:
        e.print_exc()
        logging.error(e.format_exc())
        raise e
    finally:
        conn.commit()
        if dbcursor:
            dbcursor.close()


if __name__ == '__main__':

    # delete_backup_files_by_day()
    # delete_oracle_db_log_by_12_hours()
    delete_temp_file()
    # delete_backup_files_by_output()