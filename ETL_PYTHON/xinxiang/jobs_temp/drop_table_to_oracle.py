from xinxiang import config
import logging
from xinxiang.util import my_oracle, my_runner,my_date,cons_error_code


def execute():
    conn = None
    current_time = my_date.date_time_second_str()
    tmpName = "DROP_TABLE_TO_ORACLE"
    try:
        if config.g_debug_mode:
            conn = my_oracle.oracle_get_connection_local()
        else:
            conn = my_oracle.oracle_get_connection()

        if my_runner.judge_main_server(conn):
            # 开始日志
            my_oracle.StartCleanUpAndLog(conn, "Drop_Table_To_Oracle", current_time)

            # 按时TRUNCATE TABLE数据
            my_oracle.DropOldPartition(conn, "APS_ETL_OPTG_OUTPUT_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPTG_RELEASE_STATE_HIST", 2)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPTG_RETICLE_OUTPUT_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_OUTPUT_DF_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_OUTPUT_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_RELEASE_STATE_DF_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_RELEASE_STATE_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_RETICLE_OUTPUT_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_SGS_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPTG_OUTPUT_RTD_HIST", 3)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_RELEASE_STATE_ETCH_HIST", 7)
            my_oracle.DropOldPartition(conn, "APS_ETL_OPT_OUTPUT_ETCH_HIST", 7)

            # 写完成日志
            my_oracle.EndCleanUpAndLog(conn, "Drop_Table_To_Oracle", current_time)
    except Exception as e:
        print(e)
        logging.error(e)
        msg = tmpName + "异常： " + str(e)
        # 写警告日志
        my_oracle.sendTempAlarm(conn, msg, cons_error_code.APS_TMP_CODE_XX_ETL)
        raise

    finally:
        conn.commit()
        conn.close()

if __name__ == '__main__':
    execute()