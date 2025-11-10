import logging
import os.path
from datetime import datetime, timedelta

from xinxiang import config
from xinxiang.util import my_oracle, cons_error_code, my_cmder, my_date, my_runner


def execute():

    """
    部署在主服务器上，将主服务器上的Flow产出拷贝到备用机目录
    """

    oracle_conn = None

    try:
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()
        # 若不是主服务器，则退出etl_flow备份逻辑
        if not my_runner.judge_main_server(oracle_conn):
            if oracle_conn:
                logging.info("当前服务器不是主服务器,系统退出etl_flow备份逻辑")
            return
        if not my_runner.judge_backup_server_status(oracle_conn, config.other_ip):
            if oracle_conn:
                logging.info("当前备援服务器已经DOWN,系统退出备份逻辑")
            return
        else:  # 只有主服务器要向备用服务器中拷贝
            logging.info("当前服务器是主服务器")
            table_name = "APS_ETL_FLOW"
            backup_folder = "\\\{other_ip}\workspace\ETLOutput".format(other_ip=config.other_ip)

            file_full_path = my_oracle.get_last_create_file(oracle_conn, table_name)
            if file_full_path is not None and file_full_path != '':
                file_name = file_full_path.split('\\')[-1]
                create_date_str = file_name.replace(".db", "").split("_")[-1]
                _create_date = datetime.strptime(create_date_str, '%Y%m%d%H%M%S')
                print(_create_date)
                logging.info("正在向备用服务器中拷贝")
                target_full_path = os.path.join(backup_folder, table_name)
                copy_cmder = "xcopy {source} {target} /Y".format(source=file_full_path, target=target_full_path)
                logging.info(copy_cmder)
                my_cmder.exec(copy_cmder)
                # 写版本号
                my_oracle.override_HandlingVerControl(oracle_conn, my_oracle.UUID(), table_name, file_full_path, my_date.date_time_second_short_str())
            else:
                e = Exception("No Flow Outout In Recent 30 Mins Or Service Name is Error")
                my_oracle.SaveAlarmLogData(conn=oracle_conn, ETLProcName="ETLProcCopyFLow", Exception=e, file_name=None,
                                           alarm_code=cons_error_code.APS_ETL_FLOW_CODE_XX_ETL)
    except Exception as copyException:
        logging.error("COPY_FLOW_FOR_BACKUP 异常：" + str(copyException))
        my_oracle.SaveAlarmLogData(conn=oracle_conn, ETLProcName="ETLProcCopyFLow", Exception=copyException, file_name=None,
                                   alarm_code=cons_error_code.APS_COPY_BACKUP_CODE_XX_ETL)
    finally:
        if oracle_conn:
            oracle_conn.close()


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()