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
    current_time = my_date.date_time_second_str()
    ETL_Proc_Name = "COPY_FHOPEHS_FOR_TEST"
    try:
        # 每小时6min执行
        oracle_conn = my_oracle.oracle_get_connection()
        if config.g_debug_mode:
            oracle_conn = my_oracle.oracle_get_connection_local()
        # 若不是主服务器，则退出备份逻辑
        if not my_runner.judge_main_server(oracle_conn):
            if oracle_conn:
                logging.info("当前服务器不是主服务器,系统退出备份逻辑")
            return

        else:  # 只有主服务器要向备用服务器中拷贝
            # 开始日志
            my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
            logging.info("当前服务器是主服务器")
            table_name = "APS_MID_FHOPEHS_VIEW"
            backup_folder = "\\\{test_ip}\workspace\SrcData".format(test_ip=config.test_ip)

            file_full_path = my_oracle.get_last_create_file(oracle_conn, table_name)
            # 该copy到test服务，因此需要连测试数据库
            oracle_conn = my_oracle.oracle_get_connection_local()
            if file_full_path is not None and file_full_path != '':
                file_name = file_full_path.split('\\')[-1]

                target_full_path = os.path.join(backup_folder, table_name)
                if not os.path.exists(target_full_path):
                    os.makedirs(target_full_path)
                copy_cmder = "xcopy {source} {target} /Y".format(source=file_full_path, target=target_full_path)
                logging.info(copy_cmder)
                my_cmder.exec(copy_cmder)
                # 写版本号
                my_oracle.TEST_HandlingVerControl(oracle_conn, my_oracle.UUID(), table_name, file_full_path, my_date.date_time_second_short_str())
            else:
                e = Exception("FILE IS NOT FOUND")
                logging.info(ETL_Proc_Name + str(e))
                # my_oracle.SaveAlarmLogData(conn=oracle_conn, ETLProcName="ETLProcCopyFhopehs", Exception=e, file_name=None,
                #                            alarm_code=cons_error_code.APS_COPY_BACKUP_CODE_XX_ETL)
            # 写完成日志
            my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
    except Exception as copyException:
        logging.info("COPY_FHOPEHS_FOR_TEST 异常：" + str(copyException))
        # my_oracle.SaveAlarmLogData(conn=oracle_conn, ETLProcName="ETLProcCopyFhopehs", Exception=copyException, file_name=None,
        #                            alarm_code=cons_error_code.APS_COPY_BACKUP_CODE_XX_ETL)
    finally:
        if oracle_conn:
            oracle_conn.close()


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()