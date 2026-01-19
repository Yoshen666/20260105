import logging
import os
import sys
import time
import traceback

import duckdb

# sys.path.insert(0, sys.path[0])
# sys.path.insert(0, os.path.join(sys.path[0], ".."))
# sys.path.insert(0, os.path.join(sys.path[0], "..", '..'))
path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(path, '..'))
sys.path.insert(0, os.path.join(path, '..', '..'))

from xinxiang import config
from xinxiang.util import my_oracle, cons_error_code, my_date, my_duck, my_log


def getRcpInhibit(temp_process_file_path=None, current_time=''):
    logging.info("----------------已进入ETL_RLS_RUB方法里边------------------")

    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_RLS_5M"
    # current_time = my_date.date_time_second_str()
    duck_db_temp = None
    oracle_conn = None
    temp_process_file_rpc = None
    try:
        temp_process_file_rpc = temp_process_file_path + 'rcp1.parquet'
        # logging.info("----------------已进入ETL_RLS_RUB方法里边Conn已连接，开始休眠------------------")
        # time.sleep(15)
        # logging.info("----------------已进入ETL_RLS_RUB方法进程已执行------------------")
        duck_db_temp = duckdb.connect()
        # logging.info("----------------已进入ETL_RLS_RUB方法执行完create APS_TMP_ETL_RLS_APC_PILOT_RCP1------------------")
        start_time = time.time()
        duck_db_temp.execute("""
               copy (
                 WITH  GROUP_APC_PILOT AS (
                    SELECT DISTINCT W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE,W.LOT_ID,W.ROUTE_ID,W.OPE_NO,W.CHAMBER_ID
                    FROM READ_PARQUET("{temp_process_file_path}RESULT.parquet") W
                 )
                 SELECT  W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE,W.LOT_ID,W.ROUTE_ID,W.OPE_NO,W.CHAMBER_ID
                      ,CASE WHEN CONCAT_WS('',MAX(ERI1.EQP_ID),MAX(ERI2.EQP_ID),MAX(ERI3.EQP_ID),MAX(ERI4.EQP_ID),MAX(ERI5.EQP_ID),MAX(ERI6.EQP_ID),MAX(ERI7.EQP_ID),MAX(ERI8.EQP_ID),MAX(ERI9.EQP_ID),MAX(ERI10.EQP_ID),MAX(ERI11.EQP_ID),MAX(ERI12.EQP_ID))<>''  THEN 'EqpRcpInhibit;' END AS SET_STATUS2
                 FROM GROUP_APC_PILOT W

                 -- INHIBIT1: Product
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT1.parquet") ERI1 ON ERI1.PRODUCT_ID = W.PROD_ID
                                                             AND ERI1.EQP_ID = W.EQP_ID
                                                             AND INSTR(W.M_RECIPE,ERI1.RECIPE_ID)>0
                                                             AND (ERI1.EXCLUDE_LOT_ID = '*' OR ERI1.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT2: Route+OP+Product+EQP
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT2.parquet") ERI2 ON ERI2.ROUTE_ID = W.ROUTE_ID
                                                             AND ERI2.OPE_NO = W.OPE_NO
                                                             AND ERI2.PRODUCT_ID = W.PROD_ID
                                                             AND ERI2.EQP_ID = W.EQP_ID
                                                             AND (ERI2.EXCLUDE_LOT_ID = '*' OR ERI2.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT3: Route+OP+Product+EQP+Chamber
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT3.parquet") ERI3 ON ERI3.ROUTE_ID = W.ROUTE_ID
                                                             AND ERI3.OPE_NO = W.OPE_NO
                                                             AND ERI3.PRODUCT_ID = W.PROD_ID
                                                             AND ERI3.EQP_ID = W.EQP_ID
                                                             AND ERI3.CHAMBER_ID = W.CHAMBER_ID
                                                             AND (ERI3.EXCLUDE_LOT_ID = '*' OR ERI3.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT4: Route+OP+Product+EQP+Recipe
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT4.parquet") ERI4 ON ERI4.ROUTE_ID = W.ROUTE_ID
                                                             AND ERI4.OPE_NO = W.OPE_NO
                                                             AND ERI4.PRODUCT_ID = W.PROD_ID
                                                             AND ERI4.EQP_ID = W.EQP_ID
                                                             AND INSTR(W.M_RECIPE,ERI4.RECIPE_ID)>0
                                                             AND (ERI4.EXCLUDE_LOT_ID = '*' OR ERI4.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT5: Recipe
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT5.parquet") ERI5 ON INSTR(W.M_RECIPE,ERI5.RECIPE_ID)>0
                                                             AND (ERI5.EXCLUDE_LOT_ID = '*' OR ERI5.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT6: Reticle (暫時不處理，保留為未來擴展)
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT6.parquet") ERI6 ON 1=0

                 -- INHIBIT7: EQP
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT7.parquet") ERI7 ON ERI7.EQP_ID = W.EQP_ID
                                                             AND (ERI7.EXCLUDE_LOT_ID = '*' OR ERI7.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT8: EQP+Recipe
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT8.parquet") ERI8 ON ERI8.EQP_ID = W.EQP_ID
                                                             AND INSTR(W.M_RECIPE,ERI8.RECIPE_ID)>0
                                                             AND (ERI8.EXCLUDE_LOT_ID = '*' OR ERI8.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT9: EQP+Chamber
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT9.parquet") ERI9 ON ERI9.EQP_ID = W.EQP_ID
                                                             AND ERI9.CHAMBER_ID = W.CHAMBER_ID
                                                             AND (ERI9.EXCLUDE_LOT_ID = '*' OR ERI9.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT10: EQP+Chamber+Product
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT10.parquet") ERI10 ON ERI10.EQP_ID = W.EQP_ID
                                                             AND ERI10.CHAMBER_ID = W.CHAMBER_ID
                                                             AND ERI10.PRODUCT_ID = W.PROD_ID
                                                             AND (ERI10.EXCLUDE_LOT_ID = '*' OR ERI10.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT11: EQP+Product+Recipe
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT11.parquet") ERI11 ON ERI11.EQP_ID = W.EQP_ID
                                                             AND ERI11.PRODUCT_ID = W.PROD_ID
                                                             AND INSTR(W.M_RECIPE,ERI11.RECIPE_ID)>0
                                                             AND (ERI11.EXCLUDE_LOT_ID = '*' OR ERI11.EXCLUDE_LOT_ID <> W.LOT_ID)

                 -- INHIBIT12: Product+EQP
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT12.parquet") ERI12 ON ERI12.PRODUCT_ID = W.PROD_ID
                                                             AND ERI12.EQP_ID = W.EQP_ID
                                                             AND (ERI12.EXCLUDE_LOT_ID = '*' OR ERI12.EXCLUDE_LOT_ID <> W.LOT_ID)

                 GROUP BY W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE,W.LOT_ID,W.ROUTE_ID,W.OPE_NO,W.CHAMBER_ID
               ) to '{temp_process_file_rpc}' (FORMAT PARQUET)""".format(temp_process_file_path=temp_process_file_path,temp_process_file_rpc=temp_process_file_rpc))
        logging.info("----------------INSERT INTO {tempdb}APS_TMP_ETL_RLS_APC_PILOT_RCP1 语句执行前 ------------------")
        oracle_conn = my_oracle.oracle_get_connection()
        end_time = time.time()
        my_oracle.SaveEtlMethodLog(oracleConn=oracle_conn,
                                   etlJob="APS_ETL_BR.APS_ETL_RLS_5M",
                                   methodName="Insert Into APS_TMP_ETL_RLS_APC_PILOT_RCP1",
                                   startTime=start_time,
                                   endTime=end_time,
                                   etlJobTime=current_time)
    except Exception as eee:
        logging.info("----------------已进入ETL_RLS_RUB方法执行异常------------------")
        logging.info(eee)
        traceback.print_exc()
        logging.exception(traceback.print_exc())
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, eee, "",
                                   cons_error_code.APS_ETL_RLS_CODE_XX_ETL)
        if oracle_conn:
            oracle_conn.close()
    finally:
        if oracle_conn:
            oracle_conn.close()
        duck_db_temp.close()
        logging.info("----------------ETL_RLS_RUB方法已执行结束------------------")


if __name__ == '__main__':
    print("start")
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_ETL_SUB.log"))  # 初期化Logger
    temp_process_file_path = sys.argv[1].split('@')[0]
    current_time = sys.argv[1].split('@')[1]
    getRcpInhibit(temp_process_file_path,current_time)
