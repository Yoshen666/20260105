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
                    SELECT DISTINCT W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE 
                    FROM READ_PARQUET("{temp_process_file_path}RESULT.parquet") W
                 )                                                                                                                                                                             
                 SELECT  W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE                                                                                                   
                      ,CASE WHEN CONCAT_WS('',MAX(ERI1.EQP_ID),MAX(ERI2.EQP_ID),MAX(ERI4.EQP_ID),MAX(ERI5.EQP_ID),MAX(ERI6.EQP_ID),MAX(ERI7.EQP_ID))<>''  THEN 'EqpRcpInhibit;' END AS SET_STATUS2                                                                                                                                                                                                                                                              
                 FROM GROUP_APC_PILOT W                                                                                                                                                       
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT1.parquet") ERI1 ON  ERI1.PRODUCT_ID = W.PROD_ID 
                                                             AND ERI1.EQP_ID = W.EQP_ID 
                                                             AND INSTR(W.M_RECIPE,ERI1.RECIPE_ID)>0                                             
                                                             --AND position(ERI1.RECIPE_ID in W.M_RECIPE)>0                                                                                                                                  
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT2.parquet") ERI2 ON ERI2.EQP_ID = W.EQP_ID   
                                                             AND INSTR(W.M_RECIPE,ERI2.RECIPE_ID)>0                                                 
                                                            -- AND position(ERI2.RECIPE_ID in W.M_RECIPE)>0                                      
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT4.parquet") ERI4 ON ERI4.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                        
                                                             AND ERI4.EQP_ID = W.EQP_ID                                                  
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT5.parquet") ERI5 ON ERI5.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                        
                                                             AND position(ERI5.RECIPE_ID in W.M_RECIPE)>0                                      
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT6.parquet") ERI6 ON ERI6.EQP_ID = W.EQP_ID  
                                                             AND ERI6.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                                   
                                                             AND position(ERI6.RECIPE_ID in W.M_RECIPE)>0                                                                          
                 LEFT JOIN READ_PARQUET("{temp_process_file_path}INHIBIT7.parquet") ERI7 ON  ERI7.PRODUCT_ID = (case when ERI7.PRODUCT_ID = '*' then '*' else W.PROD_ID end)                  
                                                             AND ERI7.EQP_ID = (case when ERI7.EQP_ID = '*' then '*' else W.EQP_ID end)                    
                                                             AND ERI7.SUB_LOT_TYPE =(case when ERI7.SUB_LOT_TYPE = '*' then '*' else W.SUB_LOT_TYPE end)   
                                                             AND position(ERI7.RECIPE_ID in (case when ERI7.RECIPE_ID = '*' then '*' else W.M_RECIPE end))>0 
                 GROUP BY W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE 
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
