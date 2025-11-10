import logging
import sys
import time
import traceback

import duckdb

from xinxiang.util import my_duck, my_oracle, cons_error_code, cons
import pandas as pd


def getRcpInhibit(temp_process_db_file=None, ETL_Proc_Name=""):

        start_time = time.time()
        duck_db_temp = duckdb.connect(temp_process_db_file)
        sql = """
                -- INSERT INTO {tempdb}APS_TMP_ETL_RLS_APC_PILOT_RCP1 (                                                                                                                                  
                --    PROD_ID,M_RECIPE,EQP_ID,SUB_LOT_TYPE,SET_STATUS2
                -- )    
                WITH  GROUP_APC_PILOT AS (
                    SELECT DISTINCT W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE 
                    FROM {tempdb}APS_TMP_ETL_RLS_EQPRCP_RESULT W
                )                                                                                                                                                                             
                 SELECT  W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE                                                                                                   
                      ,CASE WHEN CONCAT_WS('',MAX(ERI1.EQP_ID),MAX(ERI2.EQP_ID),MAX(ERI4.EQP_ID),MAX(ERI5.EQP_ID),MAX(ERI6.EQP_ID),MAX(ERI7.EQP_ID))<>''  THEN 'EqpRcpInhibit;' END AS SET_STATUS2                                                                                                                                                                                                                                                              
                 FROM GROUP_APC_PILOT W                                                                                                                                                       
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT1 ERI1 ON  ERI1.PRODUCT_ID = W.PROD_ID 
                                                             AND ERI1.EQP_ID = W.EQP_ID 
                                                             AND INSTR(W.M_RECIPE,ERI1.RECIPE_ID)>0                                             
                                                             --AND position(ERI1.RECIPE_ID in W.M_RECIPE)>0                                                                                                                                  
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT2 ERI2 ON ERI2.EQP_ID = W.EQP_ID   
                                                             AND INSTR(W.M_RECIPE,ERI2.RECIPE_ID)>0                                                 
                                                            -- AND position(ERI2.RECIPE_ID in W.M_RECIPE)>0                                      
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT4 ERI4 ON ERI4.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                        
                                                             AND ERI4.EQP_ID = W.EQP_ID                                                  
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT5 ERI5 ON ERI5.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                        
                                                             AND position(ERI5.RECIPE_ID in W.M_RECIPE)>0                                      
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT6 ERI6 ON ERI6.EQP_ID = W.EQP_ID  
                                                             AND ERI6.SUB_LOT_TYPE = W.SUB_LOT_TYPE                                                   
                                                             AND position(ERI6.RECIPE_ID in W.M_RECIPE)>0                                                                          
                 LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT7 ERI7 ON  ERI7.PRODUCT_ID = (case when ERI7.PRODUCT_ID = '*' then '*' else W.PROD_ID end)                  
                                                             AND ERI7.EQP_ID = (case when ERI7.EQP_ID = '*' then '*' else W.EQP_ID end)                    
                                                             AND ERI7.SUB_LOT_TYPE =(case when ERI7.SUB_LOT_TYPE = '*' then '*' else W.SUB_LOT_TYPE end)   
                                                             AND position(ERI7.RECIPE_ID in (case when ERI7.RECIPE_ID = '*' then '*' else W.M_RECIPE end))>0 
                 GROUP BY W.PROD_ID,W.M_RECIPE,W.EQP_ID,W.SUB_LOT_TYPE 
                """.format(tempdb="")
        # my_duck.exec_sql(oracle_conn=oracle_conn,
        #                  duck_db_memory=duck_db_memory,
        #                  ETL_Proc_Name=ETL_Proc_Name,
        #                  methodName="Insert Into APS_TMP_ETL_RLS_APC_PILOT_RCP1",
        #                  sql=sql,
        #                  current_time=current_time,
        #                  update_table="APS_TMP_ETL_RLS_APC_PILOT_RCP1")
        pg_exist_result = pd.read_sql(sql, duck_db_temp)
        end_time = time.time()
        print("Clone Body 执行时间", str(end_time - start_time) + "秒")
        duck_db_temp.close()



if __name__ == '__main__':
    temp_process_db_file = sys.argv[1]
    ETL_Proc_Name =  sys.argv[2]
    print(temp_process_db_file, ETL_Proc_Name)
    try:
        getRcpInhibit(temp_process_db_file, ETL_Proc_Name)
    except Exception as eee:
        oracle_conn = my_oracle.oracle_get_connection()
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, eee, "",
                                   cons_error_code.APS_ETL_RLS_CODE_XX_ETL)
        traceback.print_exc()
        logging.exception(traceback.print_exc())
        oracle_conn.close()
