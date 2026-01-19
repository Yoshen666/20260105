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
                      ,CASE WHEN CONCAT_WS('',
                            MAX(ERI1.product_id), MAX(ERI2.eqp_id), MAX(ERI3.eqp_id), MAX(ERI4.eqp_id),
                            MAX(ERI5.recipe_id), MAX(ERI7.eqp_id), MAX(ERI8.eqp_id), MAX(ERI9.eqp_id),
                            MAX(ERI10.eqp_id), MAX(ERI11.eqp_id), MAX(ERI12.eqp_id)
                        ) <> ''
                        THEN 'EqpRcpInhibit;'
                        END AS SET_STATUS2                                                                                                                                                                                                                                                              
                 FROM GROUP_APC_PILOT W                                                                                                                                                       
                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT1 ERI1
                        ON ERI1.product_id = W.PROD_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT2 ERI2
                        ON ERI2.route_id = W.ROUTE_ID
                        AND ERI2.ope_no = W.OPE_NO
                        AND ERI2.product_id = W.PROD_ID
                        AND ERI2.eqp_id = W.EQP_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT3 ERI3
                        ON ERI3.route_id = W.ROUTE_ID
                        AND ERI3.ope_no = W.OPE_NO
                        AND ERI3.product_id = W.PROD_ID
                        AND ERI3.eqp_id = W.EQP_ID
                        AND ERI3.chamber_id = W.CHAMBER_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT4 ERI4
                        ON ERI4.route_id = W.ROUTE_ID
                        AND ERI4.ope_no = W.OPE_NO
                        AND ERI4.product_id = W.PROD_ID
                        AND ERI4.eqp_id = W.EQP_ID
                        AND INSTR(W.M_RECIPE, ERI4.recipe_id) > 0

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT5 ERI5
                        ON INSTR(W.M_RECIPE, ERI5.recipe_id) > 0

                    -- INHIBIT6: Reticle 暫時略過

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT7 ERI7
                        ON ERI7.eqp_id = W.EQP_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT8 ERI8
                        ON ERI8.eqp_id = W.EQP_ID
                        AND INSTR(W.M_RECIPE, ERI8.recipe_id) > 0

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT9 ERI9
                        ON ERI9.eqp_id = W.EQP_ID
                        AND ERI9.chamber_id = W.CHAMBER_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT10 ERI10
                        ON ERI10.eqp_id = W.EQP_ID
                        AND ERI10.chamber_id = W.CHAMBER_ID
                        AND ERI10.product_id = W.PROD_ID

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT11 ERI11
                        ON ERI11.eqp_id = W.EQP_ID
                        AND ERI11.product_id = W.PROD_ID
                        AND INSTR(W.M_RECIPE, ERI11.recipe_id) > 0

                    LEFT JOIN {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT12 ERI12
                        ON ERI12.product_id = W.PROD_ID
                        AND ERI12.eqp_id = W.EQP_ID
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
