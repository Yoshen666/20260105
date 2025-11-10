# import gc
# import logging
# import os
#
# from xinxiang import config
# from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons
#
#
# def create_temp_table(duck_db):
#     pass
#
#
# def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
#     sql = """
#     INSERT  /*+ append */  INTO APS_TR_LOTHISTORY (
#         LOT_ID,OPE_NO,CLAIM_TIME,CTRL_JOB,OPE_CATEGORY,MAINPD_ID,
#         PRODSPEC_ID,EQP_ID,PRIORITY_CLASS,PH_RECIPE_ID,LOCATION_ID,
#         TECH_ID,CUSTOMER_ID,LOT_TYPE,RESERVE_TIME,PROCESS_END,
#         SCANNER_END,SCANNER_START,TRACK_IN,PROCESS_START,PREV_OP_COMP_TIME, GUID, PARTKEY
#     )
#      SELECT * FROM (
#      ---------针对找出前一个事件的站点OperationComplete对应站点是上一个站点的数据---------
#      WITH ORE_COMPLETE1 AS (
#          SELECT S.OPE_CATEGORY,S.OPE_NO,CLAIM_TIME,S.CTRL_JOB,S.LOT_ID,
#          LAG(S.OPE_NO,1,NULL) OVER(PARTITION BY LOT_ID,CTRL_JOB  ORDER BY CLAIM_TIME) AS OPE_NO2,
#          LAG(S.MAINPD_ID,1,NULL) OVER(PARTITION BY LOT_ID,CTRL_JOB  ORDER BY CLAIM_TIME) AS MAINPD_ID2
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#      ),
#      -------针对跳站等动作直接找上一个站点的数据------------
#      ORE_COMPLETE2 AS (
#          SELECT S.OPE_CATEGORY,S.OPE_NO,CLAIM_TIME,S.CTRL_JOB,S.LOT_ID,S.MAINPD_ID,S.PRODSPEC_ID,
#          S.EQP_ID,S.PRIORITY_CLASS,S.PH_RECIPE_ID,S.LOCATION_ID,S.TECH_ID,S.CUSTOMER_ID,S.LOT_TYPE,
#          LAG(S.OPE_NO,1,null) OVER(PARTITION BY LOT_ID  ORDER BY CLAIM_TIME) AS OPE_NO2
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#          AND S.OPE_CATEGORY <>'BankIn'
#      ),
#      ------------找出OperationComplete时间，实际就是该站点的到站时间-------
#      LAST_OPE_COMPLETE AS (
#          SELECT S.LOT_ID,S.OPE_NO,
#                 MAX(CLAIM_TIME) AS PREV_OP_COMP_TIME
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#          AND OPE_CATEGORY IN  ('OperationComplete','ForceOperationComplete')
#          GROUP BY S.LOT_ID,S.OPE_NO
#      ),
#      ------------找出STB转flow时间，实际就是该站点的到站时间-------
#      LAST_OPE_STB AS (
#          SELECT S.LOT_ID,S.OPE_NO,
#                 MAX(CLAIM_TIME) AS PREV_OP_COMP_TIME
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#          AND OPE_CATEGORY = 'STB'
#          GROUP BY S.LOT_ID,S.OPE_NO
#      ),
#      ------------找出OperationComplete时间，实际就是该站点的到站时间-------
#      LAST_OPE_COMPLETE_LOCATEWARD AS (
#          SELECT S.LOT_ID,S.OPE_NO,
#                 MAX(CLAIM_TIME) AS PREV_OP_COMP_TIME
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#          AND OPE_CATEGORY = 'LocateBackward'
#          GROUP BY S.LOT_ID,S.OPE_NO
#      ),
#      --------------找出相同LOT上一个事件的时间-------------
#      LAST_OPE_COMPLETE_XL AS (
#          SELECT S.LOT_ID,S.OPE_NO,CLAIM_TIME,
#                 LAG(S.CLAIM_TIME,1,NULL) OVER(PARTITION BY LOT_ID ORDER BY CLAIM_TIME) AS PREV_OP_COMP_TIME
#          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#      ),
#      LAST_OPE_COMPLETE_XL_DATA AS (
#          SELECT S.LOT_ID,S.OPE_NO,
#                MAX(PREV_OP_COMP_TIME) AS PREV_OP_COMP_TIME
#          FROM LAST_OPE_COMPLETE_XL S
#          GROUP BY S.LOT_ID,S.OPE_NO
#      ),
#      ----------找出最早的批次记录-----------
#      LAST_STB_DATA AS (
#          SELECT MIN(CLAIM_TIME) AS PREV_OP_COMP_TIME,LOT_ID
#          FROM  APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          GROUP BY S.LOT_ID
#      ),
#      -------找出类似dyb flow 到站的时间------------
#      DYB_DATA AS (
#          SELECT MIN(CLAIM_TIME) AS PREV_OP_COMP_TIME,LOT_ID,
#                 OPE_NO,MAINPD_ID
#          FROM  APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S
#          WHERE 1=1
#                AND S.OPE_CATEGORY='Branch'
#          GROUP BY S.LOT_ID,S.OPE_NO,S.MAINPD_ID
#      ),
#      -------------根据CTRL_JOB,LOT_ID 找出对应TrackIn，processStart等事件的时间--------
#      PROCESS AS(
#          SELECT CTRL_JOB,LOT_ID,
#                 MAX(MAINPD_ID) AS MAINPD_ID,
#                 MAX(EQP_ID) AS EQP_ID,
#                 MAX(PRODSPEC_ID) AS PRODSPEC_ID,
#                 MAX(PRIORITY_CLASS) AS PRIORITY_CLASS,
#                 MAX(PH_RECIPE_ID) AS PH_RECIPE_ID,
#                 MAX(LOCATION_ID) AS LOCATION_ID,
#                 MAX(TECH_ID) AS TECH_ID,
#                 MAX(CUSTOMER_ID) AS CUSTOMER_ID,
#                 MAX(LOT_TYPE) AS LOT_TYPE,
#                 MAX(RESERVE_TIME) AS RESERVE_TIME,
#                 MAX(TRACK_IN) AS TRACK_IN,
#                 MAX(PROCESS_START) AS PROCESS_START,
#                 MAX(PROCESS_END) AS PROCESS_END,
#                 MAX(SCANNER_END) AS SCANNER_END,
#                 MAX(SCANNER_START) AS SCANNER_START
#          FROM (
#              SELECT CTRL_JOB,LOT_ID,
#                     MAINPD_ID,PRODSPEC_ID,EQP_ID,PRIORITY_CLASS,
#                     PH_RECIPE_ID,LOCATION_ID,TECH_ID,CUSTOMER_ID,LOT_TYPE,
#                     CASE WHEN OPE_CATEGORY='Reserve' THEN CLAIM_TIME ELSE NULL END AS RESERVE_TIME,
#                     CASE WHEN OPE_CATEGORY='OperationStart' THEN CLAIM_TIME ELSE NULL END AS TRACK_IN,
#                     CASE WHEN OPE_CATEGORY='ProcessStart' THEN CLAIM_TIME ELSE NULL END AS PROCESS_START,
#                     CASE WHEN OPE_CATEGORY='ProcessEnd' THEN CLAIM_TIME ELSE NULL END AS PROCESS_END,
#                     CASE WHEN OPE_CATEGORY='ScannerEnd' THEN CLAIM_TIME ELSE NULL END AS SCANNER_END,
#                     CASE WHEN OPE_CATEGORY='ScannerStart' THEN CLAIM_TIME ELSE NULL END AS SCANNER_START
#                     FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW
#              WHERE OPE_CATEGORY IN ('Reserve','ProcessEnd','ScannerEnd','ScannerStart',
#                                     'OperationStart','ProcessStart')
#          ) T
#          GROUP BY T.CTRL_JOB,T.LOT_ID
#      )
#      SELECT C.LOT_ID,C.OPE_NO2 AS OPE_NO,
#             -- TO_CHAR(C.CLAIM_TIME,'YYYY-MM-DD HH24:MI:SS') AS CLAIM_TIME,
#             STRFTIME(C.CLAIM_TIME, '%Y-%m-%d %H:%M:%S') AS CLAIM_TIME,
#             C.CTRL_JOB,C.OPE_CATEGORY,
#             P.MAINPD_ID,
#             P.PRODSPEC_ID,P.EQP_ID,P.PRIORITY_CLASS,
#             P.PH_RECIPE_ID,P.LOCATION_ID,P.TECH_ID,
#             P.CUSTOMER_ID,P.LOT_TYPE,
#             -- TO_CHAR(P.RESERVE_TIME,'YYYY-MM-DD HH24:MI:SS') AS RESERVE_TIME,
#             STRFTIME(P.RESERVE_TIME, '%Y-%m-%d %H:%M:%S') AS RESERVE_TIME,
#             -- TO_CHAR(P.PROCESS_END,'YYYY-MM-DD HH24:MI:SS') AS PROCESS_END,
#             STRFTIME(P.PROCESS_END, '%Y-%m-%d %H:%M:%S') AS PROCESS_END,
#             -- TO_CHAR(P.SCANNER_END,'YYYY-MM-DD HH24:MI:SS') AS SCANNER_END,
#             STRFTIME(P.SCANNER_END, '%Y-%m-%d %H:%M:%S') AS SCANNER_END,
#             -- TO_CHAR(P.SCANNER_START,'YYYY-MM-DD HH24:MI:SS') AS SCANNER_START,
#             STRFTIME(P.SCANNER_START, '%Y-%m-%d %H:%M:%S') AS SCANNER_START,
#             -- TO_CHAR(P.TRACK_IN,'YYYY-MM-DD HH24:MI:SS') AS TRACK_IN,
#             STRFTIME(P.TRACK_IN, '%Y-%m-%d %H:%M:%S') AS TRACK_IN,
#             -- TO_CHAR(P.PROCESS_START,'YYYY-MM-DD HH24:MI:SS') AS PROCESS_START,
#             STRFTIME(P.PROCESS_START, '%Y-%m-%d %H:%M:%S') AS PROCESS_START,
#             -- CASE WHEN CE.OPE_NO IS NOT NULL THEN TO_CHAR(CE.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS')
#             CASE WHEN CE.OPE_NO IS NOT NULL THEN STRFTIME(CE.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S')
#                  -- WHEN STB.OPE_NO IS NOT NULL THEN TO_CHAR(STB.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS')
#                  WHEN STB.OPE_NO IS NOT NULL THEN STRFTIME(STB.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S')
#                  -- WHEN DD.OPE_NO IS NOT NULL THEN TO_CHAR(DD.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS')
#                  WHEN DD.OPE_NO IS NOT NULL THEN STRFTIME(DD.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S')
#                  -- WHEN LL.OPE_NO IS NOT NULL THEN TO_CHAR(LL.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS')
#                  WHEN LL.OPE_NO IS NOT NULL THEN STRFTIME(LL.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S')
#                  -- ELSE TO_CHAR(DA.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS')
#                  ELSE STRFTIME(DA.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S')
#             END AS PREV_OP_COMP_TIME,
#             -- SYS_GUID() AS GUID,
#             gen_random_uuid() AS GUID,
#             C.CLAIM_TIME AS UPDATE_TIME
#      FROM ORE_COMPLETE1 C
#      LEFT JOIN PROCESS P
#      ON C.LOT_ID = P.LOT_ID
#      AND C.CTRL_JOB = P.CTRL_JOB
#      LEFT JOIN LAST_OPE_COMPLETE CE
#      ON CE.LOT_ID = C.LOT_ID
#      AND CE.OPE_NO = C.OPE_NO2
#      LEFT JOIN LAST_OPE_STB STB
#      ON STB.LOT_ID = C.LOT_ID
#      AND STB.OPE_NO = C.OPE_NO2
#      LEFT JOIN LAST_STB_DATA DA
#      ON DA.LOT_ID = C.LOT_ID
#      LEFT JOIN DYB_DATA DD
#      ON DD.LOT_ID = C.LOT_ID
#      AND DD.OPE_NO = C.OPE_NO2
#      AND DD.MAINPD_ID = C.MAINPD_ID2
#      LEFT JOIN LAST_OPE_COMPLETE_LOCATEWARD LL
#      ON LL.LOT_ID = C.LOT_ID
#      AND LL.OPE_NO = C.OPE_NO2
#      WHERE C.OPE_CATEGORY IN  ('OperationComplete','ForceOperationComplete')
#      UNION ALL
#      SELECT DISTINCT C.LOT_ID,C.OPE_NO2 AS OPE_NO,
#             -- TO_CHAR(C.CLAIM_TIME,'YYYY-MM-DD HH24:MI:SS') AS CLAIM_TIME,
#             STRFTIME(C.CLAIM_TIME, '%Y-%m-%d %H:%M:%S') AS CLAIM_TIME,
#             C.CTRL_JOB,C.OPE_CATEGORY,C.MAINPD_ID,
#             C.PRODSPEC_ID,C.EQP_ID,C.PRIORITY_CLASS,
#             C.PH_RECIPE_ID,C.LOCATION_ID,C.TECH_ID,
#             C.CUSTOMER_ID,C.LOT_TYPE,
#             '' AS RESERVE_TIME, '' AS PROCESS_END,'' AS SCANNER_END,'' AS SCANNER_START,
#             '' AS TRACK_IN,'' AS PROCESS_START,
#             -- TO_CHAR(XL.PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS') AS PREV_OP_COMP_TIME,
#             STRFTIME(XL.PREV_OP_COMP_TIME, '%Y-%m-%d %H:%M:%S') AS PREV_OP_COMP_TIME,
#             gen_random_uuid() AS GUID,
#             C.CLAIM_TIME AS UPDATE_TIME
#      FROM ORE_COMPLETE2 C
#      LEFT JOIN LAST_OPE_COMPLETE_XL_DATA XL
#      ON XL.LOT_ID = C.LOT_ID
#      AND XL.OPE_NO = C.OPE_NO2
#      WHERE 1=1
#      AND C.OPE_CATEGORY IN  ('GatePass','LocateForward','LocateBackward')
#      )
#      ORDER BY CLAIM_TIME DESC,PREV_OP_COMP_TIME DESC
#     """.format(uuid=uuid, current_time=current_time, part_code=cons.RLS_PART_VALUE_NEW)
#     my_duck.exec_sql(oracle_conn=oracle_conn,
#                      duck_db_memory=duck_db_memory,
#                      ETL_Proc_Name=ETL_Proc_Name,
#                      methodName="Insert Into APS_TR_LOTHISTORY",
#                      sql=sql,
#                      current_time=current_time,
#                      update_table="APS_TR_LOTHISTORY")
#
#
# def execute():
#     ###############################################################
#     ### 以下参数必须定义
#     ### ETL_Proc_Name    : ETL 名称
#     ### current_time     ：请直接拷贝
#     ### current_time_short ：请直接拷贝
#     ### uuid             ：请直接拷贝
#     ### target_table     : 该ETL输出表名
#     ### used_table_list  : 该ETL使用到的，参考到的表名(中间表不算)
#     ### target_table_sql ： 该ETL输出表定义SQL
#     ###############################################################
#     ETL_Proc_Name = "APS_ETL_BR.APS_TR_LOTHISTORY_5M"
#     current_time = my_date.date_time_second_str()
#     current_time_short = my_date.date_time_second_short_str()
#     uuid = my_oracle.UUID()
#
#     target_table = "APS_TR_LOTHISTORY"
#     used_table_list = ['APS_MID_FHOPEHS_VIEW']
#     target_table_sql = """
#         create table {}APS_TR_LOTHISTORY
#         (
#           lot_id            VARCHAR(64),
#           ope_no            VARCHAR(64),
#           claim_time        VARCHAR(64),
#           ctrl_job          VARCHAR(64),
#           ope_category      VARCHAR(64),
#           mainpd_id         VARCHAR(64),
#           prodspec_id       VARCHAR(64),
#           eqp_id            VARCHAR(64),
#           priority_class    VARCHAR(64),
#           ph_recipe_id      VARCHAR(64),
#           location_id       VARCHAR(64),
#           tech_id           VARCHAR(64),
#           customer_id       VARCHAR(64),
#           lot_type          VARCHAR(64),
#           prev_op_comp_time VARCHAR(64),
#           reserve_time      VARCHAR(64),
#           track_in          VARCHAR(64),
#           process_start     VARCHAR(64),
#           scanner_start     VARCHAR(64),
#           scanner_end       VARCHAR(64),
#           process_end       VARCHAR(64),
#           guid              VARCHAR(64) not null,
#           partkey           VARCHAR(64) not null,
#           PRIMARY KEY (GUID, PARTKEY)
#         )
#     """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
#     target_db_file = my_duck.get_target_file_name(target_table, current_time_short)
#
#     oracle_conn = None
#     try:
#         oracle_conn = my_oracle.oracle_get_connection()
#         # 开始日志
#         my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
#         # 创建DuckDB
#         duck_db_memory = my_duck.create_duckdb_in_momory(target_table_sql)
#         if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
#             duck_db_memory.sql('SET threads TO 2')
#             # 加上TMP目录:LQN:2023/08/21
#             duck_db_memory.execute(
#                 "SET temp_directory='{}'".format(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)))
#         # 创建Temp表
#         create_temp_table(duck_db_memory)
#         # Attach用到的表
#         my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
#         ################################################################################################################
#         ## 以下为业务逻辑
#         ################################################################################################################
#
#         biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
#
#         ################################################################################################################
#         ## 以上为业务逻辑
#         ################################################################################################################
#         # 导出到目标文件中
#         target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory(duck_db_memory,
#                                                                                   target_table,
#                                                                                   target_table_sql.format("file_db."),
#                                                                                   current_time_short)
#         # 写版本号
#         my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file)
#         # 写完成日志
#         my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
#     except Exception as e:
#         # 写警告日志
#         my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, cons_error_code.APS_TR_TYPE_CODE_XX_ETL)
#         logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
#         raise e
#     finally:
#         oracle_conn.commit()
#         oracle_conn.close()
#         if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
#             # 删除TMP目录:LQN:2023/08/21
#             if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
#                 os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
#         gc.collect()  # 内存释放
#
#
# if __name__ == '__main__':
#     # 单JOB测试用
#     print("start")
#     execute()