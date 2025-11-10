# import logging
# import os
# import time
#
# from xinxiang import config
# from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons
#
#
# def create_temp_table(duck_db):
#     '''
#     这里在内存中创建使用到的临时表
#     '''
#     sql = """
#     create table APS_TMP_CSFRINHIBIT_WIP
#     (
#         mainpd_id VARCHAR(64)
#     )
#     """
#     duck_db.sql(sql)
#
# def biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
#     sql = """
#         INSERT  /*+ append */  INTO APS_TR_CSFRINHIBIT(
#               PARENTID,PRODUCTID, ROUTEID, OPENO, EQUIPMENTID, PASS_FLG_PRC,
#               PASS_FLG_MFG, PRC_UPT_USER, UPDATE_TIME, PARTCODE, CREATE_TIME, PRC_UPDATE_TIME,
#               MFG_UPDATE_TIME, CHIPBODY
#             )
#              WITH TOOL AS (
#                    SELECT EQP_ID,MAX(L.REAL_MODULE) AS MODULE
#                    FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL L
#                    WHERE L.REAL_MODULE IN ('PHOTO','WET','DIFF')
#                    AND L.REAL_MODULE IN ('PHOTO','WET','DIFF') AND  HOST_EQP_FLAG ='Y'
#                    GROUP BY EQP_ID
#              )
#              SELECT '{uuid}' AS PARENTID,
#                     V.PRODUCTID,
#                     V.ROUTEID,
#                     V.OPENO,V.EQUIPMENTID,
#                     V.PASS_FLG_PRC,V.PASS_FLG_MFG,
#                     V.PRC_UPT_USER,
#                     STRFTIME(V.PRC_UPT_TIME, '%Y-%m-%d %H:%M:%S') AS UPDATE_TIME,
#                     '' AS PARTCODE,
#                     STRFTIME(V.CREATETIME, '%Y-%m-%d %H:%M:%S') AS CREATE_TIME,
#                     STRFTIME(V.PRC_UPT_TIME, '%Y-%m-%d %H:%M:%S') AS PRC_UPDATE_TIME,
#                     STRFTIME(V.MFG_UPT_TIME, '%Y-%m-%d %H:%M:%S') AS MFG_UPDATE_TIME,
#                     SUBSTRING(V.PRODUCTID, 1, POSITION('.' IN V.PRODUCTID)-1) AS CHIPBODY
#              FROM APS_SYNC_CSFRINHIBIT.APS_SYNC_CSFRINHIBIT V
#              INNER JOIN APS_TMP_CSFRINHIBIT_WIP W
#              ON W.MAINPD_ID = V.ROUTEID
#              INNER JOIN  TOOL L
#              ON V.EQUIPMENTID = L.EQP_ID
#     """.format(uuid=uuid, current_time=current_time)
#
#     my_duck.exec_sql(oracle_conn=oracle_conn,
#                      duck_db_memory=duck_db_memory,
#                      ETL_Proc_Name=ETL_Proc_Name,
#                      methodName="Insert Into APS_TR_CSFRINHIBIT",
#                      sql=sql,
#                      current_time=current_time)
#
#
# def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
#     sql = """
#       INSERT INTO APS_TMP_CSFRINHIBIT_WIP(
#         MAINPD_ID
#         )
#           SELECT DISTINCT SUBSTRING(MAINPD_ID, 1,
#            POSITION('.' IN MAINPD_ID)-1) AS MAINPD_ID
#           FROM APS_SYNC_WIP.APS_SYNC_WIP
#     """.format(current_time=current_time)
#
#     my_duck.exec_sql(oracle_conn=oracle_conn,
#                      duck_db_memory=duck_db_memory,
#                      ETL_Proc_Name=ETL_Proc_Name,
#                      methodName="Insert Into APS_TMP_CSFRINHIBIT_WIP",
#                      sql=sql,
#                      current_time=current_time)
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
#     ETL_Proc_Name = "APS_ETL_BR.APS_TR_CSFRINHIBIT_5M"
#     current_time = my_date.date_time_second_str()
#     current_time_short = my_date.date_time_second_short_str()
#     uuid = my_oracle.UUID()
#
#     target_table = "APS_TR_CSFRINHIBIT"
#     used_table_list = ['APS_SYNC_WIP',
#                        'APS_SYNC_ETL_TOOL',
#                        'APS_SYNC_CSFRINHIBIT']
#     target_table_sql = """
#         create table {}APS_TR_CSFRINHIBIT
#         (
#             parentid        VARCHAR(60),
#             productid       VARCHAR(60),
#             routeid         VARCHAR(60),
#             openo           VARCHAR(60),
#             equipmentid     VARCHAR(60),
#             pass_flg_prc    VARCHAR(60),
#             pass_flg_mfg    VARCHAR(60),
#             prc_upt_user    VARCHAR(60),
#             update_time     VARCHAR(60),
#             partcode        VARCHAR(60),
#             create_time     VARCHAR(64),
#             prc_update_time VARCHAR(64),
#             mfg_update_time VARCHAR(64),
#             chipbody        VARCHAR(64)
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
#         # 创建Temp表
#         create_temp_table(duck_db_memory)
#         # Attach用到的表
#         my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
#         ################################################################################################################
#         ## 以下为业务逻辑
#         ################################################################################################################
#         biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
#
#         row_count = my_duck.get_row_count_in_duckdb(duck_db=duck_db_memory, tableName="APS_TMP_CSFRINHIBIT_WIP")
#         if row_count == 0:
#             my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time, "APS_TMP_CSFRINHIBIT_WIP没有数据，请及时确认一下！！")
#             return
#
#         biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
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
#         logging.debug(e)
#         raise e
#     finally:
#         oracle_conn.commit()
#         oracle_conn.close()
#         # 删除TMP目录:LQN:2023/08/21
#         if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
#             os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
#
#
# if __name__ == '__main__':
#     # 单JOB测试用
#     print("start")
#     execute()