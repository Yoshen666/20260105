# import logging
#
# from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons
#
#
# def create_temp_table(duck_db):
#     sql = """
#     CREATE TABLE FLOW_LIST(
#         MAINPD_ID VARCHAR(384)
#     )
#     """
#     duck_db.sql(sql)
#
#
# def biz_method_01(duck_db_memory, uuid, current_time):
#     sql = """
#         insert into FLOW_LIST
#         select DISTINCT SUBSTR(MAINPD_ID, 1,POSITION('.' IN MAINPD_ID)-1) AS MAINPD_ID
#         from APS_SYNC_WIP.APS_SYNC_WIP
#         where 1=1
#     """
#     duck_db_memory.sql(sql)
#     sql = """
#     insert into APS_TR_CSFRINHIBIT(
#                     select '{uuid}'                           as PARENTID,
#                     A.PRODUCTID                                 as PRODUCTID,
#                     A.ROUTEID                                   as ROUTEID,
#                     A.OPENO                                     as OPENO,
#                     A.EQUIPMENTID                               as EQUIPMENTID,
#                     A.PASS_FLG_PRC                              as PASS_FLG_PRC,
#                     A.PASS_FLG_MFG                              as PASS_FLG_MFG,
#                     A.PRC_UPT_USER                              as PRC_UPT_USER,
#                     '{current_time}'                            as UPDATE_TIME,
#                     ''                                          as PARTCODE,
#                     A.CREATETIME                                as CREATE_TIME,
#                     A.PRC_UPT_TIME                              as PRC_UPDATE_TIME,
#                     A.MFG_UPT_TIME                              as MFG_UPDATE_TIME,
#                     SUBSTRING(A.PRODUCTID, 1, POSITION('.' in A.PRODUCTID) -1) as CHIPBODY
#                     from CSFRINHIBIT.CSFRINHIBIT A INNER JOIN FLOW_LIST B ON A.ROUTEID = B.MAINPD_ID
#             )
#     """.format(uuid=uuid, current_time=current_time)
#     duck_db_memory.sql(sql)
#
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
#     ETL_Proc_Name = "APS_ETL_BR.APS_USERFLAG_ALL_20M"
#     current_time = my_date.date_time_second_str()
#     current_time_short = my_date.date_time_second_short_str()
#     uuid = my_oracle.UUID()
#
#     target_table = "APS_TR_CSFRINHIBIT"
#     used_table_list = ['APS_SYNC_WIP', 'CSFRINHIBIT'] # TODO 这里要注意Amy的输出路径是否正确
#     target_table_sql = """
#         CREATE TABLE {}APS_TR_CSFRINHIBIT(
#             PARENTID VARCHAR(60),
#             PRODUCTID VARCHAR(60),
#             ROUTEID VARCHAR(60),
#             OPENO VARCHAR(60),
#             EQUIPMENTID VARCHAR(60),
#             PASS_FLG_PRC VARCHAR(60),
#             PASS_FLG_MFG VARCHAR(60),
#             PRC_UPT_USER VARCHAR(60),
#             UPDATE_TIME VARCHAR(60),
#             PARTCODE VARCHAR(60),
#             CREATE_TIME VARCHAR(60),
#             PRC_UPDATE_TIME VARCHAR(60),
#             MFG_UPDATE_TIME VARCHAR(60),
#             CHIPBODY VARCHAR(60)
#          )
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
#
#         biz_method_01(duck_db_memory, uuid, current_time)
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
#         my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, cons_error_code.APS_ETL_USER_FLAG)
#         logging.debug(e)
#         raise e
#     finally:
#         oracle_conn.commit()
#         oracle_conn.close()
#
#
# if __name__ == '__main__':
#     # 单JOB测试用
#     print("start")
#     execute()