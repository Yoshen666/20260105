import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    sql = """
        CREATE {table_type} TABLE FLOW_LIST( 
            MAINPD_ID VARCHAR(384)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
        CREATE {table_type} TABLE MES_FRLRCP(
            MNTR_PRODSPEC_ID VARCHAR(384) 
        )
    """.format(table_type=table_type)
    duck_db.sql(sql)


def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    insert into {tempdb}FLOW_LIST(MAINPD_ID) (
        SELECT DISTINCT SUBSTRING(MAINPD_ID, 1, POSITION('.' IN MAINPD_ID)-1) AS MAINPD_ID
        FROM APS_SYNC_WIP.APS_SYNC_WIP
    )
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into FLOW_LIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="FLOW_LIST")

    sql = """
        insert into {tempdb}MES_FRLRCP(MNTR_PRODSPEC_ID) (
            SELECT DISTINCT MNTR_PRODSPEC_ID as MNTR_PRODSPEC_ID
            FROM APS_SYNC_MES_FRLRCP.APS_SYNC_MES_FRLRCP
        )
        """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into FLOW_LIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="FLOW_LIST")
    sql = """
    insert into APS_TR_CSFRINHIBIT(                                                             
                    select '{uuid}'                         as PARENTID,                 
                    A.PRODUCTID                             as PRODUCTID,                
                    A.ROUTEID                               as ROUTEID,                  
                    A.OPENO                                 as OPENO,                    
                    A.PASS_FLG_MFG                          as PASS_FLG_MFG,              
                    A.PASS_FLG_PRC                          as PASS_FLG_PRC,             
                    A.EQUIPMENTID                           as EQUIPMENTID,             
                    A.CREATETIME                            as CREATETIME,                                 
                    A.PRC_UPT_USER                          as PRC_UPT_USER,              
                    A.PRC_UPT_TIME                          as PRC_UPT_TIME,          
                    A.PRC_MEMO                              as PRC_MEMO,
                    A.MFG_UPT_USER                          as MFG_UPT_USER,
                    A.MFG_UPT_TIME                          as MFG_UPT_TIME,
                    A.MFG_MEMO                              as MFG_MEMO,
                    A.PARTNAME                              as PARTNAME,                
                    SUBSTRING(A.PRODUCTID, 1, POSITION('.' in A.PRODUCTID) -1) as CHIPBODY   
                    from CSFRINHIBIT.CSFRINHIBIT A                                                       
                    WHERE EXISTS (SELECT 1 FROM {tempdb}FLOW_LIST B WHERE A.ROUTEID = B.MAINPD_ID)   
                    OR EXISTS (SELECT 1 FROM {tempdb}MES_FRLRCP P WHERE P.MNTR_PRODSPEC_ID = A.PRODUCTID)   
            )
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into FLOW_LIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="FLOW_LIST")

def execute():
    ###############################################################
    ### 以下参数必须定义
    ### ETL_Proc_Name    : ETL 名称
    ### current_time     ：请直接拷贝
    ### current_time_short ：请直接拷贝
    ### uuid             ：请直接拷贝
    ### target_table     : 该ETL输出表名
    ### used_table_list  : 该ETL使用到的，参考到的表名(中间表不算)
    ### target_table_sql ： 该ETL输出表定义SQL
    ###############################################################
    ETL_Proc_Name = "APS_ETL_BR.APS_TR_CSFRINHIBIT_15M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_TR_CSFRINHIBIT"
    used_table_list = ['APS_SYNC_WIP', 'APS_SYNC_MES_FRLRCP', 'CSFRINHIBIT']
    target_table_sql = """
        create table {}APS_TR_CSFRINHIBIT
        (
            PARENTID VARCHAR,
            PRODUCTID VARCHAR,
            ROUTEID VARCHAR,
            OPENO VARCHAR,
            PASS_FLG_MFG VARCHAR,
            PASS_FLG_PRC VARCHAR,
            EQUIPMENTID VARCHAR,
            CREATETIME VARCHAR,
            PRC_UPT_USER VARCHAR,
            PRC_UPT_TIME VARCHAR,
            PRC_MEMO VARCHAR,
            MFG_UPT_USER VARCHAR,
            MFG_UPT_TIME VARCHAR,
            MFG_MEMO VARCHAR,
            PARTNAME VARCHAR,
            CHIPBODY VARCHAR
        )
    """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
    target_db_file = my_duck.get_target_file_name(target_table, current_time_short)

    # -------------------------- 内存模式改成文件模式
    _temp_db_path = os.path.join(config.g_mem_speed_etl_output_path, target_table, "inprocess")
    if not os.path.exists(_temp_db_path):
        os.makedirs(_temp_db_path)

    temp_db_file = os.path.join(_temp_db_path, target_table + "_" + current_time_short + "_temp.db")
    # 处理中文件
    in_process_db_file = os.path.join(_temp_db_path, target_table + "_" + current_time_short + ".db")
    # 结果文件
    target_db_file = os.path.join(config.g_mem_etl_output_path, target_table,
                                  target_table + "_" + current_time_short + ".db")
    # --------------------------

    oracle_conn = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        # 开始日志
        my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
        # -------------------------- 内存模式改成文件模式
        # 创建DuckDB
        duck_db_memory = my_duck.create_duckdb_in_file(_temp_db_path, in_process_db_file, target_table_sql)
        duck_db_memory.sql('SET threads TO 4')
        duck_db_memory.execute(
            "SET temp_directory='{}'".format(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)))

        if not config.g_debug_mode:
            create_temp_table(duck_db_memory)
        else:
            duck_db_temp = my_duck.create_duckdb_for_temp_table(_temp_db_path, temp_db_file)
            # 创建Temp表
            create_temp_table(duck_db_temp)
            duck_db_temp.commit()
            duck_db_temp.close()
            my_duck.attach_temp_db_write_able(duck_db_memory, "TEMPDB", temp_db_file)
        # --------------------------
        # Attach用到的表
        my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################

        biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                   in_process_db_file=in_process_db_file,
                                                                                   target_table=target_table,
                                                                                   current_time=current_time_short)
        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 写完成日志
        my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 导出到目标文件中
        my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                  in_process_db_file=in_process_db_file,
                                                                  target_table=target_table,
                                                                  current_time=current_time_short)
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, cons_error_code.APS_ETL_USER_FLAG)
        raise e
    finally:
        oracle_conn.commit()
        oracle_conn.close()
        # 删除TMP目录:LQN:2023/08/21
        if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
            os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            os.remove(temp_db_file)
        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()