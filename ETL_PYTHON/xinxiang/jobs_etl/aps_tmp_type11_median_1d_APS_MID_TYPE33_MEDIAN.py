import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code


def create_temp_table(duck_db):
    pass


def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT  /*+ append */  INTO APS_MID_IDLE_DUMMY12_MEDIAN(                             
         PARENTID, PROCESS_TIME, TOOLG_ID, UPDATE_TIME, PARTCODE                                                                     
        )                                                                                  
        WITH TOOL_DATA AS (                                                              
             SELECT EQP_ID,                                                              
                    MAX(EQP_G) AS TOOLG_ID                                               
             FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T                                
             WHERE T.HOST_EQP_FLAG='Y'        
             GROUP BY EQP_ID                                                             
        ),                                                                               
        COMPLETE_DATA AS (                                                                      
             SELECT D.TOOLG_ID,                                                            
              -- ROUND((C.PROCESS_END_TIME - C.PROCESS_START_TIME)*24,3) AS PROCESS_TIME,   
               ROUND((extract(epoch from(CAST(C.PROCESS_END_TIME AS TIMESTAMP) 
                     - (CAST(C.PROCESS_START_TIME AS TIMESTAMP))))) / 3600.0, 3) AS PROCESS_TIME,                  
             ROW_NUMBER() OVER (PARTITION BY TOOLG_ID  ORDER BY PROCESS_START_TIME DESC) AS RN     
             FROM APS_SYNC_OCS_AUTODUMMY_COMPLETE.APS_SYNC_OCS_AUTODUMMY_COMPLETE C        
             INNER JOIN TOOL_DATA D                                                      
             ON D.EQP_ID = C.TOOLID                                                      
             WHERE C.SELECTTYPE='IDLE'     
             AND C.JOBSTATE='End'                                                                    
             AND C.PROCESS_END_TIME IS NOT NULL AND C.PROCESS_END_TIME <> '' AND C.PROCESS_START_TIME IS NOT NULL AND C.PROCESS_START_TIME <> ''      
             AND C.PROCESS_END_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '90 day'                                    
        ),                                                                                           
        SOURE_DATA AS (                                                                         
             SELECT DISTINCT 
                    ---MEDIAN(CAST(PROCESS_TIME AS DECIMAL)) OVER(PARTITION BY TOOLG_ID ) AS PROCESS_TIME, 
                    QUANTILE_CONT(CAST(PROCESS_TIME AS DECIMAL),0.5) OVER(PARTITION BY TOOLG_ID ) AS PROCESS_TIME,                                
                   TOOLG_ID,RN                                                                
            FROM COMPLETE_DATA                                                           
        ),                                                                               
        GROUP_DATA AS (                                                                         
            SELECT                                                                                   
                   MAX(PROCESS_TIME) AS PROCESS_TIME,                                           
                   TOOLG_ID,MAX(RN) AS RN                                                     
            FROM SOURE_DATA                                                              
            GROUP BY TOOLG_ID                                                            
        )                                                                                
        SELECT  '{uuid}',                                                 
               PROCESS_TIME,                                                             
               TOOLG_ID,                                                                 
               '{current_time}',                                                   
               ''                                                     
        FROM GROUP_DATA                                                                  
        --WHERE RN > 10                                                                                                                                                                                                                                                                                                                                           
    """.format(uuid=uuid, current_time=current_time)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_MID_IDLE_DUMMY12_MEDIAN",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_MID_IDLE_DUMMY12_MEDIAN")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_TMP_TYPE11_MEDIAN_1D_APS_MID_TYPE33_MEDIAN"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_MID_IDLE_DUMMY12_MEDIAN"
    used_table_list = ['APS_SYNC_ETL_TOOL', 'APS_SYNC_OCS_AUTODUMMY_COMPLETE']
    target_table_sql = """
    create table {}APS_MID_IDLE_DUMMY12_MEDIAN
    (
        parentid     VARCHAR(64),
        toolg_id     VARCHAR(64),
        process_time decimal,
        update_time  VARCHAR(64),
        partcode     VARCHAR(64)
    )
    """.format("")  # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
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
        if not os.path.exists(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)):
            os.makedirs(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid))
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, "XETL052")
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