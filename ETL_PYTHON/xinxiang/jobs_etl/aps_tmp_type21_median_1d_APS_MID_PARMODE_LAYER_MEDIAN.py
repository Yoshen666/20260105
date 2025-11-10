import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''
    sql = """
        create {table_type} table APS_TMP_PARMODE_LAYER_MEDIAN_TOOL
        (
          tool_id  VARCHAR(64),
          MINOR_CH    VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_PARMODE_LAYER_MEDIAN_CH_COUNT
       (
         CTRL_JOB  VARCHAR(64),
         CH_COUNT    VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

def GetMinorChFlagData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_PARMODE_LAYER_MEDIAN_TOOL (                                
           TOOL_ID, MINOR_CH
        )                                                                                      
        SELECT TOOL_ID, 
               SUBSTRING(CH_ID, POSITION('.' IN CH_ID)+1) AS MINOR_CH 
        FROM APS_ETL_TOOL.APS_ETL_TOOL
        WHERE MINOR_CH_FLAG = 1                                                                       
        """.format(uuid=uuid, current_time=current_time,tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_PARMODE_LAYER_MEDIAN_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_PARMODE_LAYER_MEDIAN_TOOL")

def GetChCountData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_PARMODE_LAYER_MEDIAN_CH_COUNT (                                
           CTRL_JOB, CH_COUNT
        )                                                                                      
        SELECT CTRL_JOB, 
               COUNT(*) AS CH_COUNT 
        FROM APS_TR_TYPE21_PARMODE.APS_TR_TYPE21_PARMODE T
        WHERE CAST(QTY AS DECIMAL) >= 12 
        AND NOT EXISTS (SELECT 1 FROM {tempdb}APS_TMP_PARMODE_LAYER_MEDIAN_TOOL L 
                       WHERE L.TOOL_ID = T.TOOL_ID AND L.MINOR_CH = T.MEMO)   
        GROUP BY T.CTRL_JOB                                                                   
        """.format(uuid=uuid, current_time=current_time,tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_PARMODE_LAYER_MEDIAN_CH_COUNT",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_PARMODE_LAYER_MEDIAN_CH_COUNT")

def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_MID_PARMODE_LAYER_MEDIAN (                                
       PARENTID, TOOLG_ID, LAYER, PT, UPDATE_TIME, PARTCODE
    )                                                                                      
    WITH TYPE21 AS (                                                                      
          SELECT ROUND((EXTRACT(EPOCH FROM(CAST(PROCESS_END AS TIMESTAMP) - (CAST(PROCESS_START AS TIMESTAMP))))) / 3600.0, 3) AS PT, 
               PROCESS_START, 
               PROCESS_END,
               PPID,QTY, CLAIM_MEMO,MEMO,TOOL_ID,TOOLG_ID,LAYER,
              -- CASE WHEN INSTR(replace(replace(replace(replace(replace(CLAIM_MEMO,'&','|'),'@',''),';',''),')',''),'(',''),'|') =0 THEN 1 ELSE LENGTH(replace(replace(replace(replace(replace(CLAIM_MEMO,'&','|'),'@',''),';',''),')',''),'(','')) - LENGTH(REPLACE(replace(replace(replace(replace(replace(CLAIM_MEMO,'&','|'),'@',''),';',''),')',''),'(',''),'|','')) +1 END AS COUNT    
              CAST(C.CH_COUNT AS DECIMAL(18,0)) AS COUNT 
          FROM APS_TR_TYPE21_PARMODE.APS_TR_TYPE21_PARMODE T 
          INNER JOIN {tempdb}APS_TMP_PARMODE_LAYER_MEDIAN_CH_COUNT C
          ON C.CTRL_JOB  = T.CTRL_JOB    
          AND NOT EXISTS (SELECT 1 FROM {tempdb}APS_TMP_PARMODE_LAYER_MEDIAN_TOOL L 
                       WHERE L.TOOL_ID = T.TOOL_ID AND L.MINOR_CH = T.MEMO)                                                                                                             
    ),
    NUM_DATA AS (
        SELECT ROUND((25/CAST(QTY AS DECIMAL))*PT*COUNT, 3) AS PT,
               PROCESS_START, 
               PROCESS_END,
               QTY, TOOLG_ID,LAYER,
               COUNT,
               ROW_NUMBER() OVER(PARTITION BY TOOLG_ID,LAYER ORDER BY PROCESS_END DESC) AS RN
        FROM TYPE21
    ),
    COUNT_DATA AS (
        SELECT PT,
               PROCESS_START, 
               PROCESS_END,
               QTY, TOOLG_ID, LAYER,
               COUNT,
               RN
        FROM NUM_DATA
        WHERE RN <= 30 
    ),                                                                                  
    PT_DATA AS (                                                                          
         SELECT 
              QUANTILE_CONT(CAST(PT AS DECIMAL),0.5) OVER(PARTITION BY TOOLG_ID, LAYER ) AS MAX_50
             ,TOOLG_ID, RN, LAYER                                            
         FROM COUNT_DATA                                                                  
    ),                                                                                    
    RESULT_DATA AS (                                                                      
        SELECT  TOOLG_ID, LAYER                                          
                ,MAX(MAX_50) AS PT,MAX(RN) AS RN                                           
        FROM PT_DATA                                                                       
        GROUP BY TOOLG_ID, LAYER                                       
    )                                                                                     
    SELECT '{uuid}',                                                      
            TOOLG_ID, 
            LAYER,                                            
            PT,                                                                            
            '{current_time}',                                                         
            ''                                           
    FROM RESULT_DATA                                                                      
    WHERE RN >= 30                                                                           
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_MID_PARMODE_LAYER_MEDIAN",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_MID_PARMODE_LAYER_MEDIAN")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_TMP_TYPE21_MEDIAN_1D_APS_MID_PARMODE_LAYER_MEDIAN"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_MID_PARMODE_LAYER_MEDIAN"
    used_table_list = ['APS_TR_TYPE21_PARMODE', 'APS_ETL_TOOL']
    target_table_sql = """
        create table {}APS_MID_PARMODE_LAYER_MEDIAN
        (
          parentid    VARCHAR(64) not null,
          toolg_id    VARCHAR(64),
          layer       VARCHAR(64),
          pt          VARCHAR(64),
          update_time VARCHAR(64) not null,
          partcode    VARCHAR(64) not null
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

        GetMinorChFlagData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        GetChCountData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory, in_process_db_file=in_process_db_file, target_table=target_table, current_time=current_time_short)
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, "XETL058")
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