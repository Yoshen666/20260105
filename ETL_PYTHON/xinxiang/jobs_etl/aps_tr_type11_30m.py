import gc
import logging
import os
import shutil

import duckdb

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, my_file


def create_temp_table(duck_db):
    pass


def GetType11FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_TR_TYPE11 (                           
        GUID, PROD_ID, PROD_TECH, OPE_NO, TOOLG_ID, ARRIVAL, TRACK_IN, TRACK_OUT, PARTKEY, BANK_TIME
    )                                                                                    
     WITH TOOLG_DATA AS (                                                                                        
          SELECT EQP_ID, MAX(EQP_G) AS TOOLG_ID                                          
          FROM  APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL                                      
          GROUP BY EQP_ID                                                                
     ),                                                                                  
     PROCESS_START_DATA AS (                                                                                     
          SELECT LOT_ID,CTRL_JOB,MAX(CLAIM_TIME) AS CLAIM_TIME                           
          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW                                  
          WHERE 1=1                                                                      
          AND OPE_CATEGORY = 'OperationStart'                                              
          AND CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '35 day'
          GROUP BY LOT_ID,CTRL_JOB                                                       
     ),                                                                                  
     TYPE11 AS (                                                                         
          SELECT                                                                         
                LH.PRODSPEC_ID AS PROD_ID,
                P.GROUP_TECH AS PROD_TECH,                                               
                LH.OPE_NO,                                                               
                TD.TOOLG_ID,                                                             
                LH.PREV_OP_COMP_TIME,      
                --modify by xiecheng for version up                                              
                --SD.CLAIM_TIME AS OP_START_DATE_TIME, 
                COALESCE(SD.CLAIM_TIME, LH.OP_START_DATE_TIME) AS OP_START_DATE_TIME,                                     
                LH.CLAIM_TIME,                                                           
                -- TRUNC(LH.BANK_PERIOD_TIME*24,2) AS BANK_TIME
                CAST(CAST(LH.BANK_PERIOD_TIME AS DECIMAL)*24 AS DECIMAL(18, 2)) AS BANK_TIME
          FROM  APS_TMP_LOTHISTORY_VIEW.APS_TMP_LOTHISTORY_VIEW LH                            
          INNER JOIN TOOLG_DATA TD                                                       
          ON TD.EQP_ID = LH.EQP_ID  
          LEFT JOIN APS_SYNC_PRODUCT.APS_SYNC_PRODUCT P 
          ON P.PRODSPEC_ID = LH.PRODSPEC_ID
          LEFT JOIN PROCESS_START_DATA SD                                              
          ON SD.LOT_ID = LH.LOT_ID AND SD.CTRL_JOB = LH.CTRL_JOB                         
          WHERE  LH.OPE_CATEGORY='OperationComplete'                                     
          AND LH.CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '30 day'
          AND LH.OP_START_DATE_TIME is not null 
     )                                                                                   
     SELECT  '{uuid}' AS GUID,                                                         
             PROD_ID, 
             PROD_TECH,                                                                   
             OPE_NO,                                                                     
             TOOLG_ID,                                                                   
             -- TO_CHAR(PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS') AS ARRIVAL,
             PREV_OP_COMP_TIME AS ARRIVAL,
             -- TO_CHAR(OP_START_DATE_TIME,'YYYY-MM-DD HH24:MI:SS') AS TRACK_IN,
             OP_START_DATE_TIME AS TRACK_IN,
             -- TO_CHAR(CLAIM_TIME,'YYYY-MM-DD HH24:MI:SS') AS TRACK_OUT,
             CLAIM_TIME AS TRACK_OUT,
             OP_START_DATE_TIME AS PARTKEY,                                              
             BANK_TIME                                                                   
     FROM  TYPE11 T                                                                                                                                                   
    """.format(uuid=uuid)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE11",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE11")


def GetType11OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_TR_TYPE11 (                           
        GUID, PROD_ID, PROD_TECH, OPE_NO, TOOLG_ID, ARRIVAL, TRACK_IN, TRACK_OUT, PARTKEY, BANK_TIME
    )                                                                                    
     WITH TOOLG_DATA AS (                                                                                        
          SELECT EQP_ID, MAX(EQP_G) AS TOOLG_ID                                          
          FROM  APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL                                      
          GROUP BY EQP_ID                                                                
     ),                                                                                  
     PROCESS_START_DATA AS (                                                                                     
          SELECT LOT_ID,CTRL_JOB,MAX(CLAIM_TIME) AS CLAIM_TIME                           
          FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW                                  
          WHERE 1=1                                                                      
          AND OPE_CATEGORY = 'OperationStart'                                              
          AND CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '35 day'
          GROUP BY LOT_ID,CTRL_JOB                                                       
     ),                                                                                  
     TYPE11 AS (                                                                         
          SELECT                                                                         
                LH.PRODSPEC_ID AS PROD_ID,  
                P.GROUP_TECH AS PROD_TECH,                                              
                LH.OPE_NO,                                                               
                TD.TOOLG_ID,                                                             
                LH.PREV_OP_COMP_TIME,      
                --modify by xiecheng for version up                                              
                --SD.CLAIM_TIME AS OP_START_DATE_TIME, 
                COALESCE(SD.CLAIM_TIME, LH.OP_START_DATE_TIME) AS OP_START_DATE_TIME,                                     
                LH.CLAIM_TIME,                                                           
                -- TRUNC(LH.BANK_PERIOD_TIME*24,2) AS BANK_TIME
                CAST(CAST(LH.BANK_PERIOD_TIME AS DECIMAL)*24 AS DECIMAL(18, 2)) AS BANK_TIME
          FROM  APS_TMP_LOTHISTORY_VIEW.APS_TMP_LOTHISTORY_VIEW LH                            
          INNER JOIN TOOLG_DATA TD                                                       
          ON TD.EQP_ID = LH.EQP_ID  
          LEFT JOIN APS_SYNC_PRODUCT.APS_SYNC_PRODUCT P 
          ON P.PRODSPEC_ID = LH.PRODSPEC_ID 
          LEFT JOIN PROCESS_START_DATA SD                                              
          ON SD.LOT_ID = LH.LOT_ID AND SD.CTRL_JOB = LH.CTRL_JOB                         
          WHERE  LH.OPE_CATEGORY='OperationComplete'                                     
          AND LH.CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '30 day'
          AND LH.OP_START_DATE_TIME is not null 
     )                                                                                   
     SELECT  '{uuid}' AS GUID,                                                         
             PROD_ID,
             PROD_TECH,                                                                    
             OPE_NO,                                                                     
             TOOLG_ID,                                                                   
             -- TO_CHAR(PREV_OP_COMP_TIME,'YYYY-MM-DD HH24:MI:SS') AS ARRIVAL,
             PREV_OP_COMP_TIME AS ARRIVAL,
             -- TO_CHAR(OP_START_DATE_TIME,'YYYY-MM-DD HH24:MI:SS') AS TRACK_IN,
             OP_START_DATE_TIME AS TRACK_IN,
             -- TO_CHAR(CLAIM_TIME,'YYYY-MM-DD HH24:MI:SS') AS TRACK_OUT,
             CLAIM_TIME AS TRACK_OUT,
             OP_START_DATE_TIME AS PARTKEY,                                              
             BANK_TIME                                                                   
     FROM  TYPE11 T                                                                      
      WHERE                                                                               
         NOT EXISTS (                                                                     
              SELECT 1  FROM LAST_APS_TR_TYPE11.APS_TR_TYPE11 T11
               WHERE T11.PROD_ID = T.PROD_ID
                    AND T11.PROD_TECH = T.PROD_TECH        
                    AND T11.OPE_NO = T.OPE_NO 
                    AND T11.TOOLG_ID = T.TOOLG_ID                     
                    AND T11.TRACK_IN =  T.OP_START_DATE_TIME                                        
                   AND T11.ARRIVAL = T.PREV_OP_COMP_TIME        
                    AND T11.TRACK_OUT = T.CLAIM_TIME        
        )                                                                                
    """.format(uuid=uuid)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE11",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE11")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_TR_TYPE11_30M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_TR_TYPE11"
    used_table_list = ['APS_SYNC_ETL_TOOL', 'APS_MID_FHOPEHS_VIEW', 'APS_TMP_LOTHISTORY_VIEW','APS_SYNC_PRODUCT']
    target_table_sql = """
        create table {}APS_TR_TYPE11
        (
          prod_id   VARCHAR(64) not null,
          PROD_TECH VARCHAR(64),
          toolg_id  VARCHAR(64) not null,
          recipe    VARCHAR(64),
          arrival   VARCHAR(64),
          track_in  VARCHAR(64) not null,
          track_out VARCHAR(64),
          guid      VARCHAR(64) not null,
          partkey   VARCHAR(64) not null,
          ope_no    VARCHAR(64) not null,
          bank_time decimal
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
        # 取上一次的APS_TR_TYPE11版本
        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_TR_TYPE11")
        # if 取不到
        if file_name is None:
            GetType11FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # else 取到的情况下
        # 手动 Attach APS_TR_TYPE11 AS LAST_APS_TR_TYPE11
        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_TR_TYPE11",
                                 target_db_file=file_name)

            GetType11OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            last_table_name = 'LAST_APS_TR_TYPE11.APS_TR_TYPE11'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_MID_PH_LOTHISTORY DATA IS NULL")
                return
            sql = """insert into APS_TR_TYPE11 select * from {last_table_name}
                     WHERE CAST(partkey AS TIMESTAMP) > DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '40 day'""".format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)

            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_TR_TYPE11")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_TR_TYPE11 DATA IS NULL")
            return
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, 'XETL033')
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