import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_runner


def create_temp_table(duck_db):
    pass


def GetProdPartsSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_ETL_MERGE_CONS(                    
      PARENTID, TOOLG_ID, PROD_ID, STAGE, BATCH_ID, WIP_ATTR1, MODE, UPDATE_TIME, PARTCODE                                                        
    )                                                                          
     WITH SOURCE_DATA AS (
        SELECT DISTINCT 
               PARTS_ID,
              (SELECT STRING_AGG(DISTINCT PRODSPEC_ID,',' ORDER BY PRODSPEC_ID) 
               FROM APS_SYNC_MES_BOND_PROD_PARTS.APS_SYNC_MES_BOND_PROD_PARTS OO 
               WHERE OO.PARTS_ID = P.PARTS_ID  
               )||','|| PARTS_ID AS PRODSPEC_ID 
        FROM APS_SYNC_MES_BOND_PROD_PARTS.APS_SYNC_MES_BOND_PROD_PARTS P
    )
    SELECT  '{uuid}' AS PARENTID,
            'PX_BOND' AS TOOLG_ID,
            PRODSPEC_ID AS PROD_ID,
            '=' AS STAGE,
            '=' AS BATCH_ID,
            '<>' AS WIP_ATTR1,
            'Batch' AS MODE,
            '{current_time}' AS UPDATE_TIME,                              
            '' AS PARTCODE
    FROM SOURCE_DATA
    """.format(uuid=uuid, current_time=current_time)


    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_WAFER_START",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_WAFER_START")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_MERGE_CONS_10M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_MERGE_CONS"
    used_table_list = ['APS_SYNC_MES_BOND_PROD_PARTS']
    target_table_sql = """
        create table {}APS_ETL_MERGE_CONS
        (
          parentid    VARCHAR(64) not null,
          toolg_id    VARCHAR(512),
          tool_id    VARCHAR(512),
          prod_id      VARCHAR(512),
          layer         VARCHAR(512),
          stage         VARCHAR(512),
          plan_id         VARCHAR(512),
          step_id         VARCHAR(512),
          recipe         VARCHAR(512),
          ppid         VARCHAR(512),
          batch_id     VARCHAR(512),
          wip_attr1     VARCHAR(512),
          wip_attr2     VARCHAR(512),
          mode        VARCHAR(64),
          update_time VARCHAR(64),
          partcode    VARCHAR(64)
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
        # 获取ProdParts信息
        GetProdPartsSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select CASE WHEN parentid 	= '' THEN NULL ELSE parentid          END AS  parentid  , 
                                          CASE WHEN toolg_id 	= '' THEN NULL ELSE toolg_id 	      END AS  toolg_id 	,
                                          CASE WHEN tool_id  	= '' THEN NULL ELSE tool_id  	      END AS  tool_id  	,
                                          CASE WHEN prod_id  	= '' THEN NULL ELSE prod_id  	      END AS  prod_id  	,
                                          CASE WHEN layer    	= '' THEN NULL ELSE layer    	      END AS  layer    	,
                                          CASE WHEN stage    	= '' THEN NULL ELSE stage    	      END AS  stage    	,
                                          CASE WHEN plan_id  	= '' THEN NULL ELSE plan_id  	      END AS  plan_id  	,
                                          CASE WHEN step_id  	= '' THEN NULL ELSE step_id  	      END AS  step_id  	,
                                          CASE WHEN recipe   	= '' THEN NULL ELSE recipe   	      END AS  recipe   	,
                                          CASE WHEN ppid     	= '' THEN NULL ELSE ppid     	      END AS  ppid     	,
                                          CASE WHEN batch_id 	= '' THEN NULL ELSE batch_id 	      END AS  batch_id 	,
                                          CASE WHEN wip_attr1   = '' THEN NULL ELSE wip_attr1         END AS  wip_attr1 , 
                                          CASE WHEN wip_attr2   = '' THEN NULL ELSE wip_attr2         END AS  wip_attr2 , 
                                          CASE WHEN mode        = '' THEN NULL ELSE mode              END AS  mode      , 
                                          CASE WHEN update_time = '' THEN NULL ELSE update_time       END AS  update_time
                                     from APS_ETL_MERGE_CONS
                                 """
            postgres_table_define = """etl_merge_cons(parentid,toolg_id,tool_id,prod_id,layer,stage,plan_id,step_id,
                                        recipe,ppid,batch_id,wip_attr1,wip_attr2,mode,update_time)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_merge_cons",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                oracle_conn=oracle_conn,
                                                ETL_Proc_Name=ETL_Proc_Name)
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                   in_process_db_file=in_process_db_file,
                                                                                   target_table=target_table,
                                                                                   current_time=current_time_short)
        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 加入PG执行结束的时间更新
        my_oracle.Update_PG_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_ETL_MERGE_CONS_CODE_XX_ETL)
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