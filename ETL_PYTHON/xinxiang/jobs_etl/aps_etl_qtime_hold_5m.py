import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_runner


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''
    sql = """
    create table APS_TMP_ETL_QTIME_HOLD_FLOW
    (
        prod_id VARCHAR(60),
        plan_id VARCHAR(60),
        ope_no  VARCHAR(60),
        step_id VARCHAR(60)
    )
    """
    duck_db.sql(sql)

def biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT  /*+ append */  INTO APS_ETL_QTIME_HOLD(                      
             PARENTID, PROD_ID, PLAN_ID, STEP_ID, LAYER, STAGE, TOOL_ID, CH_ID,
            USED_TC, VIRTUAL_TIME_TC, HOLD_FLAG, UPDATE_TIME, PARTCODE                                                              
         )                                                                            
          SELECT '{uuid}'  AS PARENTID,                                   
                 E.PROD_ID,                                                           
                 E.MAINPD_ID AS PLAN_ID,                                              
                 F.STEP_ID,                                                           
                 --SUBSTR(E.NEXT_OPE_NO, 1, INSTR(E.NEXT_OPE_NO,'.',-1)-1) AS LAYER,   
                 SUBSTRING(E.NEXT_OPE_NO, 1, POSITION('.' IN E.NEXT_OPE_NO)-1) AS LAYER,
                 --SUBSTR(E.NEXT_OPE_NO, INSTR(E.NEXT_OPE_NO,'.',-1)+1) AS STAGE, 
                  SUBSTRING(E.NEXT_OPE_NO, POSITION('.' IN E.NEXT_OPE_NO)+1) AS STAGE,      
                 CASE WHEN E.EQP_ID NOT LIKE '%.%' THEN E.EQP_ID                      
                 ELSE SUBSTRING(E.EQP_ID, 1, POSITION('.' IN E.EQP_ID)-1) END AS TOOL_ID,   
                 E.EQP_ID AS CH_ID,                                                   
                 E.USED_TC,                                                           
                 E.VIRTUAL_TIME_TC,                                                   
                 E.HOLD_FLAG,                                                         
                 '{current_time}' AS UPDATE_TIME,                               
                 '' AS PARTCODE                    
          FROM APS_SYNC_RTD_QTIME_PROD_EQP.APS_SYNC_RTD_QTIME_PROD_EQP E                                                 
          INNER JOIN {tempdb}APS_TMP_ETL_QTIME_HOLD_FLOW F                                       
          ON F.PLAN_ID = E.MAINPD_ID  AND F.PROD_ID = E.PROD_ID                       
          AND F.OPE_NO = E.NEXT_OPE_NO                                                                              
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_QTIME_HOLD",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_QTIME_HOLD")


def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
      INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_QTIME_HOLD_FLOW(                
        PROD_ID, PLAN_ID, OPE_NO, STEP_ID                                                            
     )                                                                         
      SELECT PROD_ID,PLAN_ID,                                                  
             OPE_NO,STEP_ID                                                    
      FROM APS_ETL_FLOW.APS_ETL_FLOW                                                                                                         
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_QTIME_HOLD_FLOW",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_QTIME_HOLD_FLOW")

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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_QTIME_HOLD_5M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_QTIME_HOLD"
    used_table_list = ['APS_ETL_FLOW',
                       'APS_SYNC_RTD_QTIME_PROD_EQP'
                       ]
    target_table_sql = """
        create table {}APS_ETL_QTIME_HOLD
        (
            parentid        VARCHAR(64) not null,
            prod_id         VARCHAR(64) not null,
            plan_id         VARCHAR(64) not null,
            step_id         VARCHAR(64) not null,
            layer           VARCHAR(64) not null,
            stage           VARCHAR(64) not null,
            tool_id         VARCHAR(64) not null,
            ch_id           VARCHAR(64) not null,
            used_tc         VARCHAR(64),
            virtual_time_tc VARCHAR(64),
            hold_flag       VARCHAR(64),
            update_time     VARCHAR(64),
            partcode        VARCHAR(64) not null,
            PRIMARY KEY (PARENTID, PROD_ID, PLAN_ID, STEP_ID, LAYER, STAGE, TOOL_ID, CH_ID, PARTCODE)
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

        row_count = my_duck.get_row_count_in_duckdb(duck_db=duck_db_memory, tableName="{tempdb}APS_TMP_ETL_QTIME_HOLD_FLOW".format(tempdb=my_duck.get_temp_table_mark()))
        if row_count == 0:
            my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time,
                                       "APS_TMP_ETL_QTIME_HOLD_FLOW没有数据，请及时确认一下！！")
            return

        biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select parentid,prod_id, plan_id,step_id,layer,stage,tool_id,
                                           ch_id, case when used_tc = '' then NULL ELSE used_tc END AS used_tc,
                                           case when virtual_time_tc = '' then NULL ELSE virtual_time_tc END AS virtual_time_tc, 
                                            case when hold_flag = '' then NULL ELSE hold_flag END AS hold_flag, 
                                            case when update_time = '' then NULL ELSE update_time END AS update_time from APS_ETL_QTIME_HOLD
                                 """
            postgres_table_define = """etl_qtime_hold(parentid,prod_id, plan_id,step_id,layer,stage,tool_id,
                                                      ch_id,used_tc,virtual_time_tc, hold_flag, update_time)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_qtime_hold",  # 要小写
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
                                   cons_error_code.APS_ETL_QTIME_HOLD_CODE_XX_ETL)
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
