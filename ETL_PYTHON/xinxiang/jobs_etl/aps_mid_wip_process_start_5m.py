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
    create {table_type} table APS_TMP_WIP_PROCESS_START_TOOL
    (
        tool_id  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_TMP_WIP_PROCESS_START_HIST
    (
        EQP_ID VARCHAR(64), 
        LOT_ID VARCHAR(64), 
        CTRL_JOB VARCHAR(64), 
        PROCESS_START VARCHAR(64), 
        PROCESS_END VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_WIP_PROCESS_START_WIP
        (
            EQP_ID VARCHAR(64), 
            CTRL_JOB VARCHAR(64), 
            PROCESS_START VARCHAR(64), 
            PROCESS_END VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    pass

def InsertToolTemp(duck_db, current_time, uuid, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_WIP_PROCESS_START_TOOL
    (TOOL_ID)
        SELECT DISTINCT TOOL_ID 
        FROM APS_ETL_TOOL.APS_ETL_TOOL ET 
        WHERE  ET.PROCESS_MODE <>'parallel_mode' 
    """.format(tempdb=my_duck.get_temp_table_mark())
    # duck_db.sql(sql)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_WIP_PROCESS_START_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_WIP_PROCESS_START_TOOL")

def InsertHistTemp(duck_db, current_time, uuid, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_WIP_PROCESS_START_HIST
    (EQP_ID, LOT_ID, CTRL_JOB, PROCESS_START, PROCESS_END)
    select F.EQP_ID,                                                                                                                                                                                       
            F.LOT_ID,                                                                                                   
            F.CTRL_JOB,                                                                                                 
            CASE WHEN OPE_CATEGORY='ProcessStart' THEN F.CLAIM_TIME ELSE NULL END AS PROCESS_START,                                             
            CASE WHEN OPE_CATEGORY='ProcessEnd' THEN F.CLAIM_TIME ELSE NULL END AS PROCESS_END
     from aps_mid_fhopehs_view.aps_mid_fhopehs_view f
     inner join {tempdb}APS_TMP_WIP_PROCESS_START_TOOL t 
     on t.tool_id = f.eqp_id
     where OPE_CATEGORY IN ('ProcessStart','ProcessEnd')
     and claim_time > DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '1 day' 
    """.format(tempdb=my_duck.get_temp_table_mark())
    # duck_db.sql(sql)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_WIP_PROCESS_START_HIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_WIP_PROCESS_START_HIST")

#   联合V_ETL_WIP的资讯，防止出现数据出入
def InsertWipTemp(duck_db, current_time, uuid, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_WIP_PROCESS_START_WIP
    (EQP_ID, CTRL_JOB, PROCESS_START, PROCESS_END)
    select F.EQP_ID,                                                                                                                                                                                                                                                                                          
            F.CTRL_JOB,                                                                                                 
            F.PROCESS_START,                                             
            F.PROCESS_END
    from {tempdb}APS_TMP_WIP_PROCESS_START_HIST f
    UNION ALL
    select W.EQP_ID,                                                                                                                                                                                                                                                                                          
           W.CTRL_JOB_ID AS CTRL_JOB,                                                                                                 
           W.PROCESS_START_TIME AS PROCESS_START,                                             
           NULL AS PROCESS_END
     from APS_SYNC_WIP.APS_SYNC_WIP W
     inner join {tempdb}APS_TMP_WIP_PROCESS_START_TOOL t 
     on t.tool_id = w.eqp_id
     WHERE W.PROCESS_START_TIME IS NOT NULL AND W.PROCESS_START_TIME <>''
    """.format(tempdb=my_duck.get_temp_table_mark())
    # duck_db.sql(sql)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_WIP_PROCESS_START_WIP",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_WIP_PROCESS_START_WIP")

def InsertProcessStartDataTemp(duck_db, current_time, uuid, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO APS_MID_WIP_PROCESS_START
    (PARENTID,eqp_id,ctrl_job,PROCESS_START,PROCESS_END,next_PROCESS_END,next_PROCESS_START,new_process_start,conti_flag,UPDATE_TIME,PARTCODE)
        with hist_result as (
            select max(eqp_id) as eqp_id,
                   ctrl_job,
                   max(PROCESS_START) as PROCESS_START,
                   max(PROCESS_END) as PROCESS_END
            from {tempdb}APS_TMP_WIP_PROCESS_START_WIP 
            group by ctrl_job
        ),
        next_data as (
            select eqp_id,ctrl_job,
              PROCESS_START,PROCESS_END,
              LAG(PROCESS_END,1,NULL) OVER(PARTITION BY EQP_ID ORDER BY PROCESS_START) as next_PROCESS_END,
              LAG(PROCESS_START,1,NULL) OVER(PARTITION BY EQP_ID ORDER BY PROCESS_START) as next_PROCESS_START
        from hist_result
        where PROCESS_START IS NOT NULL
        )
        select '{uuid}' AS PARENTID,
               eqp_id,ctrl_job,PROCESS_START,PROCESS_END,
               next_PROCESS_END,next_PROCESS_START,
               case when PROCESS_START > next_PROCESS_START and PROCESS_START < next_PROCESS_END and (PROCESS_END IS NULL OR PROCESS_END > next_PROCESS_END)  then next_PROCESS_END
               else 
               (case when PROCESS_START > next_PROCESS_START and PROCESS_END is null and next_PROCESS_END is null then NULL
               else 
                PROCESS_START
               end)
               end new_process_start,
               case when PROCESS_START > next_PROCESS_START and PROCESS_START < next_PROCESS_END and (PROCESS_END IS NULL OR PROCESS_END > next_PROCESS_END)  then '1'
               else 
               (case when PROCESS_START > next_PROCESS_START and PROCESS_END is null and next_PROCESS_END is null then NULL
               else 
                '0'
               end)
               end conti_flag,
               '{current_time}' AS UPDATE_TIME,
              '' AS PARTCODE
        from next_data d 
    """.format(tempdb=my_duck.get_temp_table_mark(),current_time=current_time,uuid=uuid)
    # duck_db.sql(sql)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_MID_WIP_PROCESS_START",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_MID_WIP_PROCESS_START")


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
    ETL_Proc_Name = "APS_MID_BR.APS_MID_WIP_PROCESS_START_5M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str();
    uuid = my_oracle.UUID()

    target_table = "APS_MID_WIP_PROCESS_START"
    used_table_list = ['APS_ETL_TOOL','APS_MID_FHOPEHS_VIEW','APS_SYNC_WIP']
    target_table_sql = """
        create table {}APS_MID_WIP_PROCESS_START
        (
          parentid    VARCHAR(64),
          eqp_id      VARCHAR(64),
          ctrl_job    VARCHAR(64),
          process_start VARCHAR(64),
          process_end   VARCHAR(64),
          next_process_start VARCHAR(64),
          next_process_end  VARCHAR(64),
          new_process_start  VARCHAR(64),
          conti_flag      VARCHAR(64),
          update_time VARCHAR(64),
          partcode    VARCHAR(32)
        )
    """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
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
        InsertToolTemp(duck_db_memory, current_time=current_time, uuid=uuid, oracle_conn=oracle_conn, ETL_Proc_Name=ETL_Proc_Name)
        InsertHistTemp(duck_db_memory, current_time=current_time, uuid=uuid, oracle_conn=oracle_conn, ETL_Proc_Name=ETL_Proc_Name)
        InsertWipTemp(duck_db_memory, current_time=current_time, uuid=uuid, oracle_conn=oracle_conn, ETL_Proc_Name=ETL_Proc_Name)
        InsertProcessStartDataTemp(duck_db_memory, current_time=current_time, uuid=uuid, oracle_conn=oracle_conn, ETL_Proc_Name=ETL_Proc_Name)

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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_MID_RLS_CODE_XX_ETL)
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
    print("start")
    execute()