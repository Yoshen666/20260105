import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_file, my_runner

import pandas as pd

from xinxiang.util.oracle_to_duck_his import delete_over_three_version


def create_temp_table(duck_db):
    pass

def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT /*+ append */
            INTO APS_ETL_MASK_HISTORY
        (RETICLE_GROUP,
        RETICLE_ID,
        STATUS,
        POSITION_FROM,
        POSITION,
        EVENT,
        EVENT_TIME,
        UPDATE_TIME)
    SELECT MAX(MH.MASK_GROUP) AS RETICLE_GROUP,
         MH.RTCL_ID AS RETICLE_ID,
         (CASE
           WHEN MAX(MH.STATUS) IN
                ('STABLE', 'PASS', 'WAIT', 'HOLD', 'FREEZE') 
           THEN
            'ACTIVE'
           ELSE
            MAX(MH.STATUS)
         END) AS STATUS,
         MAX(MH.LOC_ID) AS POSITION_FROM,
         MAX(MH.NEXT_LOC_ID) AS POSITION,
         --NVL(DECODE(MAX(MH.LOC_STATE),
                    --'I',
                   -- 'Load',
                  --  'O',
                   -- 'Unload',
                 --   MAX(MH.LOC_STATE)),
             --'Load') AS EVENT,
         COALESCE((CASE WHEN MAX(MH.LOC_STATE) ='I'
         THEN 'Load'
         WHEN MAX(MH.LOC_STATE) ='O' 
         THEN 'Unload'
         ELSE MAX(MH.LOC_STATE) END ),'Load') AS EVENT,     
        --TO_CHAR(MH.TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS EVENT_TIME,
        MH.TIMESTAMP AS EVENT_TIME,  
        CAST('{current_time}' AS TIMESTAMP) AS UPDATE_TIME
    FROM APS_TMP_MASK_HISTORY_VIEW.APS_TMP_MASK_HISTORY_VIEW MH
    WHERE MH.FUNC_ID IN ('F_TRACKIN', 'F_TRACKOUT')
    GROUP BY MH.RTCL_ID, MH.TIMESTAMP
    """.format(current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_MASK_HISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_MASK_HISTORY")


def biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT /*+ append */
            INTO APS_ETL_MASK_HISTORY
        (RETICLE_GROUP,
        RETICLE_ID,
        STATUS,
        POSITION_FROM,
        POSITION,
        EVENT,
        EVENT_TIME,
        UPDATE_TIME)
    SELECT MAX(MH.MASK_GROUP) AS RETICLE_GROUP,
         MH.RTCL_ID AS RETICLE_ID,
         (CASE
           WHEN MAX(MH.STATUS) IN
                ('STABLE', 'PASS', 'WAIT', 'HOLD', 'FREEZE') 
           THEN
            'ACTIVE'
           ELSE
            MAX(MH.STATUS)
         END) AS STATUS,
         MAX(MH.LOC_ID) AS POSITION_FROM,
         MAX(MH.NEXT_LOC_ID) AS POSITION,
         --NVL(DECODE(MAX(MH.LOC_STATE),
                    --'I',
                   -- 'Load',
                  --  'O',
                   -- 'Unload',
                 --   MAX(MH.LOC_STATE)),
             --'Load') AS EVENT,
         COALESCE((CASE WHEN MAX(MH.LOC_STATE) ='I'
         THEN 'Load'
         WHEN MAX(MH.LOC_STATE) ='O' 
         THEN 'Unload'
         ELSE MAX(MH.LOC_STATE) END ),'Load') AS EVENT,     
        --TO_CHAR(MH.TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS EVENT_TIME,
        MH.TIMESTAMP AS EVENT_TIME,  
        CAST('{current_time}' AS TIMESTAMP) AS UPDATE_TIME
    FROM APS_TMP_MASK_HISTORY_VIEW.APS_TMP_MASK_HISTORY_VIEW MH
    WHERE MH.FUNC_ID IN ('F_TRACKIN', 'F_TRACKOUT')
    AND NOT EXISTS (SELECT H.RETICLE_ID  FROM LAST_APS_ETL_MASK_HISTORY.APS_ETL_MASK_HISTORY H WHERE H.RETICLE_ID = MH.RTCL_ID 
    AND CAST(H.EVENT_TIME AS TIMESTAMP) = CAST(MH.TIMESTAMP AS TIMESTAMP))
    GROUP BY MH.RTCL_ID, MH.TIMESTAMP
    """.format(current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_MASK_HISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_MASK_HISTORY")

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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_MASK_HISTORY_60M"
    current_time = my_date.date_time_second_str()
    current_time_min = my_date.date_time_min_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_MASK_HISTORY"
    used_table_list = ['APS_TMP_MASK_HISTORY_VIEW']
    target_table_sql = """
        create table {}APS_ETL_MASK_HISTORY
        (
            reticle_group VARCHAR(60),
            reticle_id    VARCHAR(60) not null,
            status        VARCHAR(60) not null,
            position_from VARCHAR(60),
            position      VARCHAR(60) not null,
            event         VARCHAR(20) not null,
            event_time    TIMESTAMP  not null,
            update_time   TIMESTAMP not null,
            PRIMARY KEY (RETICLE_ID, EVENT_TIME)
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
        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_ETL_MASK_HISTORY")
        if file_name is None:
            biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_ETL_MASK_HISTORY",
                                 target_db_file=file_name)
            biz_method_02(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            last_table_name = 'LAST_APS_ETL_MASK_HISTORY.APS_ETL_MASK_HISTORY'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_ETL_MASK_HISTORY DATA IS NULL")
                return
            sql = """insert into APS_ETL_MASK_HISTORY select * from {last_table_name}
                     WHERE CAST(EVENT_TIME AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '60 day'""".format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)
            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_ETL_MASK_HISTORY")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_ETL_MASK_HISTORY DATA IS NULL")
            return
        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            pg_conn = my_postgres.decide_postgres_db_connection(export_to_pg_prod_test=False,
                                                                pg_table_name="etl_mask_history")

            sql_key_in_pg = """
                SELECT DISTINCT RETICLE_ID, EVENT_TIME
                FROM yth.etl_mask_history
                where event_time >= timestamp '{current_time}' - INTERVAL '1 day'
                ORDER BY RETICLE_ID ASC, EVENT_TIME ASC
            """.format(current_time=current_time_min)
            pg_exist_result = pd.read_sql(sql_key_in_pg, pg_conn)
            print(pg_exist_result)

            select_sql_in_duck = """select 
                        CASE WHEN reticle_group='' THEN NULL ELSE reticle_group END AS reticle_group,
                        CASE WHEN reticle_id='' THEN NULL ELSE reticle_id END AS reticle_id,
                        CASE WHEN status='' THEN NULL ELSE status END AS status,
                        CASE WHEN position_from='' THEN NULL ELSE position_from END AS position_from,
                        CASE WHEN position='' THEN NULL ELSE position END AS position,
                        CASE WHEN event='' THEN NULL ELSE event END AS event,
                        CASE WHEN event_time='' THEN NULL ELSE event_time END AS event_time,
                        CASE WHEN update_time='' THEN NULL ELSE update_time END AS update_time
                    FROM APS_ETL_MASK_HISTORY t
                    WHERE event_time >= DATE_TRUNC('second', CAST('{current_time}' AS TIMESTAMP)) - INTERVAL '1 day'
                    AND NOT EXISTS(
                        select 1 
                        from pg_exist_result pd
                        where t.reticle_id = pd.reticle_id
                        and   t.event_time = pd.event_time
                    )
                """.format(current_time=current_time_min)
            postgres_table_define = """etl_mask_history(reticle_group,reticle_id,status,position_from,position,event,event_time,update_time)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_mask_history",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                copy_to_yto=False,
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
        # 嘗試導出到目標文件中
        try:
            my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                      in_process_db_file=in_process_db_file,
                                                                      target_table=target_table,
                                                                      current_time=current_time_short)
        except Exception as export_err:
            logging.warning("清理 DuckDB 資源失敗: {export_err}".format(export_err=export_err))
        # 嘗試寫警告日志
        try:
            my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                       cons_error_code.APS_ETL_MASK_HISTORY_CODE_XX_ETL)
        except Exception as log_err:
            logging.warning("寫入錯誤日誌失敗: {log_err}".format(log_err=log_err))
        raise e
    finally:
        delete_over_three_version(table_name=target_table)
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