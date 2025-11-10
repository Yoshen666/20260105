import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, my_file


def create_temp_table(duck_db):
    pass


def GetType3FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO  APS_TR_TYPE3 (
         GUID, MODULE_ID, TOOLG_ID, TOOL_ID, PPID, QTY, PROCESS_START, PROCESS_END, PARTKEY
    )
     WITH TOOL AS (
           SELECT EQP_ID AS TOOL_ID,MAX(EQP_G) AS TOOLG_ID,
                  MAX(REAL_MODULE) AS MODULE
           FROM  APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL
           WHERE EQP_CATEGORY <> 'Measurement'
           GROUP BY EQP_ID
     )
     SELECT '{uuid}' AS GUID,
            TL.MODULE,
            TL.TOOLG_ID,
            SP.EQPID AS TOOL_ID,
            SP.CHAMBER AS PPID,
            '' AS QTY,
            STRFTIME(SP.START_TIME, '%Y-%m-%d %H:%M:%S') AS PROCESS_START,
            STRFTIME(SP.END_TIME, '%Y-%m-%d %H:%M:%S') AS PROCESS_END,
            SP.START_TIME AS PARTKEY
       FROM APS_SYNC_IDLE_DUMMY_SPEC.APS_SYNC_IDLE_DUMMY_SPEC SP
     INNER JOIN TOOL TL ON TL.TOOL_ID = SP.EQPID
     WHERE SP.START_TIME IS NOT NULL
     AND SP.END_TIME >= SP.START_TIME
     AND SP.END_TIME BETWEEN DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '30 day'
     AND DATE_TRUNC('second', CURRENT_TIMESTAMP) + INTERVAL '1 day'
    """.format(uuid=uuid)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE3",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE3")

def GetType3OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO  APS_TR_TYPE3 (
         GUID, MODULE_ID, TOOLG_ID, TOOL_ID, PPID, QTY, PROCESS_START, PROCESS_END, PARTKEY
    )
     WITH TOOL AS (
           SELECT EQP_ID AS TOOL_ID,MAX(EQP_G) AS TOOLG_ID,
                  MAX(REAL_MODULE) AS MODULE
           FROM  APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL
           WHERE EQP_CATEGORY <> 'Measurement'
           GROUP BY EQP_ID
     )
     SELECT '{uuid}' AS GUID,
            TL.MODULE,
            TL.TOOLG_ID,
            SP.EQPID AS TOOL_ID,
            SP.CHAMBER AS PPID,
            '' AS QTY,
            STRFTIME(SP.START_TIME, '%Y-%m-%d %H:%M:%S') AS PROCESS_START,
            STRFTIME(SP.END_TIME, '%Y-%m-%d %H:%M:%S') AS PROCESS_END,
            SP.START_TIME AS PARTKEY
       FROM APS_SYNC_IDLE_DUMMY_SPEC.APS_SYNC_IDLE_DUMMY_SPEC SP
     INNER JOIN TOOL TL ON TL.TOOL_ID = SP.EQPID
     WHERE SP.START_TIME IS NOT NULL
     AND SP.END_TIME >= SP.START_TIME
     AND SP.END_TIME > (
         SELECT STRPTIME(PROCESS_END, '%Y-%m-%d %H:%M:%S')
         FROM
          (
            SELECT PROCESS_END,
             ROW_NUMBER() OVER ( ORDER BY PROCESS_END DESC) AS ROW_NUMBER
            FROM LAST_APS_TR_TYPE3.APS_TR_TYPE3
              ) TEMP
                WHERE TEMP.ROW_NUMBER = '1'
        )
    """.format(uuid=uuid)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE3_OTHER",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE3")

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
    ETL_Proc_Name = "APS_TR_BR.APS_TR_TYPE3_5M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str();
    uuid = my_oracle.UUID()

    target_table = "APS_TR_TYPE3"
    used_table_list = ['APS_SYNC_ETL_TOOL', 'APS_SYNC_IDLE_DUMMY_SPEC']
    target_table_sql = """
        create table {}APS_TR_TYPE3
        (
          module_id     VARCHAR(60),
          toolg_id      VARCHAR(60),
          tool_id       VARCHAR(60),
          ppid          VARCHAR(60),
          qty           VARCHAR(60),
          process_start VARCHAR(60),
          process_end   VARCHAR(60),
          guid          VARCHAR(60) not null,
          partkey       VARCHAR(60)
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

        # 取上一次的APS_TR_TYPE3版本
        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_TR_TYPE3")
        # if 取不到
        if file_name is None:
            GetType3FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # else 取到的情况下
        # 手动 Attach APS_TR_TYPE3 AS LAST_APS_TR_TYPE3
        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_TR_TYPE3",
                                 target_db_file=file_name)

            GetType3OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            last_table_name = 'LAST_APS_TR_TYPE3.APS_TR_TYPE3'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_TR_TYPE3 DATA IS NULL")
                return
            sql = """insert into APS_TR_TYPE3 select * from {last_table_name}
                     WHERE CAST(partkey AS TIMESTAMP) > DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '40 day'""".format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)

            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_TR_TYPE3")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_TR_TYPE3 DATA IS NULL")
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, 'XETL032')
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