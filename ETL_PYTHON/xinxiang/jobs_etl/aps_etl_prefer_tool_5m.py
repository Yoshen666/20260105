import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_runner


def create_temp_table(duck_db):
    '''
    这里在内存中创建使用到的临时表
    '''
    # TODO: 根据需要创建临时表
    # 例如:
    # duck_db.sql("""
    #     CREATE TABLE temp_table_name AS
    #     SELECT * FROM source_table WHERE 1=0
    # """)
    pass


def InsertPreferToolData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    """
    主要业务逻辑处理函数
    TODO: 根据具体需求修改此函数
    """
    sql = """
    INSERT INTO APS_ETL_PROD_OPE_PREFER_TOOL
    (PARENTID, PRODSPEC_ID, LAYER, STAGE, EQP_ID, RN, UPDATE_TIME)
    SELECT DISTINCT
        '{uuid}' AS PARENTID,
        PRODSPEC_ID AS PRODSPEC_ID,
        SPLIT_PART(OPE_NO, '.', 1) AS LAYER,
        SPLIT_PART(OPE_NO, '.', 2) AS STAGE,
        EQP_ID as EQP_ID,
        RN as RN,
        '{current_time}' AS UPDATE_TIME
    FROM APS_SYNC_PROD_OPE_PREFER_TOOL.APS_SYNC_PROD_OPE_PREFER_TOOL 
    """.format(uuid=uuid, current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_PROD_OPE_PREFER_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_PROD_OPE_PREFER_TOOL")


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

    # TODO: 修改以下参数
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_PROD_OPE_PREFER_TOOL_5M"  # 修改ETL名称
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    # TODO: 修改目标表名
    target_table = "APS_ETL_PROD_OPE_PREFER_TOOL"

    # TODO: 修改来源表列表 (通常是APS_SYNC_xxxx)
    used_table_list = ['APS_SYNC_PROD_OPE_PREFER_TOOL']

    # TODO: 修改目标表结构定义
    target_table_sql = """
        create table {}APS_ETL_PROD_OPE_PREFER_TOOL
        (
          PARENTID  VARCHAR(64) not null,
          PRODSPEC_ID    VARCHAR(64),
          LAYER     VARCHAR(64),
          STAGE     VARCHAR(64),
          EQP_ID    VARCHAR(64),
          RN        VARCHAR(64),
          UPDATE_TIME   VARCHAR(64)
        )
    """.format("", target_table=target_table)  # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]

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
    duck_db_memory = None

    try:
        oracle_conn = my_oracle.oracle_get_connection()
        # 开始日志
        my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)

        # -------------------------- 内存模式改成文件模式
        # 创建DuckDB
        duck_db_memory = my_duck.create_duckdb_in_file(_temp_db_path, in_process_db_file, target_table_sql)
        duck_db_memory.sql('SET threads TO 4')

        # 设置临时目录
        temp_dir = os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        duck_db_memory.execute("SET temp_directory='{}'".format(temp_dir))

        # 创建临时表
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

        # TODO: 调用业务逻辑处理函数 - 修改函数名和参数
        InsertPreferToolData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################

        # 复制到PostgreSQL (如果需要)
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            # TODO: 修改PostgreSQL相关的SQL和表定义
            select_sql_in_duck = """select parentid,
                                           PRODSPEC_ID,
                                           LAYER,
                                           STAGE,
                                           EQP_ID,
                                           RN,
                                           CASE WHEN update_time='' THEN NULL ELSE update_time END AS update_time,
                                           CASE WHEN update_time='' THEN NULL ELSE update_time END AS ver_timekey
                                     from APS_ETL_PROD_OPE_PREFER_TOOL
                                 """

            postgres_table_define = """etl_prefer_tool(parentid,PROD_ID,LAYER,STAGE,TOOL_ID,RN,update_time,ver_timekey)
                                    """

            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_prefer_tool",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                oracle_conn=oracle_conn,
                                                ETL_Proc_Name=ETL_Proc_Name)

        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                   in_process_db_file=in_process_db_file,
                                                                                   target_table=target_table,
                                                                                   current_time=current_time_short)
        duck_db_memory = None  # 已关闭

        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 加入PG执行结束的时间更新
        my_oracle.Update_PG_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 写完成日志
        my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)

    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))

        # 尝试导出结果文件（即使出错）
        if duck_db_memory is not None:
            try:
                target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                           in_process_db_file=in_process_db_file,
                                                                                           target_table=target_table,
                                                                                           current_time=current_time_short)
            except:
                pass

        # 写警告日志
        if oracle_conn is not None:
            # TODO: 修改错误代码常量
            my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                       cons_error_code.APS_ETL_WHITE_LIST_CODE_XX_ETL)
        raise e

    finally:
        if oracle_conn is not None:
            oracle_conn.commit()
            oracle_conn.close()

        # 删除临时目录
        temp_dir = os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)
        if os.path.exists(temp_dir):
            try:
                os.remove(temp_dir)
            except:
                pass

        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            try:
                os.remove(temp_db_file)
            except:
                pass

        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("ETL开始执行...")
    try:
        execute()
        print("ETL执行完成")
    except Exception as e:
        print("ETL执行失败: {e}")
        raise