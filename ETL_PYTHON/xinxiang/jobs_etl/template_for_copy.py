import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons


def create_temp_table(duck_db):
    '''
    这里在内存中创建使用到的临时表
    '''
    # sql = """
    # create table APS_TMP_ETL_SGS_RLS_FLOW
    # (
    #   prod_id  VARCHAR(64),
    #   plan_id  VARCHAR(64),
    #   ope_no   VARCHAR(64),
    #   layer    VARCHAR(64),
    #   stage    VARCHAR(64),
    #   step_id  VARCHAR(64),
    #   toolg_id VARCHAR(64)
    # )
    # """
    # duck_db.sql(sql) TODO 改成你自己的逻辑
    pass


def biz_method_01(duck_db_memory, uuid, current_time):
    # sql = """
    # INSERT  /*+ append */  INTO APS_ETL_SGS_RLS(
    # PARENTID, TOOLG_ID, PROD_ID, PLAN_ID, STEP_ID, LAYER, STAGE, TOOL_ID,
    # CREATE_TIME, PASS_FLG_PRC, PRC_UPDATE_TIME, PASS_FLG_MFG, MFG_UPDATE_TIME, UPDATE_TIME, PARTCODE
    # )
    # SELECT
    #     '{uuid}' AS PARENTID,
    #     F.TOOLG_ID,
    #     C.PROD_ID,
    #     C.PLAN_ID || '.%',
    #     F.STEP_ID,
    #     F.LAYER,
    #     F.STAGE,
    #     C.TOOL_ID,
    #     C.CREATE_TIME,
    #     C.PASS_FLG_PRC,
    #     C.PRC_UPDATE_TIME,
    #     C.PASS_FLG_MFG,
    #     C.MFG_UPDATE_TIME,
    #     '{current_time}' AS UPDATE_TIME,
    #     '{part_code}' AS PARTCODE
    # FROM APS_TMP_ETL_SGS_RLS_CSFR C
    # INNER JOIN APS_TMP_ETL_SGS_RLS_FLOW F
    # ON F.PROD_ID = C.PROD_ID
    # AND SUBSTRING(F.PLAN_ID, 1, POSITION('.' IN F.PLAN_ID)-1) = C.PLAN_ID
    # AND F.OPE_NO = C.OPE_NO
    # """.format(uuid=uuid, current_time=current_time, part_code=cons.RLS_PART_VALUE_NEW)
    #
    # ### AND SUBSTR(F.PLAN_ID, 1, INSTR(F.PLAN_ID,'.',-1)-1) = C.PLAN_ID
    # duck_db_memory.sql(sql) TODO 改成你自己的逻辑
    pass


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_SGS_RLS_60M" # TODO 改成你自己的逻辑
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_SGS_RLS" # TODO 改成你自己的逻辑
    used_table_list = ['APS_ETL_FLOW', 'APS_TR_CSFRINHIBIT'] # TODO 改成你自己的逻辑
    target_table_sql = """
        create table {}APS_ETL_SGS_RLS
        (
          parentid        VARCHAR(64) not null,
          toolg_id        VARCHAR(64) not null,
          prod_id         VARCHAR(64) not null,
          plan_id         VARCHAR(64) not null,
          step_id         VARCHAR(64) not null,
          layer           VARCHAR(64) not null,
          stage           VARCHAR(64) not null,
          tool_id         VARCHAR(64) not null,
          create_time     VARCHAR(64) not null,
          pass_flg_prc    VARCHAR(64) not null,
          prc_update_time VARCHAR(64) not null,
          pass_flg_mfg    VARCHAR(64) not null,
          mfg_update_time VARCHAR(64) not null,
          update_time     VARCHAR(64) not null,
          partcode        VARCHAR(64) not null
        )
    """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名] TODO 改成你自己的逻辑
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

        # TODO 改成你自己的逻辑

        biz_method_01(duck_db_memory, uuid, current_time)

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
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, cons_error_code.APS_ETL_SGS_RLS_CODE_XX_ETL) # TODO 改成你自己的CODE
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 导出到目标文件中
        my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                  in_process_db_file=in_process_db_file,
                                                                  target_table=target_table,
                                                                  current_time=current_time_short)
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