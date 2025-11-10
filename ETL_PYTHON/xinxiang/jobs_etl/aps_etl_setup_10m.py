import gc
import logging
import os
import shutil

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, my_postgres, my_runner


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''

    sql = """
    create {table_type} table APS_TMP_ETL_SETUP_BASE_DATA
    (
        prod_id                      VARCHAR(60) not null,
        prodg_id                     VARCHAR(60) not null,
        plan_id                      VARCHAR(60) not null,
        layer_stage                  VARCHAR(60) not null,
        tool_id                      VARCHAR(60) not null,
        current_recipe_code          VARCHAR(60),
        current_recipe_max_quantity  VARCHAR(60),
        current_recipe_quantity_type VARCHAR(60),
        later_recipe_code            VARCHAR(60),
        config_info                  VARCHAR(4000),
        remark                       VARCHAR(500),
        base_rule_group              INTEGER
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_TMP_ETL_SETUP_JSON_PARSED
    (
        prod_id                      VARCHAR(60) not null,
        prodg_id                     VARCHAR(60) not null,
        plan_id                      VARCHAR(60) not null,
        layer_stage                  VARCHAR(60) not null,
        tool_id                      VARCHAR(60) not null,
        current_recipe_code          VARCHAR(60),
        current_recipe_max_quantity  VARCHAR(60),
        current_recipe_quantity_type VARCHAR(60),
        later_recipe_code            VARCHAR(60),
        config_info                  VARCHAR(4000),
        remark                       VARCHAR(500),
        base_rule_group              INTEGER,
        is_product_wildcard          INTEGER,
        min_quantity                 VARCHAR(60),
        min_virtual_value            VARCHAR(60),
        first_product_key            VARCHAR(60)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)


def getBaseDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT /*+ append */ INTO {tempdb}APS_TMP_ETL_SETUP_BASE_DATA (
        prod_id, prodg_id, plan_id, layer_stage, tool_id,
        current_recipe_code, current_recipe_max_quantity, current_recipe_quantity_type,
        later_recipe_code, config_info, remark, base_rule_group
    )
    SELECT 
        REPLACE(FUNCTION_PRODSPEC_ID, '*', '%') as prod_id,
        REPLACE(FUNCTION_PRODG, '*', '%') as prodg_id,
        REPLACE(FUNCTION_FLOW_KEY, '*', '%') as plan_id,
        REPLACE(FUNCTION_OPE_NO, '*', '%') as layer_stage,
        REPLACE(FUNCTION_EQP_ID, '*', '%') as tool_id,
        CURRENT_RECIPE_CODE,
        CURRENT_RECIPE_MAX_QUANTITY,
        CURRENT_RECIPE_QUANTITY_TYPE,
        LATER_RECIPE_CODE,
        CONFIG_INFO,
        REMARK,
        ROW_NUMBER() OVER (ORDER BY FUNCTION_PRODSPEC_ID, FUNCTION_EQP_ID) * 2 - 1 as base_rule_group
    FROM APS_SYNC_RTD_SPECIAL_RECIPE_CHANGE_CONFIG.APS_SYNC_RTD_SPECIAL_RECIPE_CHANGE_CONFIG
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_SETUP_BASE_DATA",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_SETUP_BASE_DATA")


def getJsonParsedSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # 步驟1: 先展開所有 JSON keys 到臨時表
    sql_expand = """
    CREATE TEMP TABLE IF NOT EXISTS temp_json_keys AS
    SELECT 
        bd.prod_id,
        bd.prodg_id,
        bd.plan_id,
        bd.layer_stage,
        bd.tool_id,
        bd.current_recipe_code,
        bd.current_recipe_max_quantity,
        bd.current_recipe_quantity_type,
        bd.later_recipe_code,
        bd.config_info,
        bd.remark,
        bd.base_rule_group,
        CASE 
            WHEN json_extract_string(bd.config_info, '$.product') LIKE '%"*"%' THEN 1
            ELSE 0
        END as is_product_wildcard,
        json_extract_string(bd.config_info, '$.product.*.min_quantity') as wildcard_min_quantity,
        json_extract_string(bd.config_info, '$.product.*.min_virtual_value') as wildcard_min_virtual_value,
        json_keys(json_extract(bd.config_info, '$.product')) as all_keys,
        json_extract(bd.config_info, '$.product') as product_json
    FROM {tempdb}APS_TMP_ETL_SETUP_BASE_DATA bd
    """.format(tempdb=my_duck.get_temp_table_mark())

    duck_db_memory.sql(sql_expand)

    # 步驟2: 插入最終結果
    sql = """
    INSERT INTO {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED (
        prod_id, prodg_id, plan_id, layer_stage, tool_id,
        current_recipe_code, current_recipe_max_quantity, current_recipe_quantity_type,
        later_recipe_code, config_info, remark, base_rule_group,
        is_product_wildcard, min_quantity, min_virtual_value, first_product_key
    )
    SELECT 
        t.prod_id,
        t.prodg_id,
        t.plan_id,
        t.layer_stage,
        t.tool_id,
        t.current_recipe_code,
        t.current_recipe_max_quantity,
        t.current_recipe_quantity_type,
        t.later_recipe_code,
        t.config_info,
        t.remark,
        t.base_rule_group,
        t.is_product_wildcard,
        CASE 
            WHEN t.is_product_wildcard = 1 THEN t.wildcard_min_quantity
            ELSE 
                CASE 
                    WHEN len(t.all_keys) > 0 AND t.all_keys[1] != '*' THEN
                        json_extract_string(t.product_json, '$["' || t.all_keys[1] || '"]["min_quantity"]')
                    WHEN len(t.all_keys) > 1 THEN
                        json_extract_string(t.product_json, '$["' || t.all_keys[2] || '"]["min_quantity"]')
                    ELSE NULL
                END
        END as min_quantity,
        CASE 
            WHEN t.is_product_wildcard = 1 THEN t.wildcard_min_virtual_value
            ELSE 
                CASE 
                    WHEN len(t.all_keys) > 0 AND t.all_keys[1] != '*' THEN
                        json_extract_string(t.product_json, '$["' || t.all_keys[1] || '"]["min_virtual_value"]')
                    WHEN len(t.all_keys) > 1 THEN
                        json_extract_string(t.product_json, '$["' || t.all_keys[2] || '"]["min_virtual_value"]')
                    ELSE NULL
                END
        END as min_virtual_value,
        CASE 
            WHEN t.is_product_wildcard = 0 AND len(t.all_keys) > 0 THEN
                CASE 
                    WHEN t.all_keys[1] != '*' THEN t.all_keys[1]
                    WHEN len(t.all_keys) > 1 THEN t.all_keys[2]
                    ELSE NULL
                END
            ELSE NULL
        END as first_product_key
    FROM temp_json_keys t
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_SETUP_JSON_PARSED",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_SETUP_JSON_PARSED")

    # 清理臨時表
    duck_db_memory.sql("DROP TABLE IF EXISTS temp_json_keys")


def getRuleGroup1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # Rule Group 1: seq=1 (當前配方限制)
    sql = """
    INSERT /*+ append */ INTO APS_ETL_SETUP (
        parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
        tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time
    )
    SELECT 
        '{uuid}' as parentid,
        'PhotoresistControl' as rule_name,
        base_rule_group as rule_group,
        1 as seq,
        0 as inhibit_flag,
        0 as process_time,
        0 as process_cost,
        tool_id,
        CASE 
            WHEN CURRENT_RECIPE_QUANTITY_TYPE = '1' THEN 'LOT'
            ELSE 'WAFER'
        END as count_unit,
        '>=' || CURRENT_RECIPE_MAX_QUANTITY as consec_count,
        '' as request_qty,
        CASE 
            WHEN LENGTH(CURRENT_RECIPE_CODE) = 2 THEN '__' || CURRENT_RECIPE_CODE || '%'
            ELSE CURRENT_RECIPE_CODE || '%'
        END as ppid,
        REMARK,
        CAST('{current_time}' AS TIMESTAMP) as update_time
    FROM {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_SETUP (Rule Group 1 Seq 1)",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_SETUP")

    # Rule Group 1: seq=2 (後續配方強制)
    sql = """
    INSERT /*+ append */ INTO APS_ETL_SETUP (
        parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
        tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time
    )
    SELECT 
        '{uuid}' as parentid,
        'PhotoresistControl' as rule_name,
        base_rule_group as rule_group,
        2 as seq,
        1 as inhibit_flag,
        0 as process_time,
        99999 as process_cost,
        tool_id,
        'LOT' as count_unit,
        '>=1' as consec_count,
        '' as request_qty,
        CASE 
            WHEN LENGTH(LATER_RECIPE_CODE) = 2 THEN '__' || LATER_RECIPE_CODE || '%'
            ELSE LATER_RECIPE_CODE || '%'
        END as ppid,
        REMARK,
        CAST('{current_time}' AS TIMESTAMP) as update_time
    FROM {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_SETUP (Rule Group 1 Seq 2)",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_SETUP")


def getRuleGroup2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # Rule Group 2: seq=1
    sql = """
    INSERT /*+ append */ INTO APS_ETL_SETUP (
        parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
        tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time
    )
    SELECT 
        '{uuid}' as parentid,
        'PhotoresistControl' as rule_name,
        base_rule_group + 1 as rule_group,
        1 as seq,
        0 as inhibit_flag,
        0 as process_time,
        0 as process_cost,
        tool_id,
        CASE 
            WHEN CURRENT_RECIPE_QUANTITY_TYPE = '1' THEN 'LOT'
            ELSE 'WAFER'
        END as count_unit,
        '>=' || CURRENT_RECIPE_MAX_QUANTITY as consec_count,
        '' as request_qty,
        CASE 
            WHEN LENGTH(CURRENT_RECIPE_CODE) = 2 THEN '__' || CURRENT_RECIPE_CODE || '%'
            ELSE CURRENT_RECIPE_CODE || '%'
        END as ppid,
        REMARK,
        CAST('{current_time}' AS TIMESTAMP) as update_time
    FROM {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_SETUP (Rule Group 2 Seq 1)",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_SETUP")

    # Rule Group 2: seq=2 (JSON產品配置)
    sql = """
    INSERT /*+ append */ INTO APS_ETL_SETUP (
        parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
        tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time
    )
    SELECT 
        '{uuid}' as parentid,
        'PhotoresistControl' as rule_name,
        base_rule_group + 1 as rule_group,
        2 as seq,
        0 as inhibit_flag,
        0 as process_time,
        0 as process_cost,
        tool_id,
        'WAFER' as count_unit,
        '<' || min_quantity as consec_count,
        '<' || min_virtual_value as request_qty,
        CASE 
            WHEN is_product_wildcard = 1 THEN 
                CASE 
                    WHEN LENGTH(CURRENT_RECIPE_CODE) = 2 THEN '!__' || CURRENT_RECIPE_CODE || '%'
                    ELSE '!' || CURRENT_RECIPE_CODE || '%'
                END
            ELSE 
                CASE 
                    WHEN first_product_key IS NOT NULL THEN
                        CASE 
                            WHEN LENGTH(first_product_key) = 2 THEN '__' || first_product_key || '%'
                            ELSE first_product_key || '%'
                        END
                    ELSE
                        CASE 
                            WHEN LENGTH(CURRENT_RECIPE_CODE) = 2 THEN '__' || CURRENT_RECIPE_CODE || '%'
                            ELSE CURRENT_RECIPE_CODE || '%'
                        END
                END
        END as ppid,
        REMARK,
        CAST('{current_time}' AS TIMESTAMP) as update_time
    FROM {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_SETUP (Rule Group 2 Seq 2)",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_SETUP")

    # Rule Group 2: seq=3
    sql = """
    INSERT /*+ append */ INTO APS_ETL_SETUP (
        parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
        tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time
    )
    SELECT 
        '{uuid}' as parentid,
        'PhotoresistControl' as rule_name,
        base_rule_group + 1 as rule_group,
        3 as seq,
        1 as inhibit_flag,
        0 as process_time,
        99999 as process_cost,
        tool_id,
        'LOT' as count_unit,
        '>=1' as consec_count,
        '' as request_qty,
        CASE 
            WHEN LENGTH(LATER_RECIPE_CODE) = 2 THEN '__' || LATER_RECIPE_CODE || '%'
            ELSE LATER_RECIPE_CODE || '%'
        END as ppid,
        REMARK,
        CAST('{current_time}' AS TIMESTAMP) as update_time
    FROM {tempdb}APS_TMP_ETL_SETUP_JSON_PARSED
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_SETUP (Rule Group 2 Seq 3)",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_SETUP")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_PHOTORESIST_CONTROL"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_SETUP"
    used_table_list = ['APS_SYNC_RTD_SPECIAL_RECIPE_CHANGE_CONFIG']

    target_table_sql = """
        create table {}APS_ETL_SETUP
        (
            parentid       VARCHAR(64) not null,
            rule_name      VARCHAR(64) not null,
            rule_group     INTEGER not null,
            seq            INTEGER not null,
            inhibit_flag   INTEGER not null,
            process_time   INTEGER not null,
            process_cost   INTEGER not null,
            tool_id        VARCHAR(64),
            count_unit     VARCHAR(20),
            consec_count   VARCHAR(20),
            request_qty    VARCHAR(20),
            ppid           VARCHAR(100),
            remark         VARCHAR(500),
            update_time    VARCHAR(64),
            PRIMARY KEY (parentid, rule_name, rule_group, seq, update_time)
        )
    """.format("")  # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]

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
        res_dict = my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)

        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################

        # 步驟1: 基本資料轉換
        getBaseDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 步驟2: JSON解析
        getJsonParsedSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 步驟3: 生成Rule Group 1
        getRuleGroup1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 步驟4: 生成Rule Group 2
        getRuleGroup2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        my_duck.detach_all_used_table(duck_db_memory, res_dict)

        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """
                select parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
                       tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time, update_time as ver_timekey
                from APS_ETL_SETUP
            """
            postgres_table_define = """
                etl_setup(parentid, rule_name, rule_group, seq, inhibit_flag, process_time, process_cost,
                         tool_id, count_unit, consec_count, request_qty, ppid, remark, update_time, ver_timekey)
            """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_setup",  # 要小写
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
        if duck_db_memory:
            my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                      in_process_db_file=in_process_db_file,
                                                                      target_table=target_table,
                                                                      current_time=current_time_short)
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_ETL_SETUP_CODE_XX_ETL)
        raise e
    finally:
        if oracle_conn:
            oracle_conn.commit()
            oracle_conn.close()
        temp_dir = os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logging.warning("無法刪除臨時目錄 {temp_dir}: {e}")
        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            os.remove(temp_db_file)
        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()