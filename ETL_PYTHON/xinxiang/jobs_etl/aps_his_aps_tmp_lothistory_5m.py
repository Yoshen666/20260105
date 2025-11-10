import gc
import logging
import os
import shutil
from datetime import datetime, timedelta, time

import duckdb

from xinxiang import config
from xinxiang.util import  my_oracle, my_date, cons_error_code, oracle_to_duck_common
import pandas as pd
import numpy as np

from xinxiang.util.oracle_to_duck_his import delete_over_three_version


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
    ETL_Proc_Name = "APS_ETL_BR.sync_APS_TMP_LOTHISTORY"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    source_table = "V_ETL_LOTHISTORY"
    target_table_name = "APS_TMP_LOTHISTORY_VIEW"
    check_date_column = "CLAIM_TIME"
    sync_day = 90
    query_sql = """SELECT LOT_ID, OPE_NO,CASE WHEN OPE_SEQ IS NULL THEN 0 ELSE OPE_SEQ END AS OPE_SEQ,CASE WHEN PASS_COUNT IS NULL THEN 0 ELSE PASS_COUNT END AS PASS_COUNT,LINE,CLAIM_TIME,MAINPD_ID,EQP_ID,MOVE_FLAG,REWORK_MOVE_FLAG,PURE_MOVE_FLAG,DUMMY_MOVE_FLAG,PROCESS_AREA,MODULE_ID,DETAIL_MODULE_ID,CASE WHEN PRIORITY IS NULL THEN 0 ELSE PRIORITY END AS PRIORITY,CASE WHEN PRIORITY_CLASS IS NULL THEN 0 ELSE PRIORITY_CLASS END AS PRIORITY_CLASS,SHL_FLAG,CASE WHEN CYCLE_TIME IS NULL THEN 0 ELSE CYCLE_TIME END AS CYCLE_TIME,COALESCE(PREV_OP_COMP_TIME, OP_START_DATE_TIME) AS PREV_OP_COMP_TIME,BANK_ID,PREV_BANK_ID,REWORK_CHILD_FLAG,PW_FLAG,PRODSPEC_ID,PHOTO_LAYER,SHIFT_DATE,SHIFT_HOUR,SHIFT,RECIPE_ID,RETICLE_ID,CASE WHEN WAFER_QTY IS NULL THEN 0 ELSE WAFER_QTY END AS WAFER_QTY,PRODGRP_ID,EQP_TYPE,CTRL_JOB,SUB_LOT_TYPE,OP_START_DATE_TIME,OPE_CATEGORY,LAYER,CHIP_BODY,SUFFIX,CLAIM_USER,REPLACE(REPLACE(claim_memo, chr(10), ''), chr(13), '')  AS claim_memo,LOT_OWNER_ID,NENG_FLAG,LOCATION_ID,PROCESS_START_TIME,CAST_ID,CASE WHEN BANK_PERIOD_TIME IS NULL THEN 0 ELSE BANK_PERIOD_TIME END AS BANK_PERIOD_TIME, CASE WHEN HOLD_PERIOD_TIME IS NULL THEN 0 ELSE HOLD_PERIOD_TIME END AS HOLD_PERIOD_TIME,BATCH_ID,LAST_MAIN_CTRL_JOB FROM V_ETL_LOTHISTORY"""
    create_table_sql = """
        CREATE TABLE APS_TMP_LOTHISTORY_VIEW (
            LOT_ID VARCHAR,
            OPE_NO VARCHAR,
            OPE_SEQ INTEGER,
            PASS_COUNT INTEGER,
            LINE VARCHAR,
            CLAIM_TIME VARCHAR,
            MAINPD_ID VARCHAR,
            EQP_ID VARCHAR,
            MOVE_FLAG VARCHAR,
            REWORK_MOVE_FLAG VARCHAR,
            PURE_MOVE_FLAG VARCHAR,
            DUMMY_MOVE_FLAG VARCHAR,
            PROCESS_AREA VARCHAR,
            MODULE_ID VARCHAR,
            DETAIL_MODULE_ID VARCHAR,
            PRIORITY INTEGER,
            PRIORITY_CLASS INTEGER,
            SHL_FLAG VARCHAR,
            CYCLE_TIME DOUBLE,
            PREV_OP_COMP_TIME VARCHAR,
            BANK_ID VARCHAR,
            PREV_BANK_ID VARCHAR,
            REWORK_CHILD_FLAG VARCHAR,
            PW_FLAG VARCHAR,
            PRODSPEC_ID VARCHAR,
            PHOTO_LAYER VARCHAR,
            SHIFT_DATE VARCHAR,
            SHIFT_HOUR VARCHAR,
            SHIFT VARCHAR,
            RECIPE_ID VARCHAR,
            RETICLE_ID VARCHAR,
            WAFER_QTY INTEGER,
            PRODGRP_ID VARCHAR,
            EQP_TYPE VARCHAR,
            CTRL_JOB VARCHAR,
            SUB_LOT_TYPE VARCHAR,
            OP_START_DATE_TIME VARCHAR,
            OPE_CATEGORY VARCHAR,
            LAYER VARCHAR,
            CHIP_BODY VARCHAR,
            SUFFIX VARCHAR,
            CLAIM_USER VARCHAR,
            CLAIM_MEMO VARCHAR,
            LOT_OWNER_ID VARCHAR,
            NENG_FLAG VARCHAR,
            LOCATION_ID VARCHAR,
            PROCESS_START_TIME VARCHAR,
            CAST_ID VARCHAR,
            BANK_PERIOD_TIME DOUBLE,
            HOLD_PERIOD_TIME DOUBLE,
            BATCH_ID VARCHAR,
            LAST_MAIN_CTRL_JOB VARCHAR
        )
        """

    oracle_conn = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()

        _now = my_date.date_time_second_str()
        _now_short = my_date.date_time_second_short_str()

        target_path = config.g_mem_sync_result_path

        # 创建目录
        target_folder = os.path.join(target_path, target_table_name, "inprocess")
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        # 创建DuckDB的文件
        _file_name = target_table_name + "_" + my_date.date_time_second_short_str() + ".db"
        _file_name_csv = target_table_name + "_" + my_date.date_time_second_short_str() + ".csv"
        in_process_db_file = os.path.join(target_folder, _file_name)
        target_db_file = os.path.join(target_path, target_table_name, _file_name)  # 最终文件在inprocess目录上层

        # 创建DuckDB文件
        duck_db_cursor = None
        # 记录开始日志
        my_oracle.StartCleanUpAndLog(conn=oracle_conn, ETLProcName=ETL_Proc_Name, ETLStartTime=_now)

        # 把历史数据先ATTACH过来，再拷贝到新的库中去
        last_file_name = my_oracle.get_last_create_file(my_oracle.oracle_get_connection(), target_table_name)
        print(last_file_name)
        if last_file_name:
            shutil.copy2(last_file_name, in_process_db_file)
            duck_db_cursor = duckdb.connect(in_process_db_file)
            # 追加最新的数据
            # 上次执行文件的创建时间戳
            sql = """SELECT MAX({check_date_column}) FROM {target_table}""".format(check_date_column=check_date_column,
                                                                                   target_table=target_table_name)
            duck_db_cursor.execute(sql)
            last_date = duck_db_cursor.fetchall()

            _create_time_date = last_date[0][0]
            if _create_time_date is not None:
                if type(_create_time_date) is str:
                    _create_time = datetime.strptime(_create_time_date, '%Y-%m-%d %H:%M:%S')
                else:
                    _create_time = _create_time_date
                _create_time = _create_time - timedelta(hours=5) # 提前兩個小時
                print(_create_time)

                query_sql = query_sql \
                            + " where 1=1 and {check_date_column} >= TO_DATE('{create_time}', 'YYYY-MM-DD HH24:MI:SS')" \
                                .format(check_date_column=check_date_column,
                                        create_time=_create_time)
                last_one_hour_date = pd.read_sql(query_sql, oracle_conn)
                last_one_hour_date = last_one_hour_date.replace(pd.NaT, np.nan)
                last_one_hour_date = last_one_hour_date.replace(np.nan, '')

                query_duck_sql = """
                    select LOT_ID, OPE_NO,
                        CASE WHEN OPE_SEQ IS NULL THEN 0 ELSE OPE_SEQ END AS OPE_SEQ,
                        CASE WHEN PASS_COUNT IS NULL THEN 0 ELSE PASS_COUNT END AS PASS_COUNT,LINE,CLAIM_TIME,MAINPD_ID,EQP_ID,MOVE_FLAG,REWORK_MOVE_FLAG,PURE_MOVE_FLAG,DUMMY_MOVE_FLAG,PROCESS_AREA,MODULE_ID,DETAIL_MODULE_ID,
                        CASE WHEN PRIORITY IS NULL THEN 0 ELSE PRIORITY END AS PRIORITY,
                        CASE WHEN PRIORITY_CLASS IS NULL THEN 0 ELSE PRIORITY_CLASS END AS PRIORITY_CLASS,SHL_FLAG,
                        CASE WHEN CYCLE_TIME IS NULL THEN 0 ELSE CYCLE_TIME END AS CYCLE_TIME,COALESCE(PREV_OP_COMP_TIME, OP_START_DATE_TIME) AS PREV_OP_COMP_TIME,BANK_ID,PREV_BANK_ID,REWORK_CHILD_FLAG,PW_FLAG,PRODSPEC_ID,PHOTO_LAYER,SHIFT_DATE,SHIFT_HOUR,SHIFT,RECIPE_ID,RETICLE_ID,
                        CASE WHEN WAFER_QTY IS NULL THEN 0 ELSE WAFER_QTY END AS WAFER_QTY,PRODGRP_ID,EQP_TYPE,CTRL_JOB,SUB_LOT_TYPE,OP_START_DATE_TIME,OPE_CATEGORY,LAYER,CHIP_BODY,SUFFIX,CLAIM_USER,REPLACE(REPLACE(claim_memo, chr(10), ''), chr(13), '')  AS claim_memo,LOT_OWNER_ID,NENG_FLAG,LOCATION_ID,PROCESS_START_TIME,CAST_ID,CASE WHEN BANK_PERIOD_TIME IS NULL THEN 0 ELSE BANK_PERIOD_TIME END AS BANK_PERIOD_TIME, CASE WHEN HOLD_PERIOD_TIME IS NULL THEN 0 ELSE HOLD_PERIOD_TIME END AS HOLD_PERIOD_TIME,BATCH_ID,LAST_MAIN_CTRL_JOB 
                    from {target_table} where 1=1 and {check_date_column} >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '2 day'
                """.format(target_table=target_table_name,
                           check_date_column=check_date_column
                           )
                last_one_hour_date_duck = pd.read_sql(query_duck_sql, duck_db_cursor)
                last_one_hour_date_duck = last_one_hour_date_duck.replace(pd.NaT, np.nan)
                last_one_hour_date_duck = last_one_hour_date_duck.replace(np.nan, '')

                columns_str = ", ".join(last_one_hour_date.columns)

                select_insert_item = """
                  insert into {target_table_name} ({columns_str})
                    select {columns_str}  FROM last_one_hour_date HS
                    where 1=1
                    and  NOT EXISTS (
                        SELECT HL.LOT_ID FROM last_one_hour_date_duck HL
                        WHERE   
                        1=1
                        AND HL.LOT_ID = HS.LOT_ID  
                        AND HL.OPE_NO = HS.OPE_NO  
                        AND CAST(HL.CLAIM_TIME AS TIMESTAMP) = CAST(HS.CLAIM_TIME AS TIMESTAMP)  
                        )
                     """.format(target_table_name=target_table_name, columns_str=columns_str)
                duck_db_cursor.execute(select_insert_item)

                # 删除老数据
                delete_sql = """
                       delete from {target_table} where {check_date_column} < DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '90 day'
                       """.format(target_table=target_table_name,
                                  check_date_column=check_date_column,
                                  save_day=sync_day + 1)
                duck_db_cursor.execute(delete_sql)
                duck_db_cursor.close()
                try:
                    os.rename(in_process_db_file, target_db_file)
                except Exception as eee:
                    logging.exception("文件太大，等待20秒后再拷贝... %s", eee)
                    time.sleep(20)
                    os.rename(in_process_db_file, target_db_file)

                # 写版本号
                my_oracle.HandlingVerControl(oracle_conn, uuid, target_table_name, target_db_file, current_time_short)
                # 写完成日志
                my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
        else:
            # 做全量
            query_sql = query_sql + """ where 1=1 and {check_date_column} >= SYSDATE - {sync_day}""".format(
                source_table=source_table,
                check_date_column=check_date_column,
                sync_day=sync_day)
            print(query_sql)
            oracle_to_duck_common.oracle_to_duck_csv(
                conn=oracle_conn,
                etl_name=ETL_Proc_Name,
                source_table=source_table,
                target_table=target_table_name,
                query_sql=query_sql,
                create_table_sql=create_table_sql
            )
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, "XETL076")

        raise e
    finally:
        delete_over_three_version(table_name=target_table_name)
        oracle_conn.commit()
        oracle_conn.close()
        # 删除TMP目录:LQN:2023/08/21
        if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
            os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()