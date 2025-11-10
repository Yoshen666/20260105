import concurrent
import gc
import logging
import multiprocessing
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime

import duckdb

from xinxiang import config
from xinxiang.jobs_etl.helper import aps_etl_rls_new_5m_helper_02, aps_etl_rls_new_5m_helper_01
from xinxiang.util import my_date, my_oracle, my_duck, cons_error_code, my_postgres, my_runner


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_RLS_5M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()
    temp_process_db_file = None
    temp_process_file1 = None
    temp_process_file2 = None
    temp_process_file = None
    temp_process_file4 = None
    temp_process_file5 = None
    temp_process_file6 = None
    temp_process_file7 = None
    temp_process_file_result = None
    temp_process_file_path = None

    target_table = "APS_ETL_RLS"
    used_table_list = ['APS_SYNC_UMT_BOAT_CONSTAINT_SETTING',
                       'APS_SYNC_PRODUCT',
                       'APS_SYNC_UMT_EQP_BATCH_SIZE_SETTING',
                       'APS_SYNC_FVEQP',
                       'APS_SYNC_ETL_TOOL',
                       'APS_SYNC_UMT_RTD_BATCH_INFO',
                       'APS_SYNC_MLIF_HOLD_LOT',
                       'APS_SYNC_APC_INHIBIT_BATCH',
                       'APS_SYNC_SCHE_LOT_TO_EQPG',
                       'APS_SYNC_FRLOT_EQP_RESOURCE_FLAG',
                       'APS_SYNC_PF_PO_N1',
                       'APS_SYNC_WIP',
                       'APS_SYNC_FLOW_DYB_RETURN',
                       'APS_SYNC_PD_RECIPE_EQP_DYB',
                       'APS_SYNC_EQP_RCP_INHIBIT',
                       'APS_SYNC_LOT_TO_EQP',
                       'APS_SYNC_RMS_RECIPE_RETICLE',
                       'APS_SYNC_RSSPLIT_INFO',
                       'APS_SYNC_WP_SPLIT_MERGE_DETAIL_HISTORY',
                       'APS_SYNC_RSPILOT_RUNING',
                       'APS_SYNC_SCHE_APC_LOT_SPECIAL_VALUE',
                       'APS_SYNC_OP_CHAMBER_RULE',
                       'APS_SYNC_PHOTO_PATH_CONVERT',
                       'APS_SYNC_FLOW',
                       'APS_SYNC_RTD_RECIPEGROUP',
                       'APS_SYNC_MASK_GROUP_MAPPLING',
                       'APS_SYNC_FRDRBL_ID_DYB',
                       'APS_SYNC_PD_RECIPE_EQP_ALL',
                       'APS_ETL_FLOW',
                       'APS_SYNC_DIFF_BATCH_SIZE_MIX',
                       'APS_TMP_LOTHISTORY_VIEW',
                       'APS_TR_CSFRINHIBIT',
                       'APS_ETL_RTDQTTIME',
                       'APS_ETL_RTDQTTIME_UN_WMTWID',
                       'APS_ETL_RTDQTTIME_WMTWID',
                       'APS_TMP_TYPE21_MEDIAN',
                       'APS_TMP_TYPE22_MEDIAN',
                       'APS_TMP_TYPE221_MEDIAN',
                       'APS_TMP_CHARGE_TIME',
                       'APS_MID_TYPE21_RUNTIME_P_ALL',
                       'APS_MID_TYPE22_RUNTIME_P_ALL',
                       'APS_MID_TYPE221_RUNTIME_P_ALL',
                       'APS_MID_TYPE21_RUNTIME_ALL',
                       'APS_MID_TYPE22_RUNTIME_ALL',
                       'APS_MID_TYPE221_RUNTIME_ALL',
                       'APS_SYNC_MAJOR_ANOMALY_EWS_LOT',
                       'APS_SYNC_APC_INHIBIT_LOT',
                       'APS_SYNC_RETICLE_PROD',
                       'APS_SYNC_RTD_PREFER_TOOL',
                       'APS_ETL_PH_RTDQTIME',
                       'APS_SYNC_NPW_RECIPE_GROUP',
                       'APS_SYNC_RTD_IMP_SOURCE',
                       'APS_SYNC_UMT_WET_RECIPE_TANK_RULE',
                       'APS_SYNC_UNIT_EQP_CHAMBER_SETTING',
                       'APS_SYNC_MM_FRSTK',
                       'APS_SYNC_LOT_LOCATION',
                       'APS_SYNC_MES_FRLRCP',
                       'APS_SYNC_CSCMBMRCPRULE',
                       'APS_SYNC_CSCMBMRCPPRCFG_MFG',
                       'APS_SYNC_RDS_RECIPEINFO',
                       'APS_MID_PH_LOTHISTORY',
                       'APS_SYNC_LOT_ANNOTATION_INFO_BRANCH',
                       'APS_SYNC_UMT_PATH_CONVERT_SETTING',
                       'APS_SYNC_QTIME_CONSTRAINT',
                       'APS_SYNC_MES_FRLOT_RETNLIST',
                       'APS_MID_TYPE21_RUNTIME_ALL_MORE',
                       'APS_MID_TYPE22_RUNTIME_ALL_MORE',
                       'APS_ETL_TOOL',
                       'APS_MID_PARMODE_TOOL_MEDIAN',
                       'APS_MID_PARMODE_PPID_MEDIAN',
                       'APS_MID_PARMODE_TOOLG_MEDIAN',
                       'APS_MID_PARMODE_PRODG5_MEDIAN',
                       'APS_MID_PARMODE_PRODG3_MEDIAN',
                       'APS_MID_PARMODE_LAYER_MEDIAN',
                       'APS_SYNC_CROSS_LOT_CSFRCROSSXFERLOTLIST',
                       'APS_SYNC_QTIME_ISSUE_SETTING',
                       'APS_SYNC_PRODUCT_NOPILOT',
                       'APS_SYNC_RTD_PIRUN_TASK',
                       'APS_SYNC_RTD_PIRUN_TASK_DEVICE',
                       'APS_SYNC_PIRUN_TASK_SCHEDULE_LOT',
                       'APS_SYNC_SHL_LOT_FIRE_NUMBER',
                       'APS_ETL_WIP',
                       'APS_MID_TYPE21_RUNTIME_PX_ALL',
                       'APS_MID_TYPE22_RUNTIME_PX_ALL',
                       'APS_MID_TYPE221_RUNTIME_PX_ALL',
                       'APS_SYNC_SAMPLING_RATE',
                       'APS_TMP_CHARGE_TIME_TOOL'
                      ]

    target_table_sql = """
            create table {}APS_ETL_RLS 
                ( 
                  parentid             VARCHAR(60) not null, 
                  lot_id               VARCHAR(60) not null, 
                  step_id              VARCHAR(60), 
                  ope_no               VARCHAR(60), 
                  target_step_id       VARCHAR(60) not null, 
                  target_ope_no        VARCHAR(60), 
                  plan_id              VARCHAR(60), 
                  target_plan_id       VARCHAR(60) not null, 
                  toolg_id             VARCHAR(60), 
                  tool_id              VARCHAR(60) not null, 
                  recipe               VARCHAR(60) not null, 
                  ppid                 VARCHAR(100) not null, 
                  reticle_id           VARCHAR(60) not null, 
                  ch_set               VARCHAR(500), 
                  set_status           VARCHAR(500) not null, 
                  qty                  INTEGER, 
                  max_batch_size       INTEGER, 
                  min_batch_size       INTEGER, 
                  non_process_runtime  DECIMAL, 
                  process_runtime      DECIMAL not null, 
                  update_time          TIMESTAMP not null, 
                  partcode             VARCHAR(60) not null, 
                  other_min_batch_size INTEGER, 
                  recipe_setup         VARCHAR(64),
                  rls_attr1            VARCHAR(64),
                  rls_attr3            VARCHAR(64),
                  PER_PROCESS_RUNTIME  VARCHAR(64),
                  PRIMARY KEY (PARENTID, LOT_ID, TARGET_STEP_ID, TARGET_PLAN_ID, TOOL_ID, RECIPE, PPID, RETICLE_ID, PARTCODE)
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
        duck_db_memory.sql('SET threads TO 6')
        if not os.path.exists(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)):
            os.makedirs(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid))
        duck_db_memory.execute(
            "SET temp_directory='{}'".format(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)))

        if not config.g_debug_mode:
            aps_etl_rls_new_5m_helper_01.create_temp_table(duck_db_memory)
        else:
            duck_db_temp = my_duck.create_duckdb_for_temp_table(_temp_db_path, temp_db_file)
            aps_etl_rls_new_5m_helper_01.create_temp_table(duck_db_temp)
            duck_db_temp.commit()
            duck_db_temp.close()
            my_duck.attach_temp_db_write_able(duck_db_memory, "TEMPDB", temp_db_file)
        # --------------------------

        # Attach用到的表
        res_dict = my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)

        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################
        # V_OP_CHAMBER_RULE view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_CHAMBER_RULE
        aps_etl_rls_new_5m_helper_02.InsertChamberRuleDataView2Temp(duck_db_memory=duck_db_memory,
                                                                    uuid=uuid,
                                                                    current_time=current_time,
                                                                    oracle_conn=oracle_conn,
                                                                    ETL_Proc_Name=ETL_Proc_Name,
                                                                    used_table_dict=used_table_list)

        aps_etl_rls_new_5m_helper_02.InsertAnomayEwsLotData(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # DF区管内位置限制
        # --产出
        # APS_TMP_ETL_RLS_BOAT_CONSTRAINTS
        # APS_TMP_ETL_RLS_BOAT_CONSTRAINTS2
        # APS_TMP_ETL_RLS_BOAT_CONSTRAINTS3
        aps_etl_rls_new_5m_helper_02.InsertBoatConstraintsDataView2Temp(duck_db_memory=duck_db_memory, uuid=uuid,
                                                                        current_time=current_time,
                                                                        oracle_conn=oracle_conn,
                                                                        ETL_Proc_Name=ETL_Proc_Name,
                                                                        used_table_dict=used_table_list)

        # RTD PATH卡控
        # -- 产出
        # APS_TMP_ETL_RLS_MULTI_LOT
        aps_etl_rls_new_5m_helper_02.InsertRtdBatchInfoView2Temp(duck_db_memory=duck_db_memory,
                                                                 uuid=uuid,
                                                                 current_time=current_time,
                                                                 oracle_conn=oracle_conn,
                                                                 ETL_Proc_Name=ETL_Proc_Name,
                                                                 used_table_dict=used_table_list)

        # RTD MultiLot卡控
        # -- 产出
        # APS_TMP_ETL_RLS_MULTI_LOT
        aps_etl_rls_new_5m_helper_02.InsertMultiLotDataView2Temp(duck_db_memory=duck_db_memory,
                                                                 uuid=uuid,
                                                                 current_time=current_time,
                                                                 oracle_conn=oracle_conn,
                                                                 ETL_Proc_Name=ETL_Proc_Name,
                                                                 used_table_dict=used_table_list)

        # V_APC_INHIBIT_BATCH view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_APC_INHIBIT_LOT
        aps_etl_rls_new_5m_helper_02.InsertApcInhibitLotDataView2Temp(duck_db_memory=duck_db_memory,
                                                                      uuid=uuid,
                                                                      current_time=current_time,
                                                                      oracle_conn=oracle_conn,
                                                                      ETL_Proc_Name=ETL_Proc_Name,
                                                                      used_table_dict=used_table_list)

        # V_SCHE_LOT_TO_EQPG view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_LOT_TO_EQPG
        aps_etl_rls_new_5m_helper_02.InsertLotToEqpgDataView2Temp(duck_db_memory=duck_db_memory,
                                                                  uuid=uuid,
                                                                  current_time=current_time,
                                                                  oracle_conn=oracle_conn,
                                                                  ETL_Proc_Name=ETL_Proc_Name,
                                                                  used_table_dict=used_table_list)

        # V_ETL_LOTHISTORY view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_LOTHISTORY
        aps_etl_rls_new_5m_helper_02.InsertLotHistoryDataView2Temp(duck_db_memory=duck_db_memory,
                                                                   uuid=uuid,
                                                                   current_time=current_time,
                                                                   oracle_conn=oracle_conn,
                                                                   ETL_Proc_Name=ETL_Proc_Name,
                                                                   used_table_dict=used_table_list)

        # V_FRLOT_EQP_RESOURCE_FLAG view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_RESOURCE_FLAG
        aps_etl_rls_new_5m_helper_02.InsertResourceFlagDataView2Temp(duck_db_memory=duck_db_memory,
                                                                     uuid=uuid,
                                                                     current_time=current_time,
                                                                     oracle_conn=oracle_conn,
                                                                     ETL_Proc_Name=ETL_Proc_Name,
                                                                     used_table_dict=used_table_list)

        # 找到封Batch站点的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_BATCH_OPE
        aps_etl_rls_new_5m_helper_02.InsertBatchDataView2Temp(duck_db_memory=duck_db_memory,
                                                              uuid=uuid,
                                                              current_time=current_time,
                                                              oracle_conn=oracle_conn,
                                                              ETL_Proc_Name=ETL_Proc_Name,
                                                              used_table_dict=used_table_list)

        # Onhand Wip 先插入到 wip temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_WIP_V
        # APS_TMP_ETL_RLS_DYB_RETURN_LOT
        # APS_TMP_ETL_RLS_DYB_LOT_PO_N1
        # APS_TMP_ETL_RLS_DYB_LOT_RCP_LOAD
        # APS_TMP_ETL_RLS_DYB_LOT_Tool
        # APS_TMP_ETL_RLS_DYB_LOT
        # APS_TMP_ETL_RLS_WIP_FLOW
        # APS_TMP_ETL_RLS_WIP_PO_N1
        # APS_TMP_ETL_RLS_WIP_RCP_LOAD
        # APS_TMP_ETL_RLS_WIP
        aps_etl_rls_new_5m_helper_02.InsertOnhandWip2Temp(duck_db_memory=duck_db_memory,
                                                          current_time=current_time,
                                                          uuid=uuid,
                                                          oracle_conn=oracle_conn,
                                                          ETL_Proc_Name=ETL_Proc_Name,
                                                          used_table_dict=used_table_list)

        # RTD STOCKER状态卡控
        aps_etl_rls_new_5m_helper_02.InsertStockerStateDataView2Temp(duck_db_memory=duck_db_memory,
                                                                     current_time=current_time,
                                                                     uuid=uuid,
                                                                     oracle_conn=oracle_conn,
                                                                     ETL_Proc_Name=ETL_Proc_Name,
                                                                     used_table_dict=used_table_list)

        # Traget Wip 再插入到 wip temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_WIP_COMING_DATE
        # APS_TMP_ETL_RLS_WIP_COMING_FLOW_DATE
        # APS_TMP_ETL_RLS_WIP
        aps_etl_rls_new_5m_helper_02.SummaryNormalFlowTragetWip(duck_db_memory=duck_db_memory,
                                                                current_time=current_time,
                                                                uuid=uuid,
                                                                oracle_conn=oracle_conn,
                                                                ETL_Proc_Name=ETL_Proc_Name,
                                                                used_table_dict=used_table_list)

        # 获取再FlowIn的站点里所有的量测站点的数据
        aps_etl_rls_new_5m_helper_02.GetAllMeasureOpeNoData(duck_db_memory=duck_db_memory,
                                                            current_time=current_time,
                                                            uuid=uuid,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # eqp recipe inhibit summary
        # -- 产出
        # APS_TMP_ETL_RLS_EQPRCP_INHIBIT
        aps_etl_rls_new_5m_helper_02.InsertEqpRcpInhibit2Temp(duck_db_memory=duck_db_memory,
                                                              current_time=current_time,
                                                              uuid=uuid,
                                                              oracle_conn=oracle_conn,
                                                              ETL_Proc_Name=ETL_Proc_Name,
                                                              used_table_dict=used_table_list)

        # 针对 target wip的 可使用 eqp 和 recipe 以及 绑机机限！！
        # -- 产出
        # APS_TMP_ETL_RLS_LOT_TO_EQP
        # APS_TMP_ETL_RLS_EQP_TOOL
        # APS_TMP_ETL_RLS_EQPRCP_RESULT1
        # APS_TMP_ETL_RLS_EQPRCP_RESULT2
        # APS_TMP_ETL_RLS_EQPRCP_RESULT3
        # APS_TMP_ETL_RLS_EQPRCP_RESULT
        # aps_etl_rls_new_5m_helper_02.insert_eqp_recipe2temp(duck_db_memory=duck_db_memory,
        #                                                                         oracle_conn=oracle_conn,
        #                                                                         ETL_Proc_Name=ETL_Proc_Name,
        #                                                                         target_db_file=target_db_file,
        #                                                                         current_time=current_time,
        #                                                                         uuid=uuid,
        #                                                                         used_table_dict=used_table_list)
        aps_etl_rls_new_5m_helper_02.InsertEQPRecipe2Temp(duck_db_memory=duck_db_memory,
                                                          oracle_conn=oracle_conn,
                                                          ETL_Proc_Name=ETL_Proc_Name,
                                                          target_db_file=target_db_file,
                                                          current_time=current_time,
                                                          uuid=uuid,
                                                          used_table_dict=used_table_list)
        # --------------------------------------------------------------------------------------------------------------
        print("sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
        temp_process_file_result = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'rcp1' + '.parquet')
        temp_process_file = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'RESULT' + '.parquet')
        temp_process_file1 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT1' + '.parquet')
        temp_process_file2 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT2' + '.parquet')
        temp_process_file4 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT4' + '.parquet')
        temp_process_file5 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT5' + '.parquet')
        temp_process_file6 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT6' + '.parquet')
        temp_process_file7 = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_" + 'INHIBIT7' + '.parquet')
        temp_process_file_path = os.path.join(config.g_mem_speed_etl_output_path, target_table,
                                          uuid + "_")

        if os.path.exists(temp_process_file_result):
            os.remove(temp_process_file_result)
        if os.path.exists(temp_process_file):
            os.remove(temp_process_file)
        if os.path.exists(temp_process_file1):
            os.remove(temp_process_file1)
        if os.path.exists(temp_process_file2):
            os.remove(temp_process_file2)
        if os.path.exists(temp_process_file4):
            os.remove(temp_process_file4)
        if os.path.exists(temp_process_file5):
            os.remove(temp_process_file5)
        if os.path.exists(temp_process_file6):
            os.remove(temp_process_file6)

        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_RESULT) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT1) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file1))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT2) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file2))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT4) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file4))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT5) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file5))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT6) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file6))
        duck_db_memory.execute(
            """copy (select * from {tempdb}APS_TMP_ETL_RLS_EQPRCP_INHIBIT7) to '{temp_process_file}' (FORMAT PARQUET)""".format(
                tempdb=my_duck.get_temp_table_mark(),temp_process_file=temp_process_file7))
        # print("---------------------------------------")
        path = os.path.dirname(os.path.abspath(__file__))
        print("------------------------------", path)
        sub_process = subprocess.Popen(["python", os.path.join(path, "aps_etl_rls_5m_sub.py"), temp_process_file_path+'@'+current_time])
        aps_etl_rls_new_5m_helper_02.mixed(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name, None)

        print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
        sub_process.wait()
        print("---------------------------------------------------------------")
        # -- 产出
        # APS_TMP_ETL_RLS
        aps_etl_rls_new_5m_helper_02.InsertRlsTempData2Temp(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list,
                                                            temp_process_file_result = temp_process_file_result
                                                            )


        # 针对指定机群的限制目前MFG要求只有RECIPE和PRC_FLAG为Y的数据
        # -- 产出
        # APS_TMP_ETL_RLS_EQPG
        aps_etl_rls_new_5m_helper_02.InsertRlsTempByEqpgTemp(duck_db_memory=duck_db_memory,
                                                             uuid=uuid,
                                                             current_time=current_time,
                                                             oracle_conn=oracle_conn,
                                                             ETL_Proc_Name=ETL_Proc_Name,
                                                             used_table_dict=used_table_list)

        # 最终数据插入到 APS_ETL_RLS 表
        # -- 产出
        # APS_TMP_ETL_RLS_TR_RESULT
        # APS_TMP_ETL_RLS_TR_GLOBAL_RESULT
        # APS_TMP_ETL_RLS_RESULT
        # APS_ETL_RLS
        aps_etl_rls_new_5m_helper_02.InsertRlsResultData(duck_db_memory=duck_db_memory,
                                                         uuid=uuid,
                                                         current_time=current_time,
                                                         oracle_conn=oracle_conn,
                                                         ETL_Proc_Name=ETL_Proc_Name,
                                                         used_table_dict=used_table_list)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select parentid,lot_id,target_step_id,target_plan_id,toolg_id,tool_id,recipe,ppid,substring(reticle_id,1,128),
                                           CASE WHEN ch_set='' THEN NULL ELSE ch_set END AS ch_set,
                                           CASE WHEN set_status='' THEN NULL ELSE set_status END AS set_status,
                                           max_batch_size,min_batch_size,non_process_runtime,
                                           process_runtime,
                                           update_time,
                                           other_min_batch_size,
                                           CASE WHEN recipe_setup='' THEN NULL ELSE recipe_setup END AS recipe_setup,
                                           CASE WHEN target_OPE_NO ='' THEN NULL ELSE target_OPE_NO END AS target_OPE_NO,
                                           case when rls_attr1='' then null else rls_attr1 end as rls_attr1,
                                           case when rls_attr3='' then null else rls_attr3 end as rls_attr3,
                                           case when PER_PROCESS_RUNTIME='' then null else PER_PROCESS_RUNTIME end as PER_PROCESS_RUNTIME
                                           from APS_ETL_RLS
                                     """
            postgres_table_define = """etl_rls(parentid,lot_id,target_step_id,target_plan_id,toolg_id,tool_id,recipe,ppid,reticle_id,ch_set,
                                               set_status,max_batch_size,min_batch_size,non_process_runtime,process_runtime,update_time,
                                               other_min_batch_size,recipe_setup,target_OPE_NO,rls_attr1,rls_attr3,PER_PROCESS_RUNTIME)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_rls",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                oracle_conn=oracle_conn,
                                                ETL_Proc_Name=ETL_Proc_Name)
        # 导出到目标文件中
        # target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory(duck_db_memory,
        #                                                                           target_table,
        #                                                                           target_table_sql.format("file_db."),
        #                                                                           current_time_short)
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
                                       cons_error_code.APS_ETL_RLS_CODE_XX_ETL)
        except Exception as log_err:
            logging.warning("寫入錯誤日誌失敗: {log_err}".format(log_err=log_err))
        raise e
    finally:
        oracle_conn.commit()
        oracle_conn.close()
        # 删除TMP目录:LQN:2023/08/21
        if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
            os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            os.remove(temp_db_file)
        # if os.path.exists(temp_process_db_file):
        #     os.remove(temp_process_db_file)
        if os.path.exists(temp_process_file_result):
            os.remove(temp_process_file_result)
        if os.path.exists(temp_process_file):
            os.remove(temp_process_file)
        if os.path.exists(temp_process_file1):
            os.remove(temp_process_file1)
        if os.path.exists(temp_process_file2):
            os.remove(temp_process_file2)
        if os.path.exists(temp_process_file4):
            os.remove(temp_process_file4)
        if os.path.exists(temp_process_file5):
            os.remove(temp_process_file5)
        if os.path.exists(temp_process_file6):
            os.remove(temp_process_file6)
        if os.path.exists(temp_process_file7):
            os.remove(temp_process_file7)
        gc.collect()  # 内存释放


if __name__ == '__main__':
    print("start")
    start = datetime.now()
    execute()
    end = datetime.now()
    print(str(my_date.duration(start, end)) + "秒")

