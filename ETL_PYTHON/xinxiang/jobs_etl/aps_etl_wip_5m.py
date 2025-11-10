import gc
import logging
import os
from datetime import datetime

from xinxiang import config
from xinxiang.jobs_etl.helper import aps_etl_wip_5m_helper_01, aps_etl_wip_5m_helper_02
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_runner


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
    # TODO 改成你自己的逻辑
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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_WIP_5M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_WIP"
    used_table_list = ['APS_ETL_FLOW',
                       'APS_SYNC_WIP',
                       'APS_MID_FHOPEHS_VIEW',
                       'APS_SYNC_PF_PO_N1',
                       'APS_SYNC_RMS_RECIPE_RETICLE',
                       'APS_MID_FHOPEHS_RETICLE_VIEW',
                       'APS_SYNC_LOT_PROCESS_MONITOR',
                       'APS_SYNC_LOT_LOCATION',
                       'APS_SYNC_MCS_SCHED_LOCATION_INFO',
                       'APS_SYNC_FLOW_DYB_RETURN',
                       'APS_SYNC_PD_RECIPE_EQP_DYB',
                       'APS_SYNC_ETL_TOOL',
                       'APS_SYNC_PRODUCT',
                       'APS_SYNC_RTD_MOVE_TARGET',
                       'APS_SYNC_HOLD_DETAIL_HISTORY',
                       'APS_SYNC_SCHE_LOT_PURPTY_SETTING',
                       'APS_SYNC_SCHE_APC_PA',
                       'APS_TMP_LOTHISTORY_VIEW',
                       'APS_SYNC_WP_SPLIT_MERGE_DETAIL_HISTORY',
                       'APS_SYNC_RSSPLIT_INFO',
                       'APS_SYNC_FLOW_ORDER_CHANGE',
                       'APS_SYNC_FRLOT_FUTUREHOLD',
                       'APS_SYNC_QTIME_CONSTRAINT',
                       'APS_SYNC_MLIF_FOR_BATCH_MANU_REG',
                       'APS_SYNC_MLIF_FOR_WOT_MANU_REG',
                       'APS_SYNC_FVLOT_QTIME',
                       'APS_SYNC_RETICLE_PROD',
                       'APS_SYNC_RTD_PREFER_TOOL',
                       'APS_SYNC_FREQP_UTS',
                       'APS_SYNC_MES_FRLOT_RETNLIST',
                       'APS_SYNC_VIRT_QT_SETTING',
                       'APS_MID_PH_LOTHISTORY',
                       'APS_MID_PH_LOTHISTORY_A3',
                       'APS_SYNC_SETTING_MCS_LOCATION_MANUAL',
                       'APS_SYNC_LOT_ANNOTATION_INFO_BRANCH',
                       'APS_SYNC_MES_BOND_PROD_PARTS',
                       'APS_SYNC_FRLOT',
                       'APS_SYNC_MES_FSBONDGRP_MAP',
                       'APS_MID_WIP_PROCESS_START',
                       'APS_SYNC_CROSS_LOT_CSFRCROSSXFERLOTLIST',
                       'APS_SYNC_PRODUCT_PLATFORM',
                       'APS_SYNC_LOT_PRIORITY',
                       'APS_SYNC_PRIORITY_DEFINE',
                       'APS_SYNC_BONDING_OPE_NO',
                       'APS_SYNC_RTD_CAPACITY_SETTING',
                       'APS_SYNC_FLOW',
                       'APS_SYNC_IGNORE_WATER_LINE_LOT',
                       'APS_SYNC_RSPILOT_EXCEPTION']
    target_table_sql = """
        create table {}APS_ETL_WIP
        (
          parentid          VARCHAR(60) not null,
          lot_id            VARCHAR(60) not null,
          step_id           VARCHAR(60) not null,
          ope_no            VARCHAR(60) not null,
          target_step_id    VARCHAR(60) not null,
          target_ope_no     VARCHAR(60) not null,
          target_toolg_id   VARCHAR(60) not null,
          prodg_id          VARCHAR(60),
          prodg_tech        VARCHAR(60),
          prod_id           VARCHAR(60) not null,
          plan_id           VARCHAR(60) not null,
          target_plan_id    VARCHAR(60) not null,
          lot_type          VARCHAR(30),
          foup_id           VARCHAR(60),
          qty               DECIMAL not null,
          pty               INTEGER not null,
          shr_flag          INTEGER,
          lot_status        VARCHAR(60) not null,
          remain_qtime      DECIMAL,
          qtime_step_id_to  VARCHAR(60),
          arrival           VARCHAR(60) not null,
          layer             VARCHAR(60),
          stage             VARCHAR(60),
          tool_id           VARCHAR(60),
          reticle_id        VARCHAR(60),
          recipe            VARCHAR(60),
          ppid              VARCHAR(60),
          job_prepare       VARCHAR(60),
          track_in          VARCHAR(60),
          process_start     VARCHAR(60),
          tech_id           VARCHAR(60),
          customer          VARCHAR(60),
          location          VARCHAR(20),
          factory           VARCHAR(20),
          parent_lot        VARCHAR(60),
          critical_ratio    DECIMAL,
          hold_code         VARCHAR(4000),
          pre_tool_id       VARCHAR(60),
          parent_tool_flag  VARCHAR(2),
          update_time       VARCHAR(60) not null,
          partcode          VARCHAR(60) not null,
          target_prod_id    VARCHAR(60) default 'N/A' not null,
          from_step_id      VARCHAR(60),
          to_step_id        VARCHAR(60),
          white_flag        VARCHAR(2),
          batch_id          VARCHAR(200),
          assign_batch_id   VARCHAR(64),
          assign_multi_id   VARCHAR(64),
          frombatch_time    VARCHAR(60),
          assign_tool_id    VARCHAR(64),
          trans_state       VARCHAR(64),
          foup_station_id   VARCHAR(64),
          inhibit_pass_flag INTEGER,
          preohb_tool_id    VARCHAR(64),
          switch_flag       VARCHAR(1),
          wip_attr1         VARCHAR(64),
          qtime_full        VARCHAR(64),
          wip_attr2         VARCHAR(64),
          pty_group         VARCHAR(64),
          conti_flag        VARCHAR(64),
          org_remain_qtime  VARCHAR(64),
          qtime_ignore_flag INTEGER,
          EXCEPT_PILOT_FLAG INTEGER,
          PRIMARY KEY (PARENTID, LOT_ID, TARGET_STEP_ID, TARGET_PLAN_ID, PARTCODE)
        )
    """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]

    # -------------------------- 内存模式改成文件模式
    _temp_db_path = os.path.join(config.g_mem_speed_etl_output_path, target_table, "inprocess")
    if not os.path.isdir(_temp_db_path):
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
            "SET temp_directory='{}'".format(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)))

        if not config.g_debug_mode:
            # 创建TEMP表
            aps_etl_wip_5m_helper_01.create_temp_table(duck_db_memory)
        else:
            # 创建实表
            duck_db_temp = my_duck.create_duckdb_for_temp_table(_temp_db_path, temp_db_file)
            # 创建Temp表
            aps_etl_wip_5m_helper_01.create_temp_table(duck_db_temp)
            duck_db_temp.commit()
            duck_db_temp.close()
            my_duck.attach_temp_db_write_able(duck_db_memory, "TEMPDB", temp_db_file)
        # --------------------------

        # Attach用到的表
        res_dict = my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################

        # Last Flow data 单独放到 aps_tmp_etl_wip_last_flow temp 表
        aps_etl_wip_5m_helper_02.GetLastFlowData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取ONhand Wip，如果是DYB的取当前和return站点的数据
        aps_etl_wip_5m_helper_02.InsertWip2Temp(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        aps_etl_wip_5m_helper_02.InsertWipHoldCode2Temp(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 针对MergeHold的逻辑，当站为MergeHold后续的Coming Lot的状态都是MergeHold。如果第二个Coming站为MergeHold，则第三个Coming站也要为MergeHold
        aps_etl_wip_5m_helper_02.InsertWipMergeHoldCode2Temp(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取Target Step信息和Remain Qtime数据
        aps_etl_wip_5m_helper_02.SummaryOnhandAndTragetWip(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Add Detach all used table
        my_duck.detach_all_used_table(duck_db_memory, res_dict)


        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################

        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select  parentid, lot_id, step_id, target_step_id, target_toolg_id,prodg_id,prodg_tech, prod_id,plan_id,target_plan_id,
                                               lot_type,foup_id,qty,pty,shr_flag,lot_status,remain_qtime,qtime_step_id_to,arrival,layer,stage,tool_id,reticle_id,
                                               recipe,ppid,track_in,process_start,tech_id,customer,location,parent_lot,critical_ratio,hold_code,pre_tool_id,update_time,
                                               factory,parent_tool_flag,target_prod_id,from_step_id,to_step_id,job_prepare,white_flag,batch_id,assign_batch_id,assign_multi_id,
                                               frombatch_time,assign_tool_id,trans_state,foup_station_id,inhibit_pass_flag,preohb_tool_id,switch_flag,
                                               CASE WHEN wip_attr1 = '' THEN NULL ELSE wip_attr1 END AS wip_attr1,
                                               CASE WHEN qtime_full ='' THEN NULL ELSE qtime_full END AS qtime_full,
                                               CASE WHEN wip_attr2 = '' THEN NULL ELSE wip_attr2 END AS wip_attr2,
                                               CASE WHEN pty_group = '' THEN NULL ELSE pty_group END AS pty_group,
                                               CASE WHEN conti_flag = '' THEN NULL ELSE conti_flag END AS conti_flag,
                                               org_remain_qtime,
                                               qtime_ignore_flag,
                                               EXCEPT_PILOT_FLAG
                                               from APS_ETL_WIP
                                       """
            postgres_table_define = """etl_wip(parentid, lot_id, step_id, target_step_id, target_toolg_id,prodg_id,prodg_tech, prod_id,plan_id,target_plan_id,
                                               lot_type,foup_id,qty,pty,shr_flag,lot_status,remain_qtime,qtime_step_id_to,arrival,layer,stage,tool_id,reticle_id,
                                               recipe,ppid,track_in,process_start,tech_id,customer,location,parent_lot,critical_ratio,hold_code,pre_tool_id,update_time,
                                               factory,parent_tool_flag,target_prod_id,from_step_id,to_step_id,job_prepare,white_flag,batch_id,assign_batch_id,assign_multi_lot,
                                               formbatch_time,assign_tool_id,trans_state,foup_station_id,inhibit_pass_flag,preohb_tool_id,switch_flag,wip_attr1,qtime_full,
                                               wip_attr2,pty_group,conti_flag,org_remain_qtime,qtime_ignore_flag,EXCEPT_PILOT_FLAG)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_wip",  # 要小写
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
                                       cons_error_code.APS_ETL_WIP_CODE_XX_ETL)
        except Exception as log_err:
            logging.warning("寫入錯誤日誌失敗: {log_err}".format(log_err=log_err))
        raise e
    finally:
        oracle_conn.commit()
        oracle_conn.close()
        if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
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