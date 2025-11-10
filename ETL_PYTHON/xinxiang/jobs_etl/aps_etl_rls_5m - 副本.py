import gc
import logging
import os
from datetime import datetime

from xinxiang import config
from xinxiang.jobs_etl.helper import aps_etl_rls_new_5m_helper_02, aps_etl_rls_new_5m_helper_01
from xinxiang.util import my_date, my_oracle, my_duck, cons_error_code, my_postgres


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
                       'APS_TMP_LOTHISTORY',
                       'CSFRINHIBIT',
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
                       'APS_MID_RTDQTIME_DATALOSS',
                       'APS_SYNC_NPW_RECIPE_GROUP',
                       'APS_SYNC_RTD_IMP_SOURCE',
                       'APS_SYNC_UMT_WET_RECIPE_TANK_RULE',
                       'APS_SYNC_UNIT_EQP_CHAMBER_SETTING',
                       'APS_SYNC_MM_FRSTK',
                       'APS_SYNC_LOT_LOCATION',
                       'APS_SYNC_MES_FRLRCP',
                       'APS_SYNC_CSCMBMRCPRULE',
                       'APS_SYNC_CSCMBMRCPPRCFG_MFG'
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
                  qty                  VARCHAR(60), 
                  max_batch_size       INTEGER, 
                  min_batch_size       INTEGER, 
                  non_process_runtime  DECIMAL, 
                  process_runtime      DECIMAL not null, 
                  update_time          TIMESTAMP not null, 
                  partcode             VARCHAR(60) not null, 
                  other_min_batch_size INTEGER, 
                  recipe_setup         VARCHAR(64) 
                )
        """.format("")  # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
    target_db_file = my_duck.get_target_file_name(target_table, current_time_short)

    oracle_conn = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        # 开始日志
        my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
        # 创建DuckDB
        duck_db_memory = my_duck.create_duckdb_in_momory(target_table_sql)
        # duck_db_memory.sql('SET threads TO 20')
        duck_db_memory.sql("SET memory_limit ='50GB'")
        if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
            duck_db_memory.sql('SET threads TO 4')
            #duck_db_memory.sql("SET memory_limit ='100GB'")
            # 加上TMP目录:LQN:2023/08/21
            duck_db_memory.execute("SET temp_directory='{}'".format(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)))
        # 创建Temp表
        aps_etl_rls_new_5m_helper_01.create_temp_table(duck_db_memory)

        # Attach用到的表
        my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
        # used_table_dict = my_duck.get_all_used_table_file(oracle_conn, duck_db_memory, used_table_list)
        # for key in used_table_dict:
        #     # print(key, used_table_dict[key])
        #     sql = "insert into APS_SRCFILE(table_name, file_name) values('{table_name}', '{file_name}')".format(table_name=key, file_name=used_table_dict[key])
        #     duck_db_memory.sql(sql)
        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################
        aps_etl_rls_new_5m_helper_02.InsertAnomayEwsLotData(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # DF区管内位置限制
        # -- 产出
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
        aps_etl_rls_new_5m_helper_02.InsertEQPRecipe2Temp(duck_db_memory=duck_db_memory,
                                                          oracle_conn=oracle_conn,
                                                          ETL_Proc_Name=ETL_Proc_Name,
                                                          target_db_file=target_db_file,
                                                          current_time=current_time,
                                                          uuid=uuid,
                                                          used_table_dict=used_table_list)

        # 针对 target wip的 可使用 eqp 和 recipe 和 reticle
        # -- 产出
        # APS_TMP_ETL_RLS_EQPRCPRTC_RESULT
        aps_etl_rls_new_5m_helper_02.InsertEQPRecipeReticle2Temp(duck_db_memory=duck_db_memory,
                                                                 uuid=uuid,
                                                                 current_time=current_time,
                                                                 oracle_conn=oracle_conn,
                                                                 ETL_Proc_Name=ETL_Proc_Name,
                                                                 used_table_dict=used_table_list)

        # RTD DF和WET需要判断同一个Foup只能对应一个Recipe数据，多个则不能排程
        # -- 产出
        # APS_TMP_ETL_RLS_FOUP_RECIPE
        aps_etl_rls_new_5m_helper_02.InsertSameFoupRecipeDataView2Temp(duck_db_memory=duck_db_memory,
                                                                       uuid=uuid,
                                                                       current_time=current_time,
                                                                       oracle_conn=oracle_conn,
                                                                       ETL_Proc_Name=ETL_Proc_Name,
                                                                       used_table_dict=used_table_list)

        # 针对ReworkComing的限制，需要找对应母批Run过的机台，找不到就找自己RUN过的机台
        # -- 产出
        # APS_TMP_ETL_RLS_SPLIT_INFO
        # APS_TMP_ETL_RLS_REWORK_COMING
        # APS_TMP_ETL_RLS_REWORK_COMING
        aps_etl_rls_new_5m_helper_02.InsertReworkComing2Temp(duck_db_memory=duck_db_memory,
                                                             uuid=uuid,
                                                             current_time=current_time,
                                                             oracle_conn=oracle_conn,
                                                             ETL_Proc_Name=ETL_Proc_Name,
                                                             used_table_dict=used_table_list)

        # 针对APC Split Lot 只允许该站点该机台上RUN，有机台的话自己是Active, 其他机台为NotActive
        # -- 产出
        # APS_TMP_ETL_RLS_APC_SPLIT_INFO
        aps_etl_rls_new_5m_helper_02.InsertLotApcSplitInfo2Temp(duck_db_memory=duck_db_memory,
                                                                uuid=uuid,
                                                                current_time=current_time,
                                                                oracle_conn=oracle_conn,
                                                                ETL_Proc_Name=ETL_Proc_Name,
                                                                used_table_dict=used_table_list)

        # 针对 特补Lot的指定机台，只允许该批次站点在该特补机台上RUN
        # -- 产出
        # APS_TMP_ETL_RLS_LOTSPECIAL_RESULT
        aps_etl_rls_new_5m_helper_02.InsertLotSpecial2Temp(duck_db_memory=duck_db_memory,
                                                           uuid=uuid,
                                                           current_time=current_time,
                                                           oracle_conn=oracle_conn,
                                                           ETL_Proc_Name=ETL_Proc_Name,
                                                           used_table_dict=used_table_list)

        # V_OP_CHAMBER_RULE view的数据插入到 temp 表
        # -- 产出
        # APS_TMP_ETL_RLS_CHAMBER_RULE
        aps_etl_rls_new_5m_helper_02.InsertChamberRuleDataView2Temp(duck_db_memory=duck_db_memory,
                                                                    uuid=uuid,
                                                                    current_time=current_time,
                                                                    oracle_conn=oracle_conn,
                                                                    ETL_Proc_Name=ETL_Proc_Name,
                                                                    used_table_dict=used_table_list)

        # csfr inhibit 6种机限summary
        # -- 产出
        # APS_TMP_ETL_RLS_CSFR_IHBT
        aps_etl_rls_new_5m_helper_02.InsertCsfrInhibit2Temp(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # 已开PATH的机台不属于机限
        # -- 产出
        # APS_TMP_ETL_RLS_CSFR_PATH
        aps_etl_rls_new_5m_helper_02.InsertCsfrPath2Temp(duck_db_memory=duck_db_memory,
                                                         uuid=uuid,
                                                         current_time=current_time,
                                                         oracle_conn=oracle_conn,
                                                         ETL_Proc_Name=ETL_Proc_Name,
                                                         used_table_dict=used_table_list)

        # Rework指定机群找出有RPC_FLAG='Y',有Recipe
        # -- 产出
        # APS_TMP_ETL_RLS_LOT_TO_EQPG_INHIBIT
        # APS_TMP_ETL_RLS_LOT_TO_EQPG_RECIPE
        aps_etl_rls_new_5m_helper_02.getReworkToEqpGActive(duck_db_memory=duck_db_memory,
                                                           uuid=uuid,
                                                           current_time=current_time,
                                                           oracle_conn=oracle_conn,
                                                           ETL_Proc_Name=ETL_Proc_Name,
                                                           used_table_dict=used_table_list)

        # 虚拟Chamber规则
        aps_etl_rls_new_5m_helper_02.getTankRule(duck_db_memory=duck_db_memory,
                                                 uuid=uuid,
                                                 current_time=current_time,
                                                 oracle_conn=oracle_conn,
                                                 ETL_Proc_Name=ETL_Proc_Name,
                                                 used_table_dict=used_table_list)

        # 除了线性回归，其他RLS数据插入到 rls temp表
        # 先把特补值和APC_Pilot 单独拆开来
        # -- 产出
        # APS_TMP_ETL_RLS_LOT_SPECIAL
        aps_etl_rls_new_5m_helper_02.getLotSpecialInhibit(duck_db_memory=duck_db_memory,
                                                          uuid=uuid,
                                                          current_time=current_time,
                                                          oracle_conn=oracle_conn,
                                                          ETL_Proc_Name=ETL_Proc_Name,
                                                          used_table_dict=used_table_list)

        # 先把特补值和APC_Pilot 单独拆开来
        # -- 产出
        # APS_TMP_ETL_RLS_APC_PILOT
        aps_etl_rls_new_5m_helper_02.getApcPliotInhibit(duck_db_memory=duck_db_memory,
                                                        uuid=uuid,
                                                        current_time=current_time,
                                                        oracle_conn=oracle_conn,
                                                        ETL_Proc_Name=ETL_Proc_Name,
                                                        used_table_dict=used_table_list)

        # 将Recipe Inhibit单独拆开来
        # -- 产出
        # APS_TMP_ETL_RLS_APC_PILOT_RCP
        aps_etl_rls_new_5m_helper_02.getRcpInhibit(duck_db_memory=duck_db_memory,
                                                   uuid=uuid,
                                                   current_time=current_time,
                                                   oracle_conn=oracle_conn,
                                                   ETL_Proc_Name=ETL_Proc_Name,
                                                   used_table_dict=used_table_list)

        # -- 产出
        # APS_TMP_ETL_RLS_RTD_QTIME
        aps_etl_rls_new_5m_helper_02.getRtdQtimeInhibit(duck_db_memory=duck_db_memory,
                                                        uuid=uuid,
                                                        current_time=current_time,
                                                        oracle_conn=oracle_conn,
                                                        ETL_Proc_Name=ETL_Proc_Name,
                                                        used_table_dict=used_table_list)

        # RTD提供的Coming wip预估不准，Qtime中间站点宇清无法知道Path的数据情况，给的RTDQtimePathIssue逻辑
        aps_etl_rls_new_5m_helper_02.getPHRtdQtimeInhibit(duck_db_memory=duck_db_memory,
                                                          uuid=uuid,
                                                          current_time=current_time,
                                                          oracle_conn=oracle_conn,
                                                          ETL_Proc_Name=ETL_Proc_Name,
                                                          used_table_dict=used_table_list)

        # CHAMBER RULE的最终串接
        aps_etl_rls_new_5m_helper_02.getChamberRuleInhibit(duck_db_memory=duck_db_memory,
                                                           uuid=uuid,
                                                           current_time=current_time,
                                                           oracle_conn=oracle_conn,
                                                           ETL_Proc_Name=ETL_Proc_Name,
                                                           used_table_dict=used_table_list)

        # 重大异常LOT卡控
        aps_etl_rls_new_5m_helper_02.getAnomalyEwsLot(duck_db_memory=duck_db_memory,
                                                      uuid=uuid,
                                                      current_time=current_time,
                                                      oracle_conn=oracle_conn,
                                                      ETL_Proc_Name=ETL_Proc_Name,
                                                      used_table_dict=used_table_list)

        # 针对MONITOR userFlag的卡控
        aps_etl_rls_new_5m_helper_02.getMonitorRecipeInhibit(duck_db_memory=duck_db_memory,
                                                             uuid=uuid,
                                                             current_time=current_time,
                                                             oracle_conn=oracle_conn,
                                                             ETL_Proc_Name=ETL_Proc_Name,
                                                             used_table_dict=used_table_list)

        # -- 产出
        # APS_TMP_ETL_RLS
        aps_etl_rls_new_5m_helper_02.InsertRlsTempData2Temp(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # 针对指定机群的限制目前MFG要求只有RECIPE和PRC_FLAG为Y的数据
        # -- 产出
        # APS_TMP_ETL_RLS_EQPG
        aps_etl_rls_new_5m_helper_02.InsertRlsTempByEqpgTemp(duck_db_memory=duck_db_memory,
                                                             uuid=uuid,
                                                             current_time=current_time,
                                                             oracle_conn=oracle_conn,
                                                             ETL_Proc_Name=ETL_Proc_Name,
                                                             used_table_dict=used_table_list)

        # 计算Type21的中位数
        # -- 产出
        # APS_TMP_ETL_RLS_TYPE21_PT
        aps_etl_rls_new_5m_helper_02.InsertType21MedianTemp(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name)

        # 计算Type22的中位数
        # -- 产出
        # APS_TMP_ETL_RLS_TYPE22_PT
        aps_etl_rls_new_5m_helper_02.InsertType22MedianTemp(duck_db_memory=duck_db_memory,
                                                            uuid=uuid,
                                                            current_time=current_time,
                                                            oracle_conn=oracle_conn,
                                                            ETL_Proc_Name=ETL_Proc_Name,
                                                            used_table_dict=used_table_list)

        # 计算Type221的中位数
        # -- 产出
        # APS_TMP_ETL_RLS_TYPE221_PT
        aps_etl_rls_new_5m_helper_02.InsertType221MedianTemp(duck_db_memory=duck_db_memory,
                                                             uuid=uuid,
                                                             current_time=current_time,
                                                             oracle_conn=oracle_conn,
                                                             ETL_Proc_Name=ETL_Proc_Name,
                                                             used_table_dict=used_table_list)

        # DF的NON_PROCESS_TYPE
        # -- 产出
        # APS_TMP_ETL_RLS_CHARGE_TIME
        aps_etl_rls_new_5m_helper_02.InsertTemChargeTimeTemp(duck_db_memory=duck_db_memory,
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
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg:
            select_sql_in_duck = """select parentid,lot_id,target_step_id,target_plan_id,toolg_id,tool_id,recipe,ppid,reticle_id,ch_set,
                                           set_status,max_batch_size,min_batch_size,non_process_runtime,process_runtime,update_time,
                                           other_min_batch_size,recipe_setup from APS_ETL_RLS
                                     """
            postgres_table_define = """etl_rls(parentid,lot_id,target_step_id,target_plan_id,toolg_id,tool_id,recipe,ppid,reticle_id,ch_set,
                                               set_status,max_batch_size,min_batch_size,non_process_runtime,process_runtime,update_time,
                                               other_min_batch_size,recipe_setup)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_rls",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define)
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory(duck_db_memory,
                                                                                  target_table,
                                                                                  target_table_sql.format("file_db."),
                                                                                  current_time_short)
        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file)
        # 写完成日志
        my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
    except Exception as e:
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_ETL_SGS_RLS_CODE_XX_ETL)
        logging.debug(e)
        raise e
    finally:
        oracle_conn.commit()
        oracle_conn.close()
        if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
            # 删除TMP目录:LQN:2023/08/21
            if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
                os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        gc.collect()  # 内存释放


if __name__ == '__main__':
    print("start")
    start = datetime.now()
    execute()
    end = datetime.now()
    print(str(my_date.duration(start, end)) + "秒")

