# 先閲讀sop_install.md
    基本流程一致，只是涉及ETL對象不一致
    需要注意（如下 Phase_2 代碼準備）
        1.回寫數據到Oracle
        2.開啓白名單寫數據到PG生產庫
        3.回寫數據到Pg測試庫（5432）
    
    Phase_2 代碼準備 在本地準備好之後，按照 sop_install.md執行

# Phase_2 代碼準備
## write_back_to_oracle.py 開啓 Phase 2 對象回寫數據到Oracle
    # 對象如下
    def execute_tool_down():
        table_name = 'APS_ETL_TOOL_DOWN'
        _execute(table_name)

    def execute_qtime_spec():
        table_name = 'APS_ETL_QTIME_SPEC'
        _execute(table_name)

    def execute_qtime_hold():
        table_name = 'APS_ETL_QTIME_HOLD'
        _execute(table_name)

    def execute_wafer_start():
        table_name = 'APS_ETL_WAFER_START'
        _execute(table_name)

    def execute_vc_record():
        table_name = 'APS_ETL_VC_RECORD'
        _execute(table_name)

    def execute_transport():
        table_name = 'APS_ETL_TRANSPORT'
        _execute(table_name)

    def execute_etl_tool():
        table_name = 'APS_ETL_TOOL'
        _execute(table_name)

    def execute_size_control():
        table_name = 'APS_ETL_SIZE_CONTROL'
        _execute(table_name)

    def execute_season_rule():
        table_name = 'APS_ETL_SEASON_RULE'
        _execute(table_name)

    def execute_sampling_lot():
        table_name = 'APS_ETL_SAMPLING_LOT'
        _execute(table_name)

    def execute_ppid_info():
        table_name = 'APS_ETL_PPID_INFO'
        _execute(table_name)

    def execute_hold_rls():
        table_name = 'APS_ETL_HOLD_RLS'
        _execute(table_name)

## SYNC_PYTHON & ETL_PYTHON 的 config/__init__.py 開啓白名單寫數據到PG生產庫
    # 對象如下
    g_etl_prod_tables = [
                     'etl_flow',              # Phase1
                     'etl_wip',               # Phase1

                     'etl_tool_down',         # Phase2
                     'etl_qtime_spec',        # Phase2
                     'etl_qtime_hold',        # Phase2
                     'etl_wafer_start',       # Phase2
                     'etl_vc_record',         # Phase2
                     'etl_transport',         # Phase2
                     'etl_tool',              # Phase2
                     'etl_size_control',      # Phase2
                     'etl_season_rule',       # Phase2
                     'etl_sampling_lot',      # Phase2
                     'etl_ppid_info',         # Phase2
                     'etl_hold_rls',          # Phase2

                     # 'etl_rls',             # Phase3
                     # 'etl_pri_wip',         # Phase3
                     # 'etl_monitor',         # Phase3
                     # 'etl_lothistory',      # Phase3
                     # 'etl_lot_op_hist',     # Phase3
                     # 'etl_mask_history',    # Phase3
                     # 'etl_sgs_rls',         # Phase3
                     # 'etl_pirun',           # Phase3
                     # 'etl_sgs_rule',        # Phase3
                     # 'etl_mask_info',       # Phase3
                     # 'etl_demand',          # Phase3
                     ]
    |||||||||||||||||||||||||||||
    # PG的測試的測試環境 修改為 測試的測試庫
    g_postgres_test_test_host = '10.52.192.99'
    g_postgres_test_test_database = 'dev_uo'
    g_postgres_test_test_port = '5432'
    g_postgres_test_test_user = 'xxetl'
    g_postgres_test_test_password = 'xxetl1234'
    g_pgserver_test_test_path = '\\\\10.52.192.99\\ETLOutput'

    >>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # PG的測試的測試環境
    g_postgres_test_test_host = '10.52.192.99'
    g_postgres_test_test_database = 'uo'
    g_postgres_test_test_port = '5433'
    g_postgres_test_test_user = 'xxetl'
    g_postgres_test_test_password = 'xxetl1234'
    g_pgserver_test_test_path = '\\\\10.52.192.99\\ETLOutput'

## SYNC_PYTHON/xinxiang/job_manager/sync_to_pg_jobs.py 回寫數據到Pg測試庫（5432）
    # 對象和寫Pg生產庫一致
    # ----------------------------------------------------------------------
    # Phase 1
    if table_name == 'APS_ETL_FLOW':
        sync_to_pg_helper.export_to_test_APS_ETL_FLOW(table_name, file_name)

    if table_name == 'APS_ETL_WIP':
        sync_to_pg_helper.export_to_test_APS_ETL_WIP(table_name, file_name)

    # Phase 2
    if table_name == 'APS_ETL_TOOL_DOWN':
        sync_to_pg_helper.export_to_test_APS_ETL_TOOL_DOWN(table_name, file_name)
    if table_name == 'APS_ETL_QTIME_SPEC':
        sync_to_pg_helper.export_to_test_APS_ETL_QTIME_SPEC(table_name, file_name)
    if table_name == 'APS_ETL_QTIME_HOLD':
        sync_to_pg_helper.export_to_test_APS_ETL_QTIME_HOLD(table_name, file_name)
    if table_name == 'APS_ETL_WAFER_START':
        sync_to_pg_helper.export_to_test_APS_ETL_WAFER_START(table_name, file_name)
    if table_name == 'APS_ETL_VC_RECORD':
        sync_to_pg_helper.export_to_test_APS_ETL_VC_RECORD(table_name, file_name)
    if table_name == 'APS_ETL_TRANSPORT':
        sync_to_pg_helper.export_to_test_APS_ETL_TRANSPORT(table_name, file_name)
    if table_name == 'APS_ETL_TOOL':
        sync_to_pg_helper.export_to_test_APS_ETL_TOOL(table_name, file_name)
    if table_name == 'APS_ETL_SIZE_CONTROL':
        sync_to_pg_helper.export_to_test_APS_ETL_SIZE_CONTROL(table_name, file_name)
    if table_name == 'APS_ETL_SEASON_RULE':
        sync_to_pg_helper.export_to_test_APS_ETL_SEASON_RULE(table_name, file_name)
    if table_name == 'APS_ETL_SAMPLING_LOT':
        sync_to_pg_helper.export_to_test_APS_ETL_SAMPLING_LOT(table_name, file_name)
    if table_name == 'APS_ETL_PPID_INFO':
        sync_to_pg_helper.export_to_test_APS_ETL_PPID_INFO(table_name, file_name)
    if table_name == 'APS_ETL_HOLD_RLS':
        sync_to_pg_helper.export_to_test_APS_ETL_HOLD_RLS(table_name, file_name)

    # Phase 3
    # if table_name == 'APS_ETL_RLS':
    #     sync_to_pg_helper.export_to_test_APS_ETL_RLS(table_name, file_name)
    # if table_name == 'APS_ETL_PRI_WIP':
    #     sync_to_pg_helper.export_to_test_APS_ETL_PRI_WIP(table_name, file_name)
    # if table_name == 'APS_ETL_MONITOR':
    #     sync_to_pg_helper.export_to_test_APS_ETL_MONITOR(table_name, file_name)
    # if table_name == 'APS_ETL_LOTHISTORY':
    #     sync_to_pg_helper.export_to_test_APS_ETL_LOTHISTORY(table_name, file_name)
    # if table_name == 'APS_ETL_LOT_OP_HIST':
    #     sync_to_pg_helper.export_to_test_APS_ETL_LOT_OP_HIST(table_name, file_name)
    # if table_name == 'APS_ETL_MASK_HISTORY':
    #     sync_to_pg_helper.export_to_test_APS_ETL_MASK_HISTORY(table_name, file_name)
    # if table_name == 'APS_ETL_SGS_RLS':
    #     sync_to_pg_helper.export_to_test_APS_ETL_SGS_RLS(table_name, file_name)
    # if table_name == 'APS_ETL_PIRUN':
    #     sync_to_pg_helper.export_to_test_APS_ETL_PIRUN(table_name, file_name)
    # if table_name == 'APS_ETL_SGS_RULE':
    #     sync_to_pg_helper.export_to_test_APS_ETL_SGS_RULE(table_name, file_name)
    # if table_name == 'APS_ETL_MASK_INFO':
    #     sync_to_pg_helper.export_to_test_APS_ETL_MASK_INFO(table_name, file_name)
    # if table_name == "APS_ETL_DEMAND":
    #     sync_to_pg_helper.export_to_test_APS_ETL_DEMAND(table_name, file_name)