x# 先閲讀sop_install.md
    基本流程一致，只是涉及ETL對象不一致
    需要注意（如下 Phase_3 代碼準備）
        1.回寫數據到Oracle（是否要回寫要確認！！！和Phase2最大的區別）
        2.開啓白名單寫數據到PG生產庫
        3.回寫數據到Pg測試庫（5432）
    
    Phase_3 代碼準備 在本地準備好之後，按照 sop_install.md執行

# Phase_3 代碼準備
## write_back_to_oracle.py 
###  （如果最終上綫，則需要在99_prod_temp_install_windows.bat中刪除 xinxiang_aps_etl_write_back_to_oracle_tool_down 定時任務）
###   如果需要觀察一階段，則需要 開啓 Phase 4 對象回寫數據到Oracle 
    TODO 高盛后续改
    
## SYNC_PYTHON & ETL_PYTHON 的 config/__init__.py 開啓白名單寫數據到PG生產庫
### 設置 g_test_and_prod_mixed = False


## SYNC_PYTHON/xinxiang/job_manager/sync_to_pg_jobs.py 回寫數據到Pg測試庫（5432）
    全部放开
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
    if table_name == 'APS_ETL_RLS':
        sync_to_pg_helper.export_to_test_APS_ETL_RLS(table_name, file_name)

    # Phase 4 TODO 要放開
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