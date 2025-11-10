import logging

g_debug_mode = False
g_copy_to_pg = True # TODO 是否将结果拷贝到Postgres中去
g_debug_file_path = r"D:\\debug"
g_debug_file = g_debug_file_path + r"\debug.db"
g_print_flag = True
g_thread_and_memory_limit = False  # 是否手动管理内存和多进程

g_log_path = 'D:/XinXiang/Log/NextChipETL'
g_log_level= logging.DEBUG

# 为了在oracle失联的情况也可以发alarm，特意单独保存error log
g_error_log_path = 'D:/XinXiang/Log/ErrorETL'
g_error_log_level= logging.ERROR

# g_driver_driver = 'oracle.jdbc.driver.OracleDriver'
# g_driver_jarFile = os.path.join(os.path.split(os.path.abspath(__file__))[0].replace("xinxiang", "").replace("config", "")[:-1], "ojdbc6.jar")

g_oracle_ip = "10.52.192.100"
g_oracle_dsn = g_oracle_ip + ':1521/uosche'
#g_oracle_dsn = "UOSCHE_PROD"
g_oracle_url = 'jdbc:oracle:thin:@' + g_oracle_ip+ ':1521/uosche'
g_oracle_user = 'uosche'
g_oracle_password = 'uosche'
g_oracle_owner = 'uosche'


# g_debug_mode = False的时候生效
local_oracle_ip = "10.52.192.99"
local_oracle_dsn = local_oracle_ip + ':1521/uosche'
local_oracle_url = 'jdbc:oracle:thin:@' + local_oracle_ip+ ':1521/uosche'
local_oracle_user = 'phschel'
local_oracle_password = 'phschel'
local_oracle_owner = 'phschel'

g_postgres_host = '10.52.192.110'
g_postgres_database = 'uo'
g_postgres_port = '5432'
g_postgres_user = 'xxetl'
g_postgres_password = 'xxetl6yhn'
# 共享的Postgres路径(最终在pg server中的路径也为{g_mem_etl_output_path}/时间戳.csv )
g_pgserver_path = '\\\\10.52.192.110\\ETLOutput'

g_test_postgres_host = '10.52.192.99'
g_test_postgres_database = 'dev_uo'
g_test_postgres_port = '5432'
g_test_postgres_user = 'xxetl'
g_test_postgres_password = 'xxetl1234'
g_test_pgserver_path = '\\\\10.52.192.99\\ETLOutput'


# 读取Inhibit等导出的dat文件路径
g_mem_oracle_dat_input_path = r'D:\workspace\ArchiveFile'
g_mem_backup_oracle_dat_input_path = r'\\{}\workspace\ArchiveFile'
# Oracle sqldr配置文件路径
# g_mem_oracle_sqldr_config_path = r'D:\workspace\ExportConfig'
# sync 产出路径
g_mem_sync_result_path = r'D:\workspace\SrcData'
g_mem_backup_sync_result_path = r'\\{}\workspace\SrcData'
# sync duckdb > dat 产出路径
# g_mem_sync_dat_output_path = r'D:\workspace\ExportData'
# ETL 产出路径
g_mem_etl_output_path = r'D:\workspace\ETLOutput'
g_mem_backup_etl_output_path = r'\\{}\workspace\ETLOutput'
# 内存的硬盤化
# g_mem_speed_etl_output_path = 'R:\workspace'
g_mem_speed_etl_output_path = r'R:\workspace'
# 一些没法通过pandas直生成表的表，将字段均生成为varchar
g_all_varchar_table = ['APS_SYNC_PD_RECIPE_EQP_REWORK',
                       'APS_SYNC_MLIF_HOLD_LOT',
                       'APS_ETL_FLOW',
                       'APS_SYNC_LOT_PROCESS_MONITOR',
                       'APS_SYNC_EQP_STATUS_E10_FORECAST',
                       'APS_SYNC_ETL_TOOL',
                       'APS_SYNC_LOT_PROCESS_MONITOR',
                       'APS_SYNC_FLOW_DYB_RETURN',
                       'APS_SYNC_PD_RECIPE_EQP_MAIN',
                       'APS_MID_FHOPEHS_RETICLE_VIEW',
                       'APS_ETL_APC_RESULT',
                       'APS_SYNC_WET_ED_DATA',
                       'APS_SYNC_OUT_DF_PURGE_COMBINE',
                       'APS_SYNC_MAJOR_ANOMALY_EWS_LOT',
                       'APS_SYNC_SCHE_PM_PROP_TIME'
                       ]

# 哪个时间点的时候，做一次将历史DuckDB变小，可设值 [00-23]
g_resize_his_db_hour = ['11', '22']

# 代表在95上，生產代碼+測試代碼混用模式: !!!重要 ，最終生產環境為 False
g_test_and_prod_mixed = False
# 已經上綫的ETL的PG產出表
g_etl_prod_tables = ['etl_flow',
                     'etl_wip'
                     ]

# PG的測試的測試環境
g_postgres_test_test_host = '10.52.192.99'
g_postgres_test_test_database = 'dev_uo'
g_postgres_test_test_port = '5432'
g_postgres_test_test_user = 'xxetl'
g_postgres_test_test_password = 'xxetl1234'
g_pgserver_test_test_path = '\\\\10.52.192.99\\ETLOutput'

# g_postgres_test_test_host = '10.52.192.99'
# g_postgres_test_test_database = 'uo'
# g_postgres_test_test_port = '5433'
# g_postgres_test_test_user = 'xxetl'
# g_postgres_test_test_password = 'xxetl1234'
# g_pgserver_test_test_path = '\\\\10.52.192.99\\ETLOutput'

my_ip = "10.52.192.95"
other_ip = "10.52.192.94"