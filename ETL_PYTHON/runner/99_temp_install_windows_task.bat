
schtasks /create /tn xinxiang_aps_etl_wip /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_wip.vbs /sc minute /mo 5
schtasks /create /tn xinxiang_aps_etl_flow /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_flow.vbs /sc hourly /mo 1
schtasks /create /tn xinxiang_aps_etl_write_back_to_oracle_wip /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_write_back_to_oracle_wip.vbs /sc minute /mo 5
schtasks /create /tn xinxiang_aps_etl_write_back_to_oracle_flow /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_write_back_to_oracle_flow.vbs /sc hourly /mo 1
pause