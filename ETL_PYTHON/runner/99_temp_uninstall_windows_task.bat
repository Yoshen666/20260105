schtasks /delete /tn "xinxiang_aps_etl_wip" /f
schtasks /delete /tn "xinxiang_aps_etl_flow" /f
schtasks /delete /tn "xinxiang_aps_etl_write_back_to_oracle_wip" /f
schtasks /delete /tn "xinxiang_aps_etl_write_back_to_oracle_flow" /f
pause