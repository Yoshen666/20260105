schtasks /create /tn xinxiang_aps_etl_pri_wip /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_pri_wip.vbs /sc minute /mo 10 /st 00:09 /f
schtasks /create /tn xinxiang_aps_etl_lothistory /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_lothistory.vbs /sc minute /mo 10 /st 00:09 /f
schtasks /create /tn xinxiang_aps_etl_lot_op_hist /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_lot_op_hist.vbs /sc hourly /mo 1 /st 00:55 /f
schtasks /create /tn xinxiang_aps_etl_mask_history /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_mask_history.vbs /sc hourly /mo 1 /st 00:40 /f
schtasks /create /tn xinxiang_aps_etl_sgs_rls /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_sgs_rls.vbs /sc hourly /mo 1 /st 00:50 /f
schtasks /create /tn xinxiang_aps_etl_pirun /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_pirun.vbs /sc minute /mo 10 /st 00:08 /f
schtasks /create /tn xinxiang_aps_etl_sgs_rule /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_sgs_rule.vbs /sc hourly /mo 12 /st 01:00 /f
schtasks /create /tn xinxiang_aps_etl_mask_info /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_mask_info.vbs /sc minute /mo 5 /st 00:04 /f
schtasks /create /tn xinxiang_aps_etl_demand /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_demand.vbs /sc minute /mo 10 /st 00:09 /f
schtasks /create /tn xinxiang_aps_etl_special_cons /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_special_cons.vbs /sc minute /mo 10 /st 00:09 /f
schtasks /create /tn xinxiang_aps_etl_monitor /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_etl_monitor.vbs /sc minute /mo 10 /st 00:09 /f

schtasks /create /f /tn xinxiang_aps_his_aps_mid_fhopehs_reticle /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_mid_fhopehs_reticle.vbs /sc minute /mo 5 /st 00:04 /f
schtasks /create /f /tn xinxiang_aps_his_aps_mid_ocs_job_group_po_state_h /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_mid_ocs_job_group_po_state_h.vbs /sc minute /mo 10 /st 00:08 /f
schtasks /create /f /tn xinxiang_aps_his_aps_mid_rspilot_run_hs /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_mid_rspilot_run_hs.vbs /sc minute /mo 5 /st 00:01 /f
schtasks /create /f /tn xinxiang_aps_his_aps_tmp_lothistory /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_tmp_lothistory.vbs /sc minute /mo 5 /st 00:01 /f
schtasks /create /f /tn xinxiang_aps_his_aps_tmp_mask_history /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_tmp_mask_history.vbs /sc minute /mo 5 /st 00:04 /f
schtasks /create /f /tn xinxiang_aps_his_aps_tmp_ocs_main_h /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_tmp_ocs_main_h.vbs /sc minute /mo 10 /st 00:08 /f
schtasks /create /tn xinxiang_aps_his_aps_mid_ph_lothistory /tr D:\XinXiang\ETL_PYTHON\runner\run_aps_his_aps_mid_ph_lothistory.vbs /sc minute /mo 5 /st 00:01 /f

schtasks /delete /f /tn "xinxiang_aps_etl_monitor"
schtasks /delete /f /tn "xinxiang_aps_etl_pri_wip"
schtasks /delete /f /tn "xinxiang_aps_etl_lothistory"
schtasks /delete /f /tn "xinxiang_aps_etl_lot_op_hist"
schtasks /delete /f /tn "xinxiang_aps_etl_mask_history"
schtasks /delete /f /tn "xinxiang_aps_etl_sgs_rls"
schtasks /delete /f /tn "xinxiang_aps_etl_pirun"
schtasks /delete /f /tn "xinxiang_aps_etl_sgs_rule"
schtasks /delete /f /tn "xinxiang_aps_etl_mask_info"
schtasks /delete /f /tn "xinxiang_aps_etl_demand"
schtasks /delete /f /tn "xinxiang_aps_etl_special_cons"

schtasks /delete /f /tn "xinxiang_aps_his_aps_mid_fhopehs_reticle"
schtasks /delete /f /tn "xinxiang_aps_his_aps_mid_ocs_job_group_po_state_h"
schtasks /delete /f /tn "xinxiang_aps_his_aps_mid_rspilot_run_hs"
schtasks /delete /f /tn "xinxiang_aps_his_aps_tmp_lothistory"
schtasks /delete /f /tn "xinxiang_aps_his_aps_tmp_mask_history"
schtasks /delete /f /tn "xinxiang_aps_his_aps_tmp_ocs_main_h"
schtasks /delete /f /tn "xinxiang_aps_his_aps_mid_ph_lothistory"

