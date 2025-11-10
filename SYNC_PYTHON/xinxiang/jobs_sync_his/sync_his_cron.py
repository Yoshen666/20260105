from xinxiang.jobs_sync_his import sync_his_jobs
from xinxiang.util import my_cron


def set_sync_his_jobs_cron(schedule):
    # V_ETL_LOTHISTORY > APS_TMP_LOTHISTORY
    # Corn In Database:0 4/5 * * * ?
    schedule.add_job(sync_his_jobs.sync_APS_TMP_LOTHISTORY, my_cron.CronTriger6.cron_triger("0 4/5 * * * ?"),
                     misfire_grace_time=60)

    # V_RSPILOT_RUN_HS > APS_MID_RSPILOT_RUN_HS
    # # Corn In Database:0 4/5 * * * ?
    # schedule.add_job(sync_his_jobs.sync_APS_MID_RSPILOT_RUN_HS, my_cron.CronTriger6.cron_triger("0 4/5 * * * ?"),
    #                  misfire_grace_time=60)

    # V_FHOPEHS > APS_MID_FHOPEHS
    # Corn In Database:0 4/5 * * * ?
    schedule.add_job(sync_his_jobs.sync_APS_MID_FHOPEHS, my_cron.CronTriger6.cron_triger("0 4/5 * * * ?"),
                     misfire_grace_time=60)

    # V_ETL_MASK_HISTORY > APS_TMP_MASK_HISTORY
    # Corn In Database:0 4/5 * * * ?
    # schedule.add_job(sync_his_jobs.sync_APS_TMP_MASK_HISTORY, my_cron.CronTriger6.cron_triger("0 4/5 * * * ?"),
    #                  misfire_grace_time=60)
    #
    # # V_FHOPEHS_RETICLE > APS_MID_FHOPEHS_RETICLE
    # # Corn In Database:0 4/5 * * * ?
    # schedule.add_job(sync_his_jobs.sync_APS_MID_FHOPEHS_RETICLE, my_cron.CronTriger6.cron_triger("0 4/5 * * * ?"),
    #                  misfire_grace_time=60)
    #
    # # VIEW_OCS_MAIN_H > APS_TMP_OCS_MAIN_H
    # # Corn In Database:0 8/10 * * * ?
    # schedule.add_job(sync_his_jobs.sync_APS_TMP_OCS_MAIN_H, my_cron.CronTriger6.cron_triger("0 8/10 * * * ?"),
    #                  misfire_grace_time=60)
    #
    # schedule.add_job(sync_his_jobs.sync_APS_MID_OCS_JOB_GROUP_PO_STATE_H, my_cron.CronTriger6.cron_triger("0 7/10 * * * ?"),
    #                  misfire_grace_time=60)