import logging
import os
import warnings

from xinxiang import config
from xinxiang.util import my_log, oracle_to_duck_common, my_oracle

warnings.filterwarnings("ignore")
my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_ETL.log"))

if __name__ == '__main__':
    conn = my_oracle.oracle_get_connection()
    source_table = "APS_TR_CSFRINHIBIT"
    target_table = "APS_TR_CSFRINHIBIT"
    etl_name = "Sync " + source_table + " to " + target_table
    query_sql = """SELECT parentid, productid, routeid, openo, equipmentid, pass_flg_prc, pass_flg_mfg, prc_upt_user, update_time, partcode, create_time, prc_update_time, mfg_update_time, chipbody FROM APS_TR_CSFRINHIBIT where partcode = '6000' """
    create_table_sql = """
    CREATE TABLE APS_TR_CSFRINHIBIT (
            parentid        VARCHAR(60),
            productid       VARCHAR(60),
            routeid         VARCHAR(60),
            openo           VARCHAR(60),
            equipmentid     VARCHAR(60),
            pass_flg_prc    VARCHAR(60),
            pass_flg_mfg    VARCHAR(60),
            prc_upt_user    VARCHAR(60),
            update_time     VARCHAR(60),
            partcode        VARCHAR(60),
            create_time     VARCHAR(64),
            prc_update_time VARCHAR(64),
            mfg_update_time VARCHAR(64),
            chipbody        VARCHAR(64)
    );
    """
    oracle_to_duck_common.sync_oracle_to_duck_by_csv(etl_name=etl_name,
                                                     source_table=source_table,
                                                     target_table=target_table,
                                                     query_sql=query_sql,
                                                     create_table_sql=create_table_sql)
