import logging

import duckdb

from xinxiang.util import oracle_to_duck_common

if __name__ == '__main__':
    source_table = "APS_ETL_TOOL"
    target_table = "APS_ETL_TOOL"
    etl_name = "Sync " + source_table + " to " + target_table

    logging.info("Start " + etl_name)
    # query_sql = """SELECT LOT_ID, MAINPD_ID, OPE_NO, ANNOTATION_FLAG, UPDATE_TIME FROM V_LOT_ANNOTATION_INFO_BRANCH"""
    # create_table_sql = """
    #             CREATE TABLE APS_SYNC_LOT_ANNOTATION_INFO_BRANCH (
    #                   LOT_ID          VARCHAR(64),
    #                   MAINPD_ID       VARCHAR(64),
    #                   OPE_NO          VARCHAR(64),
    #                   ANNOTATION_FLAG VARCHAR(1),
    #                   UPDATE_TIME     TIMESTAMP
    #             )
    #             """
    oracle_to_duck_common.sync_oracle_to_duck(etl_name=etl_name,
                                                     source_table=source_table,
                                                     target_table=target_table)
    logging.info("End " + etl_name)
