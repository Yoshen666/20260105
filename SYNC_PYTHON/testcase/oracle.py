import os
import sys
import unittest
import warnings

import jaydebeapi

from xinxiang import config
import pandas as pd

from xinxiang.util import my_oracle, my_file


class TestOracle(unittest.TestCase):
    def test_01(self):
        warnings.filterwarnings("ignore")

        conn = my_oracle.oracle_get_connection()
        sql = "select count(1) from dual"
        dbcursor = conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchone()
        print(result[0])

    def test_02(self):
        warnings.filterwarnings("ignore")
        conn = my_oracle.oracle_get_connection()
        sql = "select 1 as A, 2 as B from dual"
        dbcursor = conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchone()
        print(result)

    def test_03(self):
        warnings.filterwarnings("ignore")
        conn = my_oracle.oracle_get_connection()

        sql = "select 1 as A, 2 as B from dual"
        result = pd.read_sql_query(sql, conn)
        print(result)
        print(result['B'][0])

        conn.commit()
        conn.close()

    # def test_04(self):
    #     warnings.filterwarnings("ignore")
    #     conn = common.oracle_get_connection()
    #     print(common.GetLastPartCodeData(conn, "APS_ETL_HOLD_RLS"))
    #     conn.commit()
    #     conn.close()

    def test_05(self):
        warnings.filterwarnings("ignore")
        conn = my_oracle.oracle_get_connection()
        print(my_oracle.GetApsParamInfos(conn, "TMP_VIEW_MAPPING", 'V_EQP_STATUS_BOOKING_SETTING'))
        conn.commit()
        conn.close()

    def test_06(self):
        warnings.filterwarnings("ignore")
        conn = my_oracle.oracle_get_connection()
        dbcursor = conn.cursor()
        # sql = """
        # SELECT DBMS_METADATA.GET_DDL('TABLE', 'ALARM_SEND_LOG')
        # FROM DUAL
        # """
        sql = """
           SELECT * FROM DBA_TAB_COLS A WHERE A.TABLE_NAME = 'ALARM_SEND_LOG'
        """
        dbcursor.execute(sql)
        meta = dbcursor.fetchall()
        for col in meta:
            print(col[2], col[3], col[6])


        dbcursor.close()
        conn.close()

    def test_07(self):
        warnings.filterwarnings("ignore")

        table_name = 'APS_SYNC_FVLOT_QTIME'

        conn = my_oracle.oracle_get_connection()
        dbcursor = conn.cursor()
        # sql = """
        # SELECT DBMS_METADATA.GET_DDL('TABLE', 'ALARM_SEND_LOG')
        # FROM DUAL
        # """
        create_sql = " CREATE TABLE {table_name} ( ".format(table_name=table_name)

        sql = """
           SELECT * FROM DBA_TAB_COLS A WHERE A.TABLE_NAME = '{table_name}'
        """.format(table_name=table_name)
        dbcursor.execute(sql)
        meta = dbcursor.fetchall()

        for col in meta:
            column_name = col[2]
            column_type = col[3]
            column_length = col[6]
            if "VARCHAR2" == column_type:
                create_sql = create_sql + " {column_name} VARCHAR({column_length}), ".format(
                    column_name=column_name,
                    column_type=column_type,
                    column_length=column_length
                )
            if "NUMBER" == column_type:
                create_sql = create_sql + " {column_name} DECIMAL, ".format(
                    column_name=column_name,
                    column_type=column_type,
                    column_length=column_length
                )
            if "NUMBER" == column_type:
                create_sql = create_sql + " {column_name} DECIMAL, ".format(
                    column_name=column_name,
                    column_type=column_type,
                    column_length=column_length
                )
            if "DATE" == column_type:
                create_sql = create_sql + " {column_name} TIMESTAMP, ".format(
                    column_name=column_name,
                    column_type=column_type,
                    column_length=column_length
                )
            if "FLOAT" == column_type:
                create_sql = create_sql + " {column_name} DOUBLE PRECISION, ".format(
                    column_name=column_name,
                    column_type=column_type,
                    column_length=column_length
                )

        create_sql = create_sql + ")"
        print(create_sql)
        pos = create_sql.rfind(",")
        if pos != -1:
            create_sql = create_sql[:pos] + create_sql[pos+1:]
        print(create_sql)

    def test_08(self):
        warnings.filterwarnings("ignore")
        conn = my_oracle.oracle_get_connection()
        print(my_file.get_last_db_file(conn, "APS_SYNC_SCHE_APC_PB"))
        conn.commit()
        conn.close()
