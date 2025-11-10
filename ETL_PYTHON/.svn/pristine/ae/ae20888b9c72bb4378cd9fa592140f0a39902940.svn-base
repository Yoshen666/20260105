import logging
import socket
import time
import unittest
import warnings

import jaydebeapi

from xinxiang import config
from xinxiang.util import my_oracle, cons, my_cmder, my_log, my_date


class TestCommon(unittest.TestCase):
    def test_01(self):
        warnings.filterwarnings("ignore")
        conn = None
        try:
            conn = my_oracle.oracle_get_connection()
            _now = my_date.date_time_second_str()
            my_oracle.StartCleanUpAndLog(conn, "ETLProcNameaaaa", _now)

            time.sleep(3)
            my_oracle.EndCleanUpAndLog(conn, "ETLProcNameaaaa", _now, )

            eee = Exception("aaaaaaaa")
            my_oracle.SaveAlarmLogData(conn, ETLProcName="ETLProcNameaaaa", Exception=eee, file_name="d:/aaa.dat", alarm_code=cons.APS_ETL_HOLD_RLS_CODE_XX_ETL)
        except Exception as e:
            print(e)
            raise e
        finally:
            conn.close()

    def test_02(self):
        # print(config.g_driver_jarFile)
        warnings.filterwarnings("ignore")
        conn = None
        try:
            conn = my_oracle.oracle_get_connection()
            my_oracle.HandlingVerControl(conn, my_oracle.UUID(), cons.TABLE_BIZ_APS_ETL_RLS, "filename")
        except Exception as e:
            print(e)
            raise e
        finally:
            conn.close()

    def test_cmder(self):
        cmd = "ipconfig"
        result = my_cmder.exec(cmd)
        print(result)

    def test_log(self):
        my_log.init_log("a.log")
        logging.info("Hello")
