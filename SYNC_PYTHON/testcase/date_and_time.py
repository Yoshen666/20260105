import time
import unittest
from datetime import datetime

from xinxiang.util import my_date


class TestDateAndTime(unittest.TestCase):

    def test_01(self):
        print(my_date.date_str())
        print(my_date.date_time_minute_str())
        print(my_date.date_time_second_str())
        print(my_date.date_time_min_second_str())

        start = datetime.now()
        time.sleep(2)
        end = datetime.now()
        print(my_date.duration(start, end))