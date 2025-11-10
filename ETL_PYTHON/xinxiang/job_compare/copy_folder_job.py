import os
import difflib
import logging
import uuid
from xinxiang import config
from xinxiang.util import my_runner,my_oracle,cons_error_code,my_date

def execute():
    oracle_conn = None
    oracle_dbcursor = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        oracle_dbcursor = oracle_conn.cursor()
        sql_query = """
            select  file_name
            from APS_ETL_VER_CONTROL_DUCK t 
            where t.update_time > TO_CHAR(SYSDATE -1, 'YYYYMMDD')
            and file_name is not null
        """
        oracle_dbcursor.execute(sql_query)
        result = oracle_dbcursor.fetchall()

        print(result)
        for item in result:
            folder_name = os.path.dirname(item[0])
            # 确保日志目录存在
            if not os.path.isdir(folder_name):
                os.makedirs(folder_name)
    except Exception as e:
        logging.error(e)

    finally:
        if oracle_dbcursor is not None:
            oracle_dbcursor.close()
        if oracle_conn is not None:
            oracle_conn.close()


if __name__ == '__main__':
    execute()

