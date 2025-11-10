import warnings

import duckdb
import pandas as pd


def join_all_attach_to_one(file_name):
    warnings.filterwarnings("ignore")
    print(file_name)
    duck_file = duckdb.connect(file_name)
    sql = """
    select * from APS_SRCFILE
    """
    pd_result = pd.read_sql(sql, duck_file)

    # 遍历所有参考表
    for row in pd_result.index:
        table_name = pd_result.loc[row]['table_name']
        file_name = pd_result.loc[row]['file_name']
        print(table_name, file_name)

        sql = "ATTACH '{file_name}' AS {db_name} (READ_ONLY)".format(file_name=file_name, db_name=table_name)
        duck_file.sql(sql)

        sql = "select * from {table_name}.{table_name}".format(table_name=table_name)
        _temp = pd.read_sql(sql, duck_file)
        duck_file.sql(" drop table if exists {table_name}".format(table_name=table_name))
        duck_file.sql(" create table {table_name} as select * from _temp".format(table_name=table_name))

        insert_sql = "INSERT INTO {} select * from _temp".format(table_name)
        duck_file.sql(insert_sql)

    duck_file.close()
    del duck_file


if __name__ == '__main__':
    join_all_attach_to_one("d:\\debug.db") # 这里改成你的文件
    print("finish")