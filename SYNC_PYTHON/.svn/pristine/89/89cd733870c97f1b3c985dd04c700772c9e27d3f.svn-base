import os

import duckdb


def get_select_column_in_duckdb(duckdb_in_memory, table_name):
    sql = """
        select COLUMN_NAME from information_schema.columns
        where upper(table_name) = upper('{table_name}')
    """.format(table_name=table_name)
    duckdb_in_memory.execute(sql)
    columns = duckdb_in_memory.fetchall()
    columns_str = ','.join(column[0] for column in columns)
    return columns_str


from xinxiang import config
if __name__ == "__main__":
    db = duckdb.connect(r"E:\temp\APS_MID_FHOPEHS_RETICLE_20230911183236.db",read_only=True)
    cols_str = get_select_column_in_duckdb(db, 'aaa')
    print(cols_str)
    db.close()
