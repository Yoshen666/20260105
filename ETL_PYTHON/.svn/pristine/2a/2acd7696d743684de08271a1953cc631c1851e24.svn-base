import os
import warnings

import duckdb
import pandas
import pandas as pd
import numpy as np

from xinxiang import config
from xinxiang.util import my_oracle, my_file, my_duck

max_row_limit = 1000


def diff_row_pandas(table_name, duck_pandas, oracle_pandas):
    duck_pandas.fillna("")
    oracle_pandas.fillna("")

    # diff_pd = pd.DataFrame(columns=duck_pandas.columns)
    print(table_name + ">>>>对比中")
    diff_file_name = config.g_debug_file_path + "\\debug_diff_row_{}.txt".format(table_name)
    if os.path.exists(diff_file_name):
        os.remove(diff_file_name)
    diff_file = open(diff_file_name, 'w')

    if oracle_pandas.shape[0] > 0 and duck_pandas.shape[0] > 0:
        meansuer = np.vectorize(len)
        length_duckdb =  meansuer(duck_pandas.values.astype(str)).max(axis=0)
        # print(length_duckdb)
        length_oracle = meansuer(oracle_pandas.values.astype(str)).max(axis=0)
        # print(length_oracle)

        ###############
        # 计算 oracle 数据 duckdb数据 表头名称，看哪个长度最长
        _col_len_list = []
        _header = "T"
        _header_length = duck_pandas.columns.map(len) #列 名名称长度

        for col in range(duck_pandas.shape[1]):
            col_name = duck_pandas.columns[col]
            max_len = length_duckdb[col]
            if max_len < length_oracle[col]:
                max_len = length_oracle[col]
            if max_len < _header_length[col]:
                max_len = _header_length[col]
            _col_len_list.append(max_len)

            _header = _header + "|{:<{}}".format(col_name, max_len)
        diff_file.write(_header + "\n")
        diff_file.write("_"*len(_header) + "\n")
        ################

        # print(duck_pandas.shape[1])
        min_row_count = duck_pandas.shape[0]
        if duck_pandas.shape[0] != oracle_pandas.shape[0]:
        #     print(table_name + "件数不一致，请注意！！")
        #     diff_file.write("件数不一致:DuckDB:{}-Oracle:{}\n".format(duck_pandas.shape[0], oracle_pandas.shape[0] ))
            if min_row_count > oracle_pandas.shape[0]:
                min_row_count = oracle_pandas.shape[0]

        for row in range(min_row_count): # 只输出最小的行数的，其他不输出
            for col in range(duck_pandas.shape[1]):
                col_name = duck_pandas.columns[col]
                if col_name.lower() != "partcode" and col_name.lower() != 'update_time' and col_name.lower() != 'parentid':
                    if duck_pandas.iloc[row, col] != oracle_pandas.iloc[row, col]:
                        _result = "K"
                        for _col in range(duck_pandas.shape[1]):
                            _temp = "-"
                            _col_name = duck_pandas.columns[_col]
                            if duck_pandas.iloc[row, _col] is not None \
                                    and _col_name.lower() != "partcode" \
                                    and _col_name.lower() != "update_time" \
                                    and _col_name.lower() != "parentid" \
                                    and _col_name.lower() != "parent_id":
                                _temp = duck_pandas.iloc[row, _col]
                            try:
                                _result = _result + "|{:<{}}".format(_temp, _col_len_list[_col])
                            except:
                                _result = _result + "|{:<{}}".format("", _col_len_list[_col])
                        diff_file.write(_result + "\n")

                        _result = "O"
                        for _col in range(oracle_pandas.shape[1]):
                            _temp = "-"
                            _col_name = duck_pandas.columns[_col]
                            if duck_pandas.iloc[row, _col] is not None \
                                    and _col_name.lower() != "partcode" \
                                    and _col_name.lower() != "update_time" \
                                    and _col_name.lower() != "parentid" \
                                    and _col_name.lower() != "parent_id":
                                _temp = oracle_pandas.iloc[row, _col]
                            try:
                                _result = _result + "|{:<{}}".format(_temp, _col_len_list[_col])
                            except:
                                _result = _result + "|{:<{}}".format("", _col_len_list[_col])
                        diff_file.write(_result + "\n")

                        diff_file.write("\n")

                        continue
        diff_file.close()


def diff_row_content(file_name):
    warnings.filterwarnings("ignore")

    duck_db = duckdb.connect(file_name)
    oracle_db = my_oracle.oracle_get_connection()

    sql = "SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
    cursor = duck_db.execute(sql)
    table_list = cursor.fetchall()

    for table_name in table_list:
        table = table_name[0]
        if "APS_SRCFILE" != table:
            sql_duck = "select * from {}".format(table)

            # 内容比较
            # ——————————————————————————————————————————————————————————————————————————————————————————————————————————
            result_temp = None
            result_temp = duck_db.execute(sql_duck)
            columns = " DESC, ".join([col[0] for col in result_temp.description])
            _order_condition = " ORDER BY " + columns
            _column_condition = " , ".join([col[0] for col in result_temp.description])

            # Duck DB 检索SQL
            sql_duck_search = "select " + _column_condition + " from " + table + _order_condition + " LIMIT " + str(max_row_limit)
            pd_resul_duck = pd.read_sql_query(sql_duck_search, duck_db)
            pd_resul_duck.fillna("")

            # ORACLE 检索SQL
            pd_result_oracle = None
            partcode = my_oracle.GetLastPartCodeData(oracle_db, table)
            try:
                try:
                    sql_oracle = "select {colunms} from {table} where partcode='{partcode}' and ROWNUM <= {limit}".format(
                        colunms=_column_condition, table=table, partcode=partcode, limit=max_row_limit)
                    sql_oracle_search = sql_oracle + _order_condition
                    pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_db)
                except pandas.errors.DatabaseError as e:
                    sql_oracle = "select {colunms} from {table} where 1=1 and ROWNUM <= {limit} ".format(
                        colunms=_column_condition, table=table, limit=max_row_limit)
                    sql_oracle_search = sql_oracle + _order_condition
                    pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_db)
                pd_result_oracle.fillna("")

                diff_row_pandas(table, pd_resul_duck, pd_result_oracle)

            except Exception as ex:
                print(table + "：字段不一致")
                continue
            # ——————————————————————————————————————————————————————————————————————————————————————————————————————————

    duck_db.close()
    del duck_db
    oracle_db.close()
    del oracle_db


def diff_row_count(file_name, diff_file):
    warnings.filterwarnings("ignore")

    duck_db = duckdb.connect(file_name)
    oracle_db = my_oracle.oracle_get_connection()

    sql = "SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
    cursor = duck_db.execute(sql)
    table_list = cursor.fetchall()

    for table_name in table_list:
        table = table_name[0]
        if "APS_SRCFILE" != table:
            sql_duck = "select * from {}".format(table)
            # 行数比较
            # ——————————————————————————————————————————————————————————————————————————————————————————————————————————
            sql_for_count = "select count(1) from {}".format(table)
            duck_db.execute(sql_for_count)
            dk_result = duck_db.fetchall()
            row_in_duck = dk_result[0][0]


            dbcursor = oracle_db.cursor()
            dbcursor.execute(sql_for_count)
            or_result = dbcursor.fetchall()
            row_in_oracle = or_result[0][0]

            diff_file.write("DUCK DB ROW|" + table + " | " + str(row_in_duck) + "\n")
            diff_file.write("Oracle  ROW|" + table + " | " + str(row_in_oracle) + "\n")
            diff_file.write("件数相等关系:" + str(row_in_duck == row_in_oracle) + "\n")
            diff_file.write("件数三倍关系:" + str(3*row_in_duck == row_in_oracle) + "\n")
            if not (row_in_duck == row_in_oracle) and not(3*row_in_duck == row_in_oracle):
                diff_file.write("件数不一致也无法解释\n")
            diff_file.write("————————————————————————————————————————————————————————————————————————————————\n")

            # 内容比较
            # ——————————————————————————————————————————————————————————————————————————————————————————————————————————
            # result_temp = None
            # result_temp = duck_db.execute(sql_duck)
            # columns = " DESC, ".join([col[0] for col in result_temp.description])
            # _order_condition = " ORDER BY " + columns
            # _column_condition = " , ".join([col[0] for col in result_temp.description])
            #
            # # Duck DB 检索SQL
            # sql_duck_search = "select " + _column_condition + " from " + table + _order_condition + " LIMIT " + str(max_row_limit)
            # pd_resul_duck = pd.read_sql_query(sql_duck_search, duck_db)
            # pd_resul_duck.fillna("")
            #
            # # ORACLE 检索SQL
            # pd_result_oracle = None
            # partcode = my_oracle.GetLastPartCodeData(oracle_db, table)
            # try:
            #     try:
            #         sql_oracle = "select {colunms} from {table} where partcode='{partcode}' and ROWNUM <= {limit}".format(
            #             colunms=_column_condition, table=table, partcode=partcode, limit=max_row_limit)
            #         sql_oracle_search = sql_oracle + _order_condition
            #         pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_db)
            #     except pandas.errors.DatabaseError as e:
            #         sql_oracle = "select {colunms} from {table} where 1=1 and ROWNUM <= {limit} ".format(
            #             colunms=_column_condition, table=table, limit=max_row_limit)
            #         sql_oracle_search = sql_oracle + _order_condition
            #         pd_result_oracle = pd.read_sql_query(sql_oracle_search, oracle_db)
            #     pd_result_oracle.fillna("")
            #
            #     diff_row_pandas(table, pd_resul_duck, pd_result_oracle)
            #
            # except Exception as ex:
            #     print(table + "：字段不一致")
            #     continue
            # ——————————————————————————————————————————————————————————————————————————————————————————————————————————

    duck_db.close()
    del duck_db
    oracle_db.close()
    del oracle_db


def join_all_attach_to_one(file_name, diff_result_file):
    warnings.filterwarnings("ignore")
    duck_file = duckdb.connect(file_name)
    oracle_conn = my_oracle.oracle_get_connection()

    sql = """
    select * from APS_SRCFILE
    """
    pd_result = pd.read_sql(sql, duck_file)

    # 遍历所有参考表
    for row in pd_result.index:
        table_name = pd_result.loc[row]['table_name']
        file_name = pd_result.loc[row]['file_name']

        print("ATTACH", table_name, file_name)

        sql = "ATTACH '{file_name}' AS {db_name} (READ_ONLY)".format(file_name=file_name, db_name=table_name)
        duck_file.sql(sql)

        sql = "select * from {table_name}.{table_name}".format(table_name=table_name)
        _temp = pd.read_sql(sql, duck_file)
        duck_file.sql(" drop table if exists {table_name}".format(table_name=table_name))
        duck_file.sql(" create table {table_name} as select * from _temp".format(table_name=table_name))

        row_in_duck = my_duck.get_row_count_in_duckdb(duck_file, table_name)
        row_in_oracle = my_oracle.get_row_count_in_oracle(oracle_conn, table_name)

        diff_result_file.writelines(">>>|ATTACH|>>> DUCK DB ROW|" + table_name + " | " + str(row_in_duck) + "\n")
        diff_result_file.writelines(">>>|ATTACH|>>> Oracle  ROW|" + table_name + " | " + str(row_in_oracle) + "\n")
        diff_result_file.write("件数相等关系:" + str(row_in_duck == row_in_oracle) + "\n")
        diff_result_file.write("件数三倍关系:" + str(3 * row_in_duck == row_in_oracle) + "\n")
        if not (row_in_duck == row_in_oracle) and not (3 * row_in_duck == row_in_oracle):
            diff_result_file.write("件数不一致也无法解释\n")
        diff_result_file.writelines("————————————————————————————————————————————————————————————————————————————————\n")

    oracle_conn.close()
    duck_file.close()
    del duck_file


if __name__ == '__main__':
    my_file.init_folder()
    # TODO 也可以改成你自己的本地其他文件
    for root, dirs, files in os.walk(config.g_debug_file_path):
        for file in files:
            if file.endswith(".txt"):
                os.remove(os.path.join(root, file))

    diff_file_name = config.g_debug_file_path + "\\debug_diff_count.txt".format()
    if os.path.exists(diff_file_name):
        os.remove(diff_file_name)

    diff_result_file = open(diff_file_name, 'w')
    # 行数比较
    diff_row_count(config.g_debug_file, diff_result_file)
    # 在DuckDB中引入所有记录在APS_SRCFILE中的被使用的表，并汇入到.db文件中
    join_all_attach_to_one(config.g_debug_file, diff_result_file)
    diff_result_file.close()
    # 详细比较
    diff_row_content(config.g_debug_file)
    print("---------FINISH------------")