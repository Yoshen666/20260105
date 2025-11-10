import os
import duckdb as dk

def test():
    file_name = '_row_count_APS_TMP_ETL_WIP_QTIME_FLOW.txt'
    table_name = file_name.replace("_row_count_", "").replace(".txt", '')
    print(table_name)

def scan(path):
    result_file = os.path.join(path, '_result.txt')
    if os.path.exists(result_file):
        os.remove(result_file)
    with open(result_file, 'w') as fmain:
        files = os.listdir(path)
        for file in files:
            if file.endswith(".txt") and file != '_result.txt':
                table_name = file.replace("_row_count_", "").replace(".txt", '')
                fmain.writelines(table_name + "\n")
                fmain.writelines("--count" + "\n")
                with open(os.path.join(path, file)) as fjianshu:
                    fmain.write("::::::::::::::::::::::::::::::::::" + "\n")
                    read_data = fjianshu.read()
                    fmain.write(read_data)

                db_file_name = table_name.lower() + "." + table_name.lower() + "_0.db"
                if not os.path.exists(os.path.join(path, db_file_name)):
                    db_file_name = 'tempdb.' + table_name.lower() + "_0.db"
                db_file_path = os.path.join(path, db_file_name)
                try:
                    duckdb = dk.connect(db_file_path)

                    sql = """select * from {table_name}""".format(table_name=table_name)
                    print(table_name, sql, db_file_name)
                    pd_result = duckdb.execute(sql).df()

                    has_lot = False
                    if 'lot_id' in pd_result.columns or 'LOT_ID' in pd_result.columns:
                        print(f'{table_name}??????????')
                        has_lot = True

                    for _col in pd_result.columns:
                        if not _col.lower().startswith("o_") and not _col.lower().startswith(
                                "k_") and not _col == 'parentid' and not _col == 'partcode' and not _col == 'update_time':

                            if _col in pd_result.columns:
                                condition = pd_result[_col] == 'N'
                                if not has_lot:
                                    aa = pd_result.loc[condition, ['K_' + _col, 'O_' + _col]]  # 选择指定的列
                                    if aa.shape[0] > 0:
                                        fmain.writelines("---------------------------" * 3 + "\n")
                                        fmain.write(f'A:Table:[{table_name}] Col:[{_col}] Unmatch Rows:' + str(aa.shape[0]) + "\n")
                                        csv_str = aa.head(5).to_csv(index=False)
                                        fmain.write(csv_str)
                                else:
                                    try:
                                        aa = pd_result.loc[condition, ['K_lot_id', 'O_lot_id', 'K_' + _col, 'O_' + _col]]  # 选择指定的列
                                        if aa.shape[0] > 0:
                                            fmain.writelines("---------------------------" * 3 + "\n")
                                            fmain.write(f'B:Table:[{table_name}] Col:[{_col}] Unmatch Rows:' + str(
                                                aa.shape[0]) + "\n")
                                            csv_str = aa.head(5).to_csv(index=False)
                                            fmain.write(csv_str)
                                    except Exception as aaaasdasd:
                                        print(aaaasdasd)
                                # if pd_result.columns.__contains__('K_' + _col) and pd_result.columns.__contains__(
                                #         'O_' + _col):
                                #     pass
                                #     # aa = pd_result.loc[condition]
                                #
                                #
                                #     # for _index, row in aa.iterrows():
                                #     #     diff =  '|'.join([f'{col}: {row[col]}' for col in aa.columns if col.startswith('K_') and row[f"O_{col}"] != row[col]] )
                                #     #     diff += '|'.join([f'{col}: {row[col]}' for col in aa.columns if col.startswith('O_') and row[f"K_{col}"] != row[col]])
                                #     #     if diff:
                                #     #         fmain.write(diff+"\n")
                                #
                                #     # aa = pd_result.loc[condition, ['K_' + _col, 'O_' + _col]]  # 选择指定的列
                                #     # if aa['K_' + _col].astype(float) == aa['O_' + _col].astype(float):
                                #     #     pass
                                #     # else:
                                #     #     # 将DataFrame转换为CSV格式的字符串
                                #     #     csv_str = aa.to_csv(index=False)
                                #     #     # 将CSV格式的字符串写入文件
                                #     #     fmain.write(csv_str)

                    duckdb.close()
                except Exception as e:
                    fmain.write(f'{e}' + "\n")
                    continue

                fmain.writelines("---------------------------"*5 + "\n")


if __name__ == "__main__":
    path =  r'd:\debug'
    scan(path)
    # test()