import logging

from xinxiang.util import my_oracle


def GetApsParamInfos(conn, DicType):
    sql = """
    SELECT DIC_TYPE,
           DIC_KEY,
           DIC_VALUE,
           DIC_SEQ,
           DIC_PARAM,
           DIC_PARAM2,
           DIC_PARAM3,
           DIC_PARAM4,
           CREATE_USER,
           CREATE_TIME,
           UPDATE_USER,
           UPDATE_TIME,
           DIC_PARAM5,
           DIC_PARAM6,
           DIC_PARAM7,
           DIC_PARAM8
    FROM   APS_ETL_SYS_PARAM T
    WHERE  T.DIC_TYPE = '{}'
    """.format(DicType)
    dbcursor = None
    try:
        dbcursor = conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchall()
        return result
    except Exception as e:
        logging.error(e)
        raise e
    finally:
        if dbcursor:
            dbcursor.close()

def create_func(source, target, cron):
    temp = """

def sync_{target}():
    # Corn In DataBase: {cron}
    source_table = "{source}"
    target_table = "{target}"
    etl_name = "Sync " + source_table + " to " + target_table

    logging.info("Start " + etl_name)
    base_oracle_to_duck.sync_oracle_to_duck(etl_name=etl_name, source_table=source_table, target_table=target_table)
    logging.info("End " + etl_name)

    """.format(source=source, target=target, cron=cron)
    return  temp

def create_trigger(source, target, cron):
    temp = """
    # {source} > {target}
    # Corn In Database:{cron}
    schedule.add_job(sync_view_jobs.sync_{target}, "cron", second="2-59/5") # TODO

""".format(
        target=target,
        source=source,
        cron=cron
    )
    return  temp


if __name__ == "__main__":
    conn = my_oracle.oracle_get_connection();
    result = GetApsParamInfos(conn, "TMP_VIEW_MAPPING")

    with open("a.txt", 'w') as file:
        for item in result:
            _str = ""
            if item[14] is not None:
                _str = item[14]
            print(item[2] + ", " + item[5] + ", " + _str)

            temp = create_func(item[2], item[5], _str)
            # temp = create_trigger(item[2], item[5], _str)

            file.write(temp + "\n")


    conn.close()

