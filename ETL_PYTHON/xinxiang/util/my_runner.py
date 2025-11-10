import logging
import socket
import traceback




def judge_main_server(oracle_conn):
    """
    根据服务器名称取当前服务器是否为主服务器
    True：是主服务器
    False：为备用服务器

    TODO: 要配置服务器信息
    -- Create table
    create table APS_ETL_DETL_SERVICE
    (
      srv_name         VARCHAR2(50),
      srv_ip           VARCHAR2(30),
      srv_port         VARCHAR2(20),
      srv_type         VARCHAR2(50),
      srv_status       VARCHAR2(20),
      is_main_dispatch VARCHAR2(10),
      bind_srv_name    VARCHAR2(50),
      memo             VARCHAR2(500),
      event_user       VARCHAR2(10),
      event_time       DATE,
      edit_user        VARCHAR2(10),
      edit_time        DATE,
      last_online_time DATE,
      cpu_rate         VARCHAR2(20),
      ram_rate         VARCHAR2(20)
    )
    tablespace USERS
      pctfree 10
      initrans 1
      maxtrans 255
      storage
      (
        initial 64K
        next 1M
        minextents 1
        maxextents unlimited
      )
    nologging;
    """
    host_name = socket.gethostname()
    sql =  """
    select * from (
    select  IS_MAIN_DISPATCH, row_number() over(order by SRV_NAME) as rn   from APS_ETL_DETL_SERVICE
        where upper(SRV_NAME) = upper('{server_name}') 
    ) where rn =1
    """.format(server_name=host_name)
    print(sql)
    dbcursor = None
    try:
        dbcursor = oracle_conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchone()
        if result is not None:
            if result[0] is not None and result[0] != "":
                if 'Y' == result[0]:
                    return True
                else:
                    return False
            else:
                print("Oracle的DETL_SERVICE中未配置主从服务器")
                raise Exception("Oracle的DETL_SERVICE中未配置主从服务器")
        else:
            print("Oracle的DETL_SERVICE中未配置主从服务器")
            raise Exception("Oracle的DETL_SERVICE中未配置主从服务器")
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e
    finally:
        if dbcursor:
            dbcursor.close()


def judge_backup_server_status(oracle_conn, other_ip):
    """
    根据IP取备援服务器是否正常工作
    True：是备援务器正常工作
    False：是备援务器已停止工作

    TODO: 要配置服务器信息
    -- Create table
    create table APS_ETL_DETL_SERVICE
    (
      srv_name         VARCHAR2(50),
      srv_ip           VARCHAR2(30),
      srv_port         VARCHAR2(20),
      srv_type         VARCHAR2(50),
      srv_status       VARCHAR2(20),
      is_main_dispatch VARCHAR2(10),
      bind_srv_name    VARCHAR2(50),
      memo             VARCHAR2(500),
      event_user       VARCHAR2(10),
      event_time       DATE,
      edit_user        VARCHAR2(10),
      edit_time        DATE,
      last_online_time DATE,
      cpu_rate         VARCHAR2(20),
      ram_rate         VARCHAR2(20)
    )
    tablespace USERS
      pctfree 10
      initrans 1
      maxtrans 255
      storage
      (
        initial 64K
        next 1M
        minextents 1
        maxextents unlimited
      )
    nologging;
    """
    host_name = socket.gethostname()
    sql =  """
    select * from (
    select  srv_status, row_number() over(order by SRV_NAME) as rn   from APS_ETL_DETL_SERVICE
        where upper(srv_ip) = upper('{other_ip}') 
    ) where rn =1
    """.format(other_ip=other_ip)
    print(sql)
    dbcursor = None
    try:
        dbcursor = oracle_conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchone()
        if result is not None:
            if result[0] is not None and result[0] != "":
                if 'RUN' == result[0]:
                    return True
                else:
                    return False
            else:
                print("Oracle的DETL_SERVICE中未配置备援服务器")
                raise Exception("Oracle的DETL_SERVICE中未配置备援服务器")
        else:
            print("Oracle的DETL_SERVICE中未配置备援服务器")
            raise Exception("Oracle的DETL_SERVICE中未配置备援服务器")
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e
    finally:
        if dbcursor:
            dbcursor.close()


def judge_server_change_plan_job(oracle_conn):

    host_name = socket.gethostname()
    sql =  """
    select * from (
    select  IS_CHANGE_PLAN_JOB, row_number() over(order by SRV_NAME) as rn   from APS_ETL_DETL_SERVICE
        where upper(SRV_NAME) = upper('{server_name}') 
    ) where rn =1
    """.format(server_name=host_name)
    print(sql)
    dbcursor = None
    try:
        dbcursor = oracle_conn.cursor()
        dbcursor.execute(sql)
        result = dbcursor.fetchone()
        if result is not None:
            if result[0] is not None and result[0] != "":
                if '1' == result[0]:
                    return True
                else:
                    return False
            else:
                print("Oracle的DETL_SERVICE中已执行过BAT档")
                raise Exception("Oracle的DETL_SERVICE中已执行过BAT档")
        else:
            print("Oracle的DETL_SERVICE中已执行过BAT档")
            raise Exception("Oracle的DETL_SERVICE中已执行过BAT档")
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e
    finally:
        if dbcursor:
            dbcursor.close()
