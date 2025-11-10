import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_runner


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''
    sql = """
       create {table_type} table APS_TMP_ETL_STATIC_CONS_TOOL
       (
           tool_id       VARCHAR(64),
           toolg_id      VARCHAR(64),
           module        VARCHAR(64),
           host_eqp_flag VARCHAR(64),
           eqp_chamber_flag VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_STATIC_CONS_PIRUN_DATA
       (
           TASK_NO                   VARCHAR(64),
           EQP_ID                    VARCHAR(64),
           CHAMBER                   VARCHAR(64),
           CONTROL_PROCESS           VARCHAR(64),
           STATUS                    VARCHAR(64),
           LOT_MOTHER                VARCHAR(64),
           LOT_SON                   VARCHAR(64),
           INHIBIT_CHAMBER_FLAG      VARCHAR(64), 
           INHIBIT_CHIP_GROUP_FLAG   VARCHAR(64),
           INHIBIT_RECIPE_FLAG       VARCHAR(64),
           INHIBIT_RECIPE_OPE_FLAG   VARCHAR(64),
           RULE_NO                   VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_STATIC_CONS_V1
       (
           PARENTID                VARCHAR(64),
           RULE_NAME               VARCHAR(64),
           ACTION                  VARCHAR(64),
           RULE_SEQ                VARCHAR(64),
           TOOLG_ID                VARCHAR(64),
           TOOL_ID                 VARCHAR(64),
           CH_ID                   VARCHAR(64),
           REMARK                  VARCHAR(64), 
           PROD_ID                 VARCHAR(64),
           PPID                    VARCHAR(64),
           LAYER                   VARCHAR(64),
           STAGE                   VARCHAR(64),
           UPDATE_TIME             VARCHAR(64),
           PARTCODE                VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create {table_type} table APS_TMP_ETL_STATIC_CONS_V2
      (
          PARENTID                VARCHAR(64),
          RULE_NAME               VARCHAR(64),
          ACTION                  VARCHAR(64),
          RULE_SEQ                VARCHAR(64),
          TOOLG_ID                VARCHAR(64),
          TOOL_ID                 VARCHAR(64),
          CH_ID                   VARCHAR(64),
          REMARK                  VARCHAR(64), 
          PROD_ID                 VARCHAR(64),
          PPID                    VARCHAR(64),
          LAYER                   VARCHAR(64),
          STAGE                   VARCHAR(64),
          UPDATE_TIME             VARCHAR(64),
          PARTCODE                VARCHAR(64)
      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           CREATE {table_type} TABLE APS_TMP_ETL_SEASON_RULE
            (
                control_type           VARCHAR(64),
                control_item           VARCHAR(64),
                toolg_id               VARCHAR(60),
                tool_id                VARCHAR(60),
                ch_id                  VARCHAR(60),
                seq                    INTEGER,
                process_time           VARCHAR(60),
                lot_type               VARCHAR(255),
                recipe_last            VARCHAR(255),
                recipe_last_mode       VARCHAR(60),
                recipe_next            VARCHAR(255),
                recipe_next_mode       VARCHAR(60),
                idletime_from          VARCHAR(60),
                idletime_to            VARCHAR(60),
                run_limit              VARCHAR(60),
                run_qty                VARCHAR(60),
                update_time            VARCHAR(60),
                partcode               VARCHAR(60),
                rule_name              VARCHAR(64),
                ppid_last              VARCHAR(256),
                ppid_next              VARCHAR(256),
                ppid_last_mode         VARCHAR(64),
                ppid_next_mode         VARCHAR(64),
                recipe_setup_last      VARCHAR(256),
                recipe_setup_next      VARCHAR(256),
                recipe_setup_last_mode VARCHAR(64),
                recipe_setup_next_mode VARCHAR(64),
                prod_id_last           VARCHAR(256),
                prod_id_last_mode      VARCHAR(64),
                prod_id_next           VARCHAR(256),
                prod_id_next_mode      VARCHAR(64),
                lot_id_last            VARCHAR(256),
                lot_id_last_mode       VARCHAR(64),
                lot_id_next            VARCHAR(256),
                lot_id_next_mode       VARCHAR(64),
                prodg_id_last          VARCHAR(256),
                prodg_id_last_mode     VARCHAR(64),
                prodg_id_next          VARCHAR(256),
                prodg_id_next_mode     VARCHAR(64),
                recipe_last_next_mode  VARCHAR(64),
                layer_last             VARCHAR(256),
                layer_next             VARCHAR(256),
                stage_last             VARCHAR(256),
                stage_next             VARCHAR(256),
                consec_unit            VARCHAR(5),
                consec_cnt             VARCHAR(60),
                consec_cnt_mode        VARCHAR(64),
                stage_next_mode        VARCHAR(64),
                layer_next_mode        VARCHAR(64),
                layer_last_mode        VARCHAR(64),
                stage_last_mode        VARCHAR(64),
                reserved_lp            VARCHAR(64),
                org_setup_flag         VARCHAR(64)
            )
           """.format(table_type=table_type)
    duck_db.sql(sql)


def GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL(         
                TOOL_ID, TOOLG_ID, MODULE, HOST_EQP_FLAG,EQP_CHAMBER_FLAG                                                         
           )                                                                     
            SELECT EQP_ID AS TOOL_ID,MAX(EQP_G) AS TOOLG_ID,                      
                   MAX(REAL_MODULE) AS MODULE, MAX(HOST_EQP_FLAG) AS HOST_EQP_FLAG,
                   MAX(EQP_CHAMBER_FLAG) AS EQP_CHAMBER_FLAG 
            FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL                                            
            GROUP BY EQP_ID                                                                                                                                                                                        
     """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_STATIC_CONS_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_STATIC_CONS_TOOL")


# 说明：因为inhibit的规则 如果INHIBIT_CHIP_GROUP_FLAG=‘Y’和T.INHIBIT_RECIPE_FLAG=‘Y’或T.INHIBIT_RECIPE_OPE_FLAG=‘Y’，
# 需要单独保存记录
def GetInhibitPirunDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_STATIC_CONS_PIRUN_DATA(         
                TASK_NO,EQP_ID,CHAMBER,CONTROL_PROCESS,STATUS,LOT_MOTHER,LOT_SON,
                INHIBIT_CHAMBER_FLAG, INHIBIT_CHIP_GROUP_FLAG,INHIBIT_RECIPE_FLAG,
                INHIBIT_RECIPE_OPE_FLAG,RULE_NO                                                        
       )                                                                     
        WITH PIRUN_TASK_DATA AS (
            SELECT T.TASK_NO,T.EQP_ID,T.CHAMBER,T.CONTROL_PROCESS,T.STATUS,T.LOT_MOTHER,T.LOT_SON,
                   T.INHIBIT_CHAMBER_FLAG, T.INHIBIT_CHIP_GROUP_FLAG,T.INHIBIT_RECIPE_FLAG,T.INHIBIT_RECIPE_OPE_FLAG,
                   CASE WHEN T.INHIBIT_CHIP_GROUP_FLAG ='Y' AND (T.INHIBIT_RECIPE_FLAG='Y' OR T.INHIBIT_RECIPE_OPE_FLAG='Y') THEN 2
                        ELSE 1 END AS RNT,
                   CASE WHEN INHIBIT_CHAMBER_FLAG='N' AND INHIBIT_EQP_FLAG='N' THEN 'N' ELSE 'Y' END AS INHIBIT_FLAG         
            FROM APS_SYNC_RTD_PIRUN_TASK.APS_SYNC_RTD_PIRUN_TASK T
        )
        SELECT T.TASK_NO,T.EQP_ID,T.CHAMBER,T.CONTROL_PROCESS,T.STATUS,T.LOT_MOTHER,T.LOT_SON,
               T.INHIBIT_CHAMBER_FLAG, T.INHIBIT_CHIP_GROUP_FLAG,T.INHIBIT_RECIPE_FLAG,T.INHIBIT_RECIPE_OPE_FLAG,
               CASE WHEN RNT= '2' THEN '21' ELSE '11' END AS RULE_NO
        FROM PIRUN_TASK_DATA T
        WHERE INHIBIT_FLAG ='Y'
        UNION ALL
        SELECT T.TASK_NO,T.EQP_ID,T.CHAMBER,T.CONTROL_PROCESS,T.STATUS,T.LOT_MOTHER,T.LOT_SON,
               T.INHIBIT_CHAMBER_FLAG, T.INHIBIT_CHIP_GROUP_FLAG,T.INHIBIT_RECIPE_FLAG,T.INHIBIT_RECIPE_OPE_FLAG,
               '22' AS RULE_NO
        FROM PIRUN_TASK_DATA T
        WHERE RNT = 2    
        AND INHIBIT_FLAG ='Y'                                                                                                                                                                                   
     """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_STATIC_CONS_PIRUN_DATA",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_STATIC_CONS_PIRUN_DATA")


# 直接塞入一条ACTIVE数据
def GetATPPirunCase1ActiveData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(         
            PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOL_ID,
            RLS_ATTR1, UPDATE_TIME, PARTCODE                                                        
        )                                                                     
        select distinct '{uuid}' AS PARENTID,
               'ATP_Pirun_Inhibit' AS RULE_NAME,
               'ACTIVE' AS ACTION,
               '1' AS RULE_SEQ,
               '%' AS EQP_ID,
               '_%' AS RLS_ATTR1,
               '{current_time}' AS UPDATE_TIME,
               '' AS PARTCODE
        from APS_SYNC_RTD_PIRUN_TASK.APS_SYNC_RTD_PIRUN_TASK t                                                                                                                                                                                       
     """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into GetATPPirunCase1ActiveData",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_STATIC_CONS")


def GetStaticConsData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(         
            PARENTID,RULE_NAME,ACTION,RULE_SEQ,LOT_ID,TOOL_ID,LAYER,STAGE,WIP_ATTR3,UPDATE_TIME                                                
        )                                                                     
        select '{uuid}' AS PARENTID,
               'ATP_Pirun_Ahead_Inhibit' AS RULE_NAME,
               'INHIBIT' AS ACTION,
               '1' AS RULE_SEQ,
               REPLACE(LOT_ID,'.','')AS LOT_ID,
               '%' AS TOOL_ID,
               SPLIT_PART(CONTROL_PROCESS, '.', 1) AS LAYER,
               SPLIT_PART(CONTROL_PROCESS, '.', 2) AS STAGE,    
               '0' AS WIP_ATTR3,
               '{current_time}' AS UPDATE_TIME
        from APS_SYNC_RTD_PIRUN_AHEAD_INHIBIT_INFO.APS_SYNC_RTD_PIRUN_AHEAD_INHIBIT_INFO t                                                                                                                                                                                       
     """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_STATIC_CONS",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_STATIC_CONS")


def GetATPPirunCase1InhibitData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_STATIC_CONS_V1(         
        PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
        REMARK, PROD_ID, PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE                                                       
    )                                                                     
    select DISTINCT '{uuid}' AS PARENTID,
           'ATP_Pirun_Inhibit' AS RULE_NAME,
           'INHIBIT' AS ACTION,
           '2' AS RULE_SEQ,
           C.TOOLG_ID,
           T.EQP_ID,
           CASE WHEN T.Inhibit_chamber_flag = 'Y' THEN 
                (CASE WHEN EXISTS (SELECT 1 FROM {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL P WHERE P.EQP_CHAMBER_FLAG='Chamber' AND P.TOOL_ID = T.EQP_ID||'.'||T.CHAMBER) 
                      THEN T.EQP_ID||'.'||T.CHAMBER ELSE T.EQP_ID END) END AS CH_ID,
           T.TASK_NO AS REMARK,
           CASE WHEN T.Inhibit_chip_group_flag = 'Y' AND T.RULE_NO <>'22' and I.task_no is not null then i.P_DEVICE_NM ||'.'|| i.P_DEVICE_NM1 end as prod_id,
           CASE WHEN T.RULE_NO <>'21' AND (T.Inhibit_recipe_flag= 'Y' OR Inhibit_recipe_ope_flag = 'Y') and I.task_no is not null 
                THEN SUBSTRING( (SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))),   
                         LENGTH(( SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))) ) 
                         -POSITION('.' IN reverse(( SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))) ) ) +2) END AS PPID,
           CASE WHEN T.RULE_NO <>'21' AND Inhibit_recipe_ope_flag = 'Y' and I.task_no is not null THEN  SUBSTRING(I.CONTROL_PROCESS, 1, POSITION('.' IN I.CONTROL_PROCESS)-1) END AS LAYER,
           CASE WHEN T.RULE_NO <>'21' AND Inhibit_recipe_ope_flag = 'Y' and I.task_no is not null THEN SUBSTRING(I.CONTROL_PROCESS, POSITION('.' IN I.CONTROL_PROCESS)+1) END AS STAGE,
           '{current_time}' AS UPDATE_TIME,
           '' AS PARTCODE
    from {tempdb}APS_TMP_ETL_STATIC_CONS_PIRUN_DATA t 
    INNER JOIN {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL C
    ON C.TOOL_ID = T.EQP_ID 
    AND C.HOST_EQP_FLAG = 'Y'
    INNER JOIN APS_SYNC_RTD_PIRUN_TASK_INHIBIT_INFO.APS_SYNC_RTD_PIRUN_TASK_INHIBIT_INFO I
    ON I.TASK_NO = T.TASK_NO
    where STATUS = 'CREATE' 
    and not exists (
        select TASK_NO 
        from APS_SYNC_PIRUN_TASK_SCHEDULE_LOT.APS_SYNC_PIRUN_TASK_SCHEDULE_LOT l
        where STATUS <> 4
        and l.TASK_NO = t.TASK_NO
    )                                                                                                                                                                                        
     """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into GetATPPirunCase1InhibitData",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_STATIC_CONS_V1")


# def GetATPPirunCase2ActiveData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
#     sql = """
#         INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(
#             PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOL_ID,
#             RLS_ATTR1, UPDATE_TIME, PARTCODE
#         )
#         select '{uuid}' AS PARENTID,
#                'ATP_Pirun_Inhibit' AS RULE_NAME,
#                'ACTIVE' AS ACTION,
#                '1' AS RULE_SEQ,
#                '%' AS EQP_ID,
#                '_%' AS RLS_ATTR1,
#                '{current_time}' AS UPDATE_TIME,
#                '' AS PARTCODE
#         from APS_SYNC_RTD_PIRUN_TASK.APS_SYNC_RTD_PIRUN_TASK t
#         INNER JOIN {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL C
#         ON C.TOOL_ID = T.EQP_ID
#         AND C.HOST_EQP_FLAG = 'Y'
#         where t.LOT_MOTHER is not null
#         or t.LOT_SON is not null
#         or exists (
#             select TASK_NO
#             from APS_SYNC_PIRUN_TASK_SCHEDULE_LOT.APS_SYNC_PIRUN_TASK_SCHEDULE_LOT l
#             where STATUS <> 4
#             and l.TASK_NO = t.TASK_NO
#         )
#      """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())
#
#     my_duck.exec_sql(oracle_conn=oracle_conn,
#                      duck_db_memory=duck_db_memory,
#                      ETL_Proc_Name=ETL_Proc_Name,
#                      methodName="Insert Into GetATPPirunCase2ActiveData",
#                      sql=sql,
#                      current_time=current_time,
#                      update_table="APS_ETL_STATIC_CONS")

def GetATPPirunCase2InhibitData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_STATIC_CONS_V2(         
        PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
        REMARK, PROD_ID, PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE                                                       
    )                                                                     
    select DISTINCT '{uuid}' AS PARENTID,
           'ATP_Pirun_Inhibit' AS RULE_NAME,
           'INHIBIT' AS ACTION,
           '2' AS RULE_SEQ,
           C.TOOLG_ID,
           T.EQP_ID,
           CASE WHEN T.Inhibit_chamber_flag = 'Y' THEN 
                (CASE WHEN EXISTS (SELECT 1 FROM {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL P WHERE P.EQP_CHAMBER_FLAG='Chamber' AND P.TOOL_ID = T.EQP_ID||'.'||T.CHAMBER) 
                      THEN T.EQP_ID||'.'||T.CHAMBER ELSE T.EQP_ID END) END AS CH_ID,
           T.TASK_NO AS REMARK,
           CASE WHEN T.Inhibit_chip_group_flag = 'Y' AND T.RULE_NO <>'22' and I.task_no is not null then i.P_DEVICE_NM ||'.'|| i.P_DEVICE_NM1 end as prod_id,
           CASE WHEN T.RULE_NO <>'21' AND (T.Inhibit_recipe_flag= 'Y' OR Inhibit_recipe_ope_flag = 'Y') and I.task_no is not null 
                THEN SUBSTRING( (SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))),   
                         LENGTH(( SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))) ) 
                         -POSITION('.' IN reverse(( SUBSTRING(I.RECIPE_NAME, 1, LENGTH(I.RECIPE_NAME)- POSITION('.' in reverse(I.RECIPE_NAME)))) ) ) +2) END AS PPID,
           CASE WHEN T.RULE_NO <>'21' AND Inhibit_recipe_ope_flag = 'Y' and I.task_no is not null THEN  SUBSTRING(I.CONTROL_PROCESS, 1, POSITION('.' IN I.CONTROL_PROCESS)-1) END AS LAYER,
           CASE WHEN T.RULE_NO <>'21' AND Inhibit_recipe_ope_flag = 'Y' and I.task_no is not null THEN SUBSTRING(I.CONTROL_PROCESS, POSITION('.' IN I.CONTROL_PROCESS)+1) END AS STAGE,
           '{current_time}' AS UPDATE_TIME,
           '' AS PARTCODE
    from {tempdb}APS_TMP_ETL_STATIC_CONS_PIRUN_DATA t
    INNER JOIN {tempdb}APS_TMP_ETL_STATIC_CONS_TOOL C
    ON C.TOOL_ID = T.EQP_ID 
    AND C.HOST_EQP_FLAG = 'Y'
    INNER JOIN APS_SYNC_RTD_PIRUN_TASK_INHIBIT_INFO.APS_SYNC_RTD_PIRUN_TASK_INHIBIT_INFO I
    ON I.TASK_NO = T.TASK_NO
    where t.LOT_MOTHER is not null
    or t.LOT_SON is not null 
    or exists (
        select TASK_NO 
        from APS_SYNC_PIRUN_TASK_SCHEDULE_LOT.APS_SYNC_PIRUN_TASK_SCHEDULE_LOT l
        where STATUS <> 4
        and l.TASK_NO = t.TASK_NO
    )                                                                                                                                                                                         
     """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into GetATPPirunCase2InhibitData",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_STATIC_CONS_V2")


def GetATPPirunInhibitResultData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(         
        PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
        REMARK, PROD_ID, PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE                                                       
    )
    WITH SOURCE_DATA AS (
        SELECT PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
               REMARK, PROD_ID, PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE
        FROM  {tempdb}APS_TMP_ETL_STATIC_CONS_V1 
        UNION ALL
        SELECT PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
               REMARK, PROD_ID, PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE
        FROM  {tempdb}APS_TMP_ETL_STATIC_CONS_V2 
    )
    SELECT distinct PARENTID, RULE_NAME, ACTION, RULE_SEQ, TOOLG_ID, TOOL_ID, CH_ID, 
           REMARK, 
           REPLACE(PROD_ID,'*','%') AS PROD_ID, 
           PPID, LAYER, STAGE, UPDATE_TIME, PARTCODE
    FROM SOURCE_DATA D
    WHERE D.PROD_ID IS NOT NULL
          OR D.PPID IS NOT NULL
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into GetATPPirunInhibitResultData",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_STATIC_CONS")


def GetRDSInhibitSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(                    
      PARENTID, RULE_NAME, ACTION, TOOLG_ID, TOOL_ID, PPID, UPDATE_TIME, PARTCODE                                                        
    ) 
    WITH RDS_INHIBIT_DATA AS (
        SELECT 'RDS_Inhibit' AS RULE_NAME,
               'INHIBIT' AS ACTION,
               T.EQP_G AS TOOLG_ID,
               TOOLID AS TOOL_ID,
               SUBSTRING( (SUBSTRING(DKEY, 1, LENGTH(DKEY)- POSITION('.' in reverse(DKEY)))),   
                             LENGTH(( SUBSTRING(DKEY, 1, LENGTH(DKEY)- POSITION('.' in reverse(DKEY)))) ) 
                             -POSITION('.' IN reverse(( SUBSTRING(DKEY, 1, LENGTH(DKEY)- POSITION('.' in reverse(DKEY)))) ) ) +2) AS PPID, 
               V.LASTSTAMPTIME,
               V.EXPIRETIME
        FROM APS_SYNC_RDS_RECIPEINFO.APS_SYNC_RDS_RECIPEINFO V 
        INNER JOIN APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T 
        ON T.EQP_ID = V.TOOLID AND T.HOST_EQP_FLAG='Y'
        WHERE V.MODULE ='ETCH' 
        --AND CAST(V.EXPIRETIME AS TIMESTAMP) < CURRENT_TIMESTAMP
    ),
    RESULT_DATA AS (
        SELECT RULE_NAME, ACTION, TOOLG_ID, TOOL_ID, PPID, EXPIRETIME,
               ROW_NUMBER() OVER(PARTITION BY TOOL_ID,PPID ORDER BY LASTSTAMPTIME DESC) AS RNT 
        FROM RDS_INHIBIT_DATA
    )
    SELECT DISTINCT '{uuid}' AS PARENTID,   
           RULE_NAME, ACTION, TOOLG_ID, TOOL_ID, PPID,
           '{current_time}' AS UPDATE_TIME,                              
            '' AS PARTCODE 
    FROM RESULT_DATA
    WHERE RNT = 1 AND CAST(EXPIRETIME AS TIMESTAMP) < CURRENT_TIMESTAMP 
    AND PPID IS NOT NULL 
    AND PPID <>''                                                                        
    """.format(uuid=uuid, current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_STATIC_CONS",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_STATIC_CONS")


def InsertSeasonRuleDataView2Temp_1(duck_db_memory=None, uuid="", current_time="", oracle_conn=None, ETL_Proc_Name="",
                                    used_table_dict=None):
    '''特殊批次控制規則數據插入到temp表'''
    sql = """
    INSERT INTO {tempdb}APS_TMP_ETL_SEASON_RULE(                                 
         CONTROL_TYPE, CONTROL_ITEM, TOOLG_ID, TOOL_ID, CH_ID, SEQ, RULE_NAME, PROCESS_TIME, PROD_ID_LAST, PROD_ID_LAST_MODE, PROD_ID_NEXT,
         PROD_ID_NEXT_MODE, LOT_ID_LAST, LOT_ID_LAST_MODE, LOT_ID_NEXT, LOT_ID_NEXT_MODE,PRODG_ID_LAST,PRODG_ID_LAST_MODE,
         PRODG_ID_NEXT,PRODG_ID_NEXT_MODE,RECIPE_LAST,RECIPE_LAST_MODE,RECIPE_NEXT,RECIPE_NEXT_MODE,LAYER_LAST, LAYER_LAST_MODE,LAYER_NEXT,LAYER_NEXT_MODE,
         STAGE_LAST,STAGE_LAST_MODE,STAGE_NEXT,STAGE_NEXT_MODE,CONSEC_UNIT,CONSEC_CNT,CONSEC_CNT_MODE, UPDATE_TIME, PARTCODE                                                                              
    )                  
    WITH APS_TMP_ETL_SEASON_RULE_TOOL AS(
            SELECT EQP_ID,                                                                 
                   MAX(EQP_G) AS EQP_G                                                          
             FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL L                                    
             WHERE L.HOST_EQP_FLAG = 'Y'
             GROUP BY EQP_ID
    )                                                                          
    SELECT DISTINCT N.CONTROL_TYPE,                                                           -- 新增：直接帶入 N.CONTROL_TYPE
           CASE WHEN POSITION('!' IN N.CONTROL_ITEM) = 1 THEN 'NOT_IN' ELSE 'IN' END AS CONTROL_ITEM,  -- 新增：判斷 IN/NOT_IN
           CASE 
             WHEN TD.EQP_G IS NOT NULL THEN TD.EQP_G  -- 來自工具表的具體設備群組
             ELSE '%'                                  -- 通配符設備使用 '%'
           END AS TOOLG_ID,                                                                 
           REPLACE(N.EQP_ID,'*','%') AS TOOL_ID,                                                                                                   
           CASE WHEN EXISTS (SELECT 1 FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T WHERE T.EQP_ID = N.EQP_ID||'.'||N.CHAMBER_ID AND EQP_CHAMBER_FLAG='Chamber' ) THEN N.EQP_ID||'.'||N.CHAMBER_ID END AS CH_ID,                                                                                                                            
           CAST('1' AS INTEGER) AS SEQ,                                                                                                                             
           'SPECIAL_LOT_CTRL' AS RULE_NAME,                                                                                                        
           CASE WHEN N.NEED_TIME = '*' OR N.NEED_TIME IS NULL THEN CAST('0' AS DECIMAL) ELSE CAST(COALESCE(N.NEED_TIME,0) AS DECIMAL) END AS PROCESS_TIME,                                                             
           CASE WHEN N.CONTROL_TYPE='1' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',')  END AS PROD_ID_LAST,                                  
           CASE WHEN N.CONTROL_TYPE='1' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END) END AS PROD_ID_LAST_MODE,         
           CASE WHEN N.CONTROL_TYPE='1' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',') END AS PROD_ID_NEXT,                                       
           CASE WHEN N.CONTROL_TYPE='1' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END)  END AS PROD_ID_NEXT_MODE,             
           CASE WHEN N.CONTROL_TYPE='2' THEN REPLACE(REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',','), '.','') END AS LOT_ID_LAST,                                   
           CASE WHEN N.CONTROL_TYPE='2' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END) END AS LOT_ID_LAST_MODE,                                                                         
           CASE WHEN N.CONTROL_TYPE='2' THEN REPLACE(REPLACE(REPLACE(N.TO_ITEM,'*','%'),'.',''),'/',',') END AS LOT_ID_NEXT,                                        
           CASE WHEN N.CONTROL_TYPE='2' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM) =1 THEN 'NOT IN' ELSE 'in' END) END AS LOT_ID_NEXT_MODE,                                                                         
           CASE WHEN N.CONTROL_TYPE='3' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',') END AS PRODG_ID_LAST,                
           CASE WHEN N.CONTROL_TYPE='3' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS PRODG_ID_LAST_MODE,        
           CASE WHEN N.CONTROL_TYPE='3' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',')  END AS PRODG_ID_NEXT,                    
           CASE WHEN N.CONTROL_TYPE='3' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS PRODG_ID_NEXT_MODE,             
           CASE WHEN N.CONTROL_TYPE='4' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',')  END AS RECIPE_LAST,                                   
           CASE WHEN N.CONTROL_TYPE='4' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS RECIPE_LAST_MODE,          
           CASE WHEN N.CONTROL_TYPE='4' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',') END AS RECIPE_NEXT,                                        
           CASE WHEN N.CONTROL_TYPE='4' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS RECIPE_NEXT_MODE,               
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE, 1, POSITION('.' IN N.CONTROL_OPE)-1) END AS LAYER_LAST,                   
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS LAYER_LAST_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE, 1,  POSITION('.' IN N.CONTROL_OPE)-1) END AS LAYER_NEXT,                   
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS LAYER_NEXT_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE,  POSITION('.' IN N.CONTROL_OPE)+1) END AS STAGE_LAST,                      
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS STAGE_LAST_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE,  POSITION('.' IN N.CONTROL_OPE)+1) END AS STAGE_NEXT,                      
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS STAGE_NEXT_MODE,                                                                 
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE 'WAFER' END AS CONSEC_UNIT,                                                               
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE N.MIN_MOVE_PCS END AS CONSEC_CNT,                                                         
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE 'LARGER' END AS CONSEC_CNT_MODE,                                                          
           '{current_time}' AS UPDATE_TIME,                                                                                                  
           '' AS PARTCODE                                                                                       
    FROM APS_SYNC_RTD_WET_SPECIAL_LOT_CONTROL.APS_SYNC_RTD_WET_SPECIAL_LOT_CONTROL N                                                                           
    LEFT JOIN APS_TMP_ETL_SEASON_RULE_TOOL TD                                                                                                           
    ON TD.EQP_ID = N.EQP_ID                                                                                                                        
    WHERE 1=1                            
         AND N.CONTROL_ITEM = '*'  -- 只處理 CONTROL_ITEM = '*' 的記錄
         AND N.NEED_TIME = 999         
         AND 
         (case when N.END_TIME IS NULL or N.END_TIME = '' then '1901-01-01 00:00' else  N.END_TIME end  )  >= (case when N.END_TIME IS NULL then '1901-01-01 00:00' else  STRFTIME(CURRENT_TIMESTAMP, '%Y-%m-%d %H:%M') end  )                                                                
         AND    (case when N.START_TIME IS NULL or N.START_TIME='' then '1901-01-01 00:00' else  N.START_TIME end  )  <= (case when N.START_TIME IS NULL then '1901-01-01 00:00' else  STRFTIME(CURRENT_TIMESTAMP, '%Y-%m-%d %H:%M') end  )                                                                                                                      
   """.format(tempdb=my_duck.get_temp_table_mark(), current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_SEASON_RULE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_SEASON_RULE")


def InsertSeasonRuleDataView2Temp_2(duck_db_memory=None, uuid="", current_time="", oracle_conn=None, ETL_Proc_Name="",
                                    used_table_dict=None):
    '''特殊批次控制規則數據插入到temp表'''
    sql = """
    INSERT INTO {tempdb}APS_TMP_ETL_SEASON_RULE(                                 
         CONTROL_TYPE, CONTROL_ITEM, TOOLG_ID, TOOL_ID, CH_ID, SEQ, RULE_NAME, PROCESS_TIME, PROD_ID_LAST, PROD_ID_LAST_MODE, PROD_ID_NEXT,
         PROD_ID_NEXT_MODE, LOT_ID_LAST, LOT_ID_LAST_MODE, LOT_ID_NEXT, LOT_ID_NEXT_MODE,PRODG_ID_LAST,PRODG_ID_LAST_MODE,
         PRODG_ID_NEXT,PRODG_ID_NEXT_MODE,RECIPE_LAST,RECIPE_LAST_MODE,RECIPE_NEXT,RECIPE_NEXT_MODE,LAYER_LAST, LAYER_LAST_MODE,LAYER_NEXT,LAYER_NEXT_MODE,
         STAGE_LAST,STAGE_LAST_MODE,STAGE_NEXT,STAGE_NEXT_MODE,CONSEC_UNIT,CONSEC_CNT,CONSEC_CNT_MODE, UPDATE_TIME, PARTCODE                                                                              
    )                                                                              
    SELECT DISTINCT N.CONTROL_TYPE,                                                           -- 新增：直接帶入 N.CONTROL_TYPE
           CASE WHEN POSITION('!' IN N.CONTROL_ITEM) = 1 THEN 'NOT_IN' ELSE 'IN' END AS CONTROL_ITEM,  -- 新增：判斷 IN/NOT_IN
           '%' AS TOOLG_ID,                                                       
           REPLACE(N.EQP_ID,'*','%') AS TOOL_ID,                                                                                                   
           CASE WHEN EXISTS (SELECT 1 FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T WHERE T.EQP_ID = N.EQP_ID||'.'||N.CHAMBER_ID AND EQP_CHAMBER_FLAG='Chamber' ) THEN N.EQP_ID||'.'||N.CHAMBER_ID END AS CH_ID,                                                                                                                            
           CAST('1' AS INTEGER) AS SEQ,                                                                                                                             
           'SPECIAL_LOT_CTRL' AS RULE_NAME,                                                                                                        
           CASE WHEN N.NEED_TIME = '*' OR N.NEED_TIME IS NULL THEN CAST('0' AS DECIMAL) ELSE CAST(COALESCE(N.NEED_TIME,0) AS DECIMAL) END AS PROCESS_TIME,                                                             
           CASE WHEN N.CONTROL_TYPE='1' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',')  END AS PROD_ID_LAST,                                  
           CASE WHEN N.CONTROL_TYPE='1' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END) END AS PROD_ID_LAST_MODE,         
           CASE WHEN N.CONTROL_TYPE='1' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',') END AS PROD_ID_NEXT,                                       
           CASE WHEN N.CONTROL_TYPE='1' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END)  END AS PROD_ID_NEXT_MODE,             
           CASE WHEN N.CONTROL_TYPE='2' THEN REPLACE(REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',','), '.','') END AS LOT_ID_LAST,                                   
           CASE WHEN N.CONTROL_TYPE='2' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM) =1 THEN 'NOT IN' ELSE 'IN' END) END AS LOT_ID_LAST_MODE,                                                                         
           CASE WHEN N.CONTROL_TYPE='2' THEN REPLACE(REPLACE(REPLACE(N.TO_ITEM,'*','%'),'.',''),'/',',') END AS LOT_ID_NEXT,                                        
           CASE WHEN N.CONTROL_TYPE='2' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM) =1 THEN 'NOT IN' ELSE 'in' END) END AS LOT_ID_NEXT_MODE,                                                                         
           CASE WHEN N.CONTROL_TYPE='3' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',') END AS PRODG_ID_LAST,                
           CASE WHEN N.CONTROL_TYPE='3' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS PRODG_ID_LAST_MODE,        
           CASE WHEN N.CONTROL_TYPE='3' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',')  END AS PRODG_ID_NEXT,                    
           CASE WHEN N.CONTROL_TYPE='3' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS PRODG_ID_NEXT_MODE,             
           CASE WHEN N.CONTROL_TYPE='4' THEN REPLACE(REPLACE(REPLACE(N.CONTROL_ITEM,'*','%'),'!',''),'/',',')  END AS RECIPE_LAST,                                   
           CASE WHEN N.CONTROL_TYPE='4' THEN (CASE WHEN POSITION('!' IN N.CONTROL_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS RECIPE_LAST_MODE,          
           CASE WHEN N.CONTROL_TYPE='4' THEN REPLACE(REPLACE(N.TO_ITEM,'*','%'),'/',',') END AS RECIPE_NEXT,                                        
           CASE WHEN N.CONTROL_TYPE='4' THEN (CASE WHEN POSITION('!' IN N.TO_ITEM)=1 THEN 'NOT IN' ELSE 'in' END) END AS RECIPE_NEXT_MODE,               
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE, 1, POSITION('.' IN N.CONTROL_OPE)-1) END AS LAYER_LAST,                   
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS LAYER_LAST_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE, 1,  POSITION('.' IN N.CONTROL_OPE)-1) END AS LAYER_NEXT,                   
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS LAYER_NEXT_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE,  POSITION('.' IN N.CONTROL_OPE)+1) END AS STAGE_LAST,                      
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS STAGE_LAST_MODE,                                                                 
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE SUBSTRING(N.CONTROL_OPE,  POSITION('.' IN N.CONTROL_OPE)+1) END AS STAGE_NEXT,                      
           CASE WHEN N.CONTROL_OPE = '*' THEN '' ELSE 'in' END AS STAGE_NEXT_MODE,                                                                 
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE 'WAFER' END AS CONSEC_UNIT,                                                               
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE N.MIN_MOVE_PCS END AS CONSEC_CNT,                                                         
           CASE WHEN N.MIN_MOVE_PCS = '*' OR N.MIN_MOVE_PCS IS NULL THEN NULL ELSE 'LARGER' END AS CONSEC_CNT_MODE,                                                          
           '{current_time}' AS UPDATE_TIME,                                                                                                  
           '' AS PARTCODE                                                                                       
    FROM APS_SYNC_RTD_WET_SPECIAL_LOT_CONTROL.APS_SYNC_RTD_WET_SPECIAL_LOT_CONTROL N                                                                                                                                                                        
    WHERE 1=1    
         AND N.EQP_ID IS NOT NULL AND N.EQP_ID <> '' AND POSITION('*' IN N.EQP_ID)>0   
         AND N.CONTROL_ITEM = '*'  -- 只處理 CONTROL_ITEM = '*' 的記錄
         AND N.NEED_TIME = 999            
         AND (case when N.END_TIME IS NULL then '1901-01-01 00:00' else  N.END_TIME end  )  >= (case when N.END_TIME IS NULL then '1901-01-01 00:00' else  STRFTIME(CURRENT_TIMESTAMP, '%Y-%m-%d %H:%M') end  )                                                          
         AND (case when N.START_TIME IS NULL then '1901-01-01 00:00' else  N.START_TIME end  )  <= (case when N.START_TIME IS NULL then '1901-01-01 00:00' else  STRFTIME(CURRENT_TIMESTAMP, '%Y-%m-%d %H:%M') end  )                                                                                                                      
          """.format(tempdb=my_duck.get_temp_table_mark(), current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_SEASON_RULE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_SEASON_RULE")


def GetSecialLotData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO APS_ETL_STATIC_CONS(         
            PARENTID,RULE_NAME,ACTION,RULE_SEQ,LOT_ID,TOOLG_ID,TOOL_ID,CH_ID,PROD_ID,PRODG_ID,RECIPE,UPDATE_TIME                                                
        )WITH APS_ETL_SPECIAL_LOT AS(
            SELECT 
                'INHIBIT' AS action,
                rule_name,
                toolg_id,
                tool_id,
                ch_id,
                lot_id_next AS lot_id,
                NULL AS prod_id,
                NULL AS prodg_id,
                NULL AS recipe,
                update_time
            FROM APS_TMP_ETL_SEASON_RULE
            WHERE control_type = 2
            UNION ALL
            SELECT 
                'INHIBIT' AS action,
                rule_name,
                toolg_id,
                tool_id,
                ch_id,
                NULL AS lot_id,
                prod_id_next AS prod_id,
                NULL AS prodg_id,
                NULL AS recipe,
                update_time
            FROM APS_TMP_ETL_SEASON_RULE
            WHERE control_type = 1
            UNION ALL
            SELECT 
                'INHIBIT' AS action,
                rule_name,
                toolg_id,
                tool_id,
                ch_id,
                NULL AS lot_id,
                NULL AS prod_id,
                prodg_id_next AS prodg_id,
                NULL AS recipe,
                update_time
            FROM APS_TMP_ETL_SEASON_RULE
            WHERE control_type = 3
            UNION ALL
            SELECT 
                'INHIBIT' AS action,
                rule_name,
                toolg_id,
                tool_id,
                ch_id,
                NULL AS lot_id,
                NULL AS prod_id,
                NULL AS prodg_id,
                recipe_next AS recipe,
                update_time
            FROM APS_TMP_ETL_SEASON_RULE
            WHERE control_type = 4
        )                                                                    
        select distinct '{uuid}' AS PARENTID,
           rule_name AS RULE_NAME,
           action AS ACTION,
           '1' AS RULE_SEQ,
           lot_id AS LOT_ID,
           toolg_id AS TOOLG_ID,
           tool_id AS TOOL_ID,
           ch_id as CH_ID,
           prod_id as PROD_ID,
           prodg_id as PRODG_ID,
           recipe as RECIPE,
           '{current_time}' AS UPDATE_TIME
        from APS_ETL_SPECIAL_LOT t                                                                                                                                                                                       
     """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_STATIC_CONS",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_STATIC_CONS")


def execute():
    ###############################################################
    ### 以下参数必须定义
    ### ETL_Proc_Name    : ETL 名称
    ### current_time     ：请直接拷贝
    ### current_time_short ：请直接拷贝
    ### uuid             ：请直接拷贝
    ### target_table     : 该ETL输出表名
    ### used_table_list  : 该ETL使用到的，参考到的表名(中间表不算)
    ### target_table_sql ： 该ETL输出表定义SQL
    ###############################################################
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_STATIC_CONS_10M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_STATIC_CONS"
    used_table_list = ['APS_SYNC_RDS_RECIPEINFO',
                       'APS_SYNC_ETL_TOOL',
                       'APS_SYNC_RTD_PIRUN_TASK',
                       'APS_SYNC_RTD_PIRUN_TASK_INHIBIT_INFO',
                       'APS_SYNC_PIRUN_TASK_SCHEDULE_LOT',
                       'APS_SYNC_RTD_PIRUN_AHEAD_INHIBIT_INFO',
                       'APS_SYNC_RTD_WET_SPECIAL_LOT_CONTROL']
    target_table_sql = """
        create table {}APS_ETL_STATIC_CONS
        (
          parentid    VARCHAR(64) not null,
          rule_name   VARCHAR(64), 
          action      VARCHAR(64), 
          toolg_id    VARCHAR(64), 
          tool_id     VARCHAR(64),
          ch_id       VARCHAR(64),
          ppid        VARCHAR(64),
          rule_seq    VARCHAR(64),
          rls_attr1   VARCHAR(64),
          layer       VARCHAR(64),
          stage       VARCHAR(64),
          remark      VARCHAR(64),
          prod_id     VARCHAR(64),
          prodg_id    VARCHAR(64),
          update_time VARCHAR(64),
          partcode    VARCHAR(64),
          LOT_ID      VARCHAR(64),
          WIP_ATTR3   VARCHAR(64),
          recipe      VARCHAR(64)
        )
    """.format("")  # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
    target_db_file = my_duck.get_target_file_name(target_table, current_time_short)

    # -------------------------- 内存模式改成文件模式
    _temp_db_path = os.path.join(config.g_mem_speed_etl_output_path, target_table, "inprocess")
    if not os.path.exists(_temp_db_path):
        os.makedirs(_temp_db_path)

    temp_db_file = os.path.join(_temp_db_path, target_table + "_" + current_time_short + "_temp.db")
    # 处理中文件
    in_process_db_file = os.path.join(_temp_db_path, target_table + "_" + current_time_short + ".db")
    # 结果文件
    target_db_file = os.path.join(config.g_mem_etl_output_path, target_table,
                                  target_table + "_" + current_time_short + ".db")
    # --------------------------

    oracle_conn = None
    try:
        oracle_conn = my_oracle.oracle_get_connection()
        # 开始日志
        my_oracle.StartCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)

        # -------------------------- 内存模式改成文件模式
        # 创建DuckDB
        duck_db_memory = my_duck.create_duckdb_in_file(_temp_db_path, in_process_db_file, target_table_sql)
        duck_db_memory.sql('SET threads TO 4')

        if not os.path.exists(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)):
            os.makedirs(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid))
        duck_db_memory.execute(
            "SET temp_directory='{}'".format(os.path.join(config.g_mem_speed_etl_output_path, 'duck_temp', uuid)))

        if not config.g_debug_mode:
            create_temp_table(duck_db_memory)
        else:
            duck_db_temp = my_duck.create_duckdb_for_temp_table(_temp_db_path, temp_db_file)
            # 创建Temp表
            create_temp_table(duck_db_temp)
            duck_db_temp.commit()
            duck_db_temp.close()
            my_duck.attach_temp_db_write_able(duck_db_memory, "TEMPDB", temp_db_file)
        # --------------------------

        # Attach用到的表
        my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
        ################################################################################################################
        ## 以下为业务逻辑
        ################################################################################################################
        # 获取tool资讯
        GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        GetInhibitPirunDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 获取WaferStart信息
        GetRDSInhibitSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # ATP_PIRUN规则
        # 情境一：ATP_Pirun已開單，但是尚未挑選Pilot。ACTIVE規則
        GetATPPirunCase1ActiveData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 情境一：ATP_Pirun已開單，但是尚未挑選Pilot。INHIBIT規則
        GetATPPirunCase1InhibitData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # # 情境二：ATP_Pirun尚未結束，且已挑選Pilot。ACTIVE規則
        # GetATPPirunCase2ActiveData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 情境二：ATP_Pirun尚未結束，且已挑選Pilot。INHIBIT規則
        GetATPPirunCase2InhibitData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 需要把inhibit的数据进行梳理，如果没有info对应的数据，则不需要提供
        GetATPPirunInhibitResultData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        GetStaticConsData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        InsertSeasonRuleDataView2Temp_1(duck_db_memory=duck_db_memory, uuid=uuid, current_time=current_time,
                                        oracle_conn=oracle_conn,
                                        ETL_Proc_Name=ETL_Proc_Name, used_table_dict=used_table_list)
        InsertSeasonRuleDataView2Temp_2(duck_db_memory=duck_db_memory, uuid=uuid, current_time=current_time,
                                        oracle_conn=oracle_conn,
                                        ETL_Proc_Name=ETL_Proc_Name, used_table_dict=used_table_list)
        GetSecialLotData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select parentid,
                                           CASE WHEN rule_name='' THEN NULL ELSE rule_name END AS rule_name,
                                           CASE WHEN action='' THEN NULL ELSE action END AS action,
                                           CASE WHEN toolg_id='' THEN NULL ELSE toolg_id END AS toolg_id,
                                           CASE WHEN tool_id='' THEN NULL ELSE tool_id END AS tool_id,
                                           case when ch_id = '' then null else ch_id end as ch_id,
                                           CASE WHEN ppid='' THEN NULL ELSE ppid END AS ppid,
                                           case when rule_seq='' then null else rule_seq end as rule_seq,
                                           case when rls_attr1='' then null else rls_attr1 end as rls_attr1,
                                           case when prod_id='' then null else prod_id end as prod_id,
                                           case when layer='' then null else layer end as layer,
                                           case when stage='' then null else stage end as stage,
                                           case when remark='' then null else remark end as remark,
                                           CASE WHEN update_time='' THEN NULL ELSE update_time END AS update_time,
                                           WIP_ATTR3,
                                           CASE WHEN lot_id='' THEN NULL ELSE lot_id END AS lot_id,
                                           CASE WHEN prodg_id='' THEN NULL ELSE prodg_id END AS prodg_id,
                                           CASE WHEN recipe='' THEN NULL ELSE recipe END AS recipe
                                     from APS_ETL_STATIC_CONS
                                 """
            postgres_table_define = """etl_static_cons(parentid,rule_name, action, toolg_id, tool_id, ch_id, ppid, rule_seq, 
                                                      rls_attr1, prod_id, layer, stage, remark, update_time, WIP_ATTR3,lot_id,prodg_id,recipe)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_static_cons",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                oracle_conn=oracle_conn,
                                                ETL_Proc_Name=ETL_Proc_Name)
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                   in_process_db_file=in_process_db_file,
                                                                                   target_table=target_table,
                                                                                   current_time=current_time_short)
        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 加入PG执行结束的时间更新
        my_oracle.Update_PG_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
        # 写完成日志
        my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time)
    except Exception as e:
        logging.exception("{ETL_Proc_Name} 處理出錯 : {e}".format(ETL_Proc_Name=ETL_Proc_Name, e=e))
        # 导出到目标文件中
        my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                  in_process_db_file=in_process_db_file,
                                                                  target_table=target_table,
                                                                  current_time=current_time_short)
        # 写警告日志
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_ETL_STATIC_CONS_CODE_XX_ETL)
        raise e
    finally:
        oracle_conn.commit()
        oracle_conn.close()
        # 删除TMP目录:LQN:2023/08/21
        if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
            os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            os.remove(temp_db_file)
        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()