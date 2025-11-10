import gc
import logging
import os
import shutil

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, my_file

#  该JOB只针对PARALLEL_MODE


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''
    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_TOOL
    (
      toolg_id  VARCHAR(64),
      eqp_id    VARCHAR(64),
      module_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_HIST
    (
      eqp_id             VARCHAR(64),
      ppid               VARCHAR(64),
      qty                VARCHAR(64),
      lot_id             VARCHAR(64),
      ctrl_job           VARCHAR(64),
      process_start      TIMESTAMP,
      process_end        TIMESTAMP,
      claim_memo         VARCHAR(4000),
      TOOLG_ID           VARCHAR(64),
      PRODG1             VARCHAR(64),
      LAYER              VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_RESULT
    (
      eqp_id             VARCHAR(64),
      ppid               VARCHAR(64),
      qty                VARCHAR(64),
      process_start      TIMESTAMP,
      process_end        TIMESTAMP,
      claim_memo         VARCHAR(4000),
      TOOLG_ID           VARCHAR(64),
      PRODG1             VARCHAR(64),
      LAYER              VARCHAR(64),
      CTRL_JOB           VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_LOTHISTORY
    (
      CTRL_JOB  VARCHAR(64),
      eqp_id    VARCHAR(64),
      RECIPE VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_RECIPE
    (
        eqp_id             VARCHAR(64),
        ppid               VARCHAR(64),
        qty                VARCHAR(64),
        process_start      TIMESTAMP,
        process_end        TIMESTAMP,
        claim_memo         VARCHAR(4000),
        TOOLG_ID           VARCHAR(64),
        PRODG1             VARCHAR(64),
        LAYER              VARCHAR(64),
        CTRL_JOB           VARCHAR(64),
        RECIPE             VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_TYPE21_PARMODE_JION
    (
        eqp_id             VARCHAR(64),
        ppid               VARCHAR(64),
        qty                VARCHAR(64),
        process_start      TIMESTAMP,
        process_end        TIMESTAMP,
        claim_memo         VARCHAR(4000),
        TOOLG_ID           VARCHAR(64),
        PRODG1             VARCHAR(64),
        LAYER              VARCHAR(64),
        CTRL_JOB           VARCHAR(64),
        RECIPE             VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_TYPE21_PARMODE_DUMMY
        (
            eqp_id             VARCHAR(64),
            ppid               VARCHAR(64),
            qty                VARCHAR(64),
            process_start      TIMESTAMP,
            process_end        TIMESTAMP,
            claim_memo         VARCHAR(4000),
            TOOLG_ID           VARCHAR(64),
            PRODG1             VARCHAR(64),
            LAYER              VARCHAR(64),
            CTRL_JOB           VARCHAR(64),
            RECIPE             VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)








def getEtlToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_TOOL (                                           
       TOOLG_ID, EQP_ID, MODULE_ID                                                             
    )                                                                          
         SELECT MAX(T.EQP_G) AS TOOLG_ID, T.EQP_ID,                                                                             
         MAX(T.REAL_MODULE) AS MODULE_ID                                                                                      
         FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T                                                                     
         WHERE T.EQP_CATEGORY <> 'Measurement' 
         AND T.EQP_CHAMBER_FLAG ='Chamber' 
         AND T.HOST_EQP_FLAG ='Y'                                         
         GROUP BY T.EQP_ID                     
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_TOOL")


def getHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_HIST (                                                
        EQP_ID, PPID, QTY, LOT_ID, CTRL_JOB, PROCESS_START, PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER
    )                                                                          
         SELECT F.EQP_ID,                                                                                                   
                F.PH_RECIPE_ID AS PPID,                                                                                     
                F.CUR_WAFER_QTY AS QTY,                                                                                     
                F.LOT_ID,                                                                                                   
                F.CTRL_JOB,                                                                                                 
                CASE WHEN OPE_CATEGORY='ProcessStart' THEN F.CLAIM_TIME ELSE NULL END AS PROCESS_START,                     
                CASE WHEN OPE_CATEGORY='ProcessEnd' THEN F.CLAIM_TIME ELSE NULL END AS ProcessEnd,                     
                CASE WHEN F.CLAIM_MEMO is not null THEN replace(replace(F.CLAIM_MEMO,'(','') ,')@;','')  END AS CLAIM_MEMO,
                T.TOOLG_ID,
                P.PRODG1, 
                SUBSTRING(F.OPE_NO, 1, POSITION('.' IN F.OPE_NO)-1) AS LAYER   
         FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW F 
         INNER JOIN {tempdb}APS_TMP_TYPE21_PARMODE_TOOL T 
         ON T.EQP_ID = F.EQP_ID
         LEFT JOIN APS_SYNC_PRODUCT.APS_SYNC_PRODUCT P
         ON P.PRODSPEC_ID = F.PRODSPEC_ID
         WHERE F.OPE_CATEGORY IN ('ProcessStart','ProcessEnd','OperationComplete')                                                                         
         AND F.CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 day'
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_HIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_HIST")



def getHistResultSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_RESULT (                                           
       EQP_ID, PPID, QTY, PROCESS_START, PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER, CTRL_JOB                                                  
    )                                                                          
         SELECT MAX(F.EQP_ID) AS EQP_ID,                                                                                    
                MAX(F.PPID) AS PPID,                                                                                        
                MAX(F.QTY) AS QTY,                                                                                          
                MAX(F.PROCESS_START) AS PROCESS_START,                                                                                                                                                                                                             
                MAX(F.PROCESS_END) AS PROCESS_END,                                                                          
                MAX(F.CLAIM_MEMO) AS CLAIM_MEMO,
                MAX(TOOLG_ID) AS TOOLG_ID,
                MAX(PRODG1) AS PRODG1,
                MAX(LAYER) AS LAYER,
                CTRL_JOB                                                       
         FROM  {tempdb}APS_TMP_TYPE21_PARMODE_HIST F                                                                                        
         GROUP BY F.CTRL_JOB                                                                                     
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_RESULT",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_RESULT")

# 加入LCRECIPE_ID,DUMMY的时间加进来，最后再把dummy的数据过滤掉
def getNewTimeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # 先抓到ctrl_job 和 lcrecipe数据
    sql = """
        INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_LOTHISTORY (                                           
           EQP_ID, CTRL_JOB, RECIPE                                                 
        )                                                                          
             SELECT DISTINCT F.EQP_ID,                                                                                    
                    F.CTRL_JOB,
                    L.RECIPE                                                       
             FROM  APS_TMP_LOTHISTORY_VIEW.APS_TMP_LOTHISTORY_VIEW F                                                                                        
             INNER JOIN  APS_ETL_LOTHISTORY.APS_ETL_LOTHISTORY L
             ON L.LOT_ID = F.LOT_ID AND L.LAYER || '.' || L.STAGE = F.OPE_NO 
             AND L.TOOL_ID = F.EQP_ID
             WHERE F.OPE_CATEGORY = 'OperationComplete'                                                                                    
        """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_LOTHISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_LOTHISTORY")
    # 匹配出recipe
    sql = """
    INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_RECIPE (                                           
       EQP_ID, PPID, QTY, PROCESS_START, PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER, CTRL_JOB, RECIPE                                                
    )                                                                          
         SELECT F.EQP_ID,                                                                                    
                F.PPID,                                                                                        
                F.QTY,                                                                                          
                F.PROCESS_START,                                                                                                                                                                                                             
                F.PROCESS_END,                                                                          
                F.CLAIM_MEMO,
                F.TOOLG_ID,
                F.PRODG1,
                F.LAYER,
                F.CTRL_JOB,
                L.RECIPE                                                    
         FROM  {tempdb}APS_TMP_TYPE21_PARMODE_RESULT F                                                                                        
         LEFT JOIN {tempdb}APS_TMP_TYPE21_PARMODE_LOTHISTORY L
         ON L.CTRL_JOB = F.CTRL_JOB AND L.EQP_ID = F.EQP_ID                                                                                    
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_RECIPE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_RECIPE")

    # 把dummy的JION起来
    sql = """
       INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_JION (                                           
          EQP_ID, PPID, QTY, PROCESS_START, PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER, CTRL_JOB, RECIPE                                               
       )                                                                          
        SELECT F.EQP_ID,                                                                                    
               F.PPID,                                                                                        
               F.QTY,                                                                                          
               F.PROCESS_START,                                                                                                                                                                                                             
               F.PROCESS_END,                                                                          
               F.CLAIM_MEMO,
               F.TOOLG_ID,
               F.PRODG1,
               F.LAYER,
               F.CTRL_JOB,
               F.RECIPE                                                     
        FROM  {tempdb}APS_TMP_TYPE21_PARMODE_RECIPE F 
        UNION ALL
        SELECT DISTINCT F.EQP_ID,                                                                                    
               '' PPID,                                                                                        
               '' QTY,                                                                                          
               E.PROCESS_START_TIME AS PROCESS_START,                                                                                                                                                                                                             
               E.PROCESS_END_TIME AS PROCESS_END,                                                                          
               F.CLAIM_MEMO,
               F.TOOLG_ID,
               '' PRODG1,
               '' LAYER,
               '' CTRL_JOB,
               '' RECIPE                                                     
        FROM  {tempdb}APS_TMP_TYPE21_PARMODE_RECIPE F
        INNER JOIN APS_SYNC_OCS_AUTODUMMY_COMPLETE.APS_SYNC_OCS_AUTODUMMY_COMPLETE E
        ON E.TOOLID = F.EQP_ID 
        WHERE E.PROCESS_START_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 day'                                                                                                                                                                      
       """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_JION",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_JION")

    # 先进行dummy连run处理
    sql = """
           INSERT INTO {tempdb}APS_TMP_TYPE21_PARMODE_DUMMY (                                           
              EQP_ID, PPID, QTY, PROCESS_START, PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER, CTRL_JOB, RECIPE                                               
           )
           WITH SOURCE_DATA AS (                                                                          
                SELECT LAG(PROCESS_START,1,NULL) OVER(PARTITION BY F.EQP_ID,CLAIM_MEMO ORDER BY PROCESS_START) as NEXT_PROCESS_START,
                       LAG(PROCESS_END,1,NULL) OVER(PARTITION BY F.EQP_ID,CLAIM_MEMO ORDER BY PROCESS_END)  as NEXT_PROCESS_END,
                       F.EQP_ID,                                                                                   
                       F.PPID,                                                                                        
                       F.QTY,                                                                                          
                       F.PROCESS_START,                                                                                                                                                                                                             
                       F.PROCESS_END,                                                                          
                       F.CLAIM_MEMO,
                       F.TOOLG_ID,
                       F.PRODG1,
                       F.LAYER,
                       F.CTRL_JOB,
                       F.RECIPE                                                     
                FROM  {tempdb}APS_TMP_TYPE21_PARMODE_JION F  
           )   
           SELECT DISTINCT EQP_ID, PPID, QTY, 
                  CASE WHEN PROCESS_START > NEXT_PROCESS_START              
                                     AND PROCESS_START < NEXT_PROCESS_END AND NEXT_PROCESS_END < PROCESS_END 
                                     THEN NEXT_PROCESS_END        
                                     ELSE PROCESS_START END AS PROCESS_START,
                  PROCESS_END, CLAIM_MEMO, TOOLG_ID, PRODG1, LAYER, CTRL_JOB, RECIPE
           FROM SOURCE_DATA 
           WHERE RECIPE <> ''                                                                                                                                                                
           """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_TYPE21_PARMODE_DUMMY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_TYPE21_PARMODE_DUMMY")



def GetType21FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_TR_TYPE21_PARMODE (                                                              
            GUID, TOOLG_ID, TOOL_ID, PPID, PROCESS_START, PROCESS_END, QTY, CLAIM_MEMO, MEMO, PARTKEY, PRODG1, LAYER, CTRL_JOB, RECIPE
    )                                                                                                                       
    with split_data as (
        select ppid,qty,
               PROCESS_START,
               PROCESS_END,
               CLAIM_MEMO,
               TOOLG_ID, 
               EQP_ID,
               TOOLG_ID,
               PRODG1,
               LAYER,
               CTRL_JOB,
               RECIPE,
               unnest(string_split(replace(replace(replace(replace(replace(CLAIM_MEMO,'&','|'),'@',''),';',''),')',''),'(',''),'|')) AS MEMO
        from {tempdb}APS_TMP_TYPE21_PARMODE_DUMMY
        WHERE PROCESS_END IS NOT NULL and PROCESS_START is not null 
        order by PROCESS_END desc
    ),
    dis_data as (
      select   distinct ppid,qty,
               PROCESS_START,
               PROCESS_END,
               CLAIM_MEMO,
               EQP_ID,
               TOOLG_ID,
               PRODG1,
               LAYER,
               CTRL_JOB,
               RECIPE,
               MEMO
        from split_data
    ),
    xin_data as (
        SELECT LAG(PROCESS_START,1,NULL) OVER(PARTITION BY PT.EQP_ID,MEMO ORDER BY PROCESS_START) as NEXT_PROCESS_START,
               LAG(PROCESS_END,1,NULL) OVER(PARTITION BY PT.EQP_ID,MEMO ORDER BY PROCESS_END)  as NEXT_PROCESS_END,
               PROCESS_START,PROCESS_END,
               TOOLG_ID,
               PRODG1,
               LAYER,
               CTRL_JOB,
               RECIPE,
               ppid,qty, CLAIM_MEMO,MEMO,EQP_ID
        from dis_data PT
    ),
    result_data as (
        select  '{uuid}' AS GUID, 
                    CASE WHEN PROCESS_START > NEXT_PROCESS_START              
                                     AND PROCESS_START < NEXT_PROCESS_END AND NEXT_PROCESS_END < PROCESS_END 
                                     THEN NEXT_PROCESS_END        
                                     ELSE PROCESS_START END AS PROCESS_START, 
                   PROCESS_END,
                   TOOLG_ID,
                   PRODG1,
                   LAYER,
                   CTRL_JOB,
                   ppid,qty, CLAIM_MEMO,MEMO,EQP_ID,RECIPE
        from xin_data   
    )                                                                                                                                             
     SELECT  GUID,                                                                                                                                                                                                              
             TOOLG_ID,                                                                                                      
             EQP_ID,
             PPID,                                                                                                  
             PROCESS_START,                                                                                                                                                                                                    
             PROCESS_END,                                                                                                                                                                                              
             QTY,  
             CLAIM_MEMO,
             MEMO,                                                                                                         
             PROCESS_START AS PARTKEY,
             PRODG1,
             LAYER,
             CTRL_JOB,
             RECIPE
     FROM  result_data T                                                                                                                                                                                                                                                                                         
    """.format(uuid=uuid, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE21_PARMODE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE21_PARMODE")


def GetType21OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT  /*+ append */  INTO APS_TR_TYPE21_PARMODE (                                                              
            GUID, TOOLG_ID, TOOL_ID, PPID, PROCESS_START, PROCESS_END, QTY, CLAIM_MEMO, MEMO, PARTKEY, PRODG1, LAYER, CTRL_JOB, RECIPE
    )                                                                                                                       
    with split_data as (
        select ppid,qty,
               PROCESS_START,
               PROCESS_END,
               CLAIM_MEMO,
               TOOLG_ID, 
               EQP_ID,
               TOOLG_ID,
               PRODG1, 
               LAYER,
               CTRL_JOB,
               RECIPE,
               unnest(string_split(replace(replace(replace(replace(replace(CLAIM_MEMO,'&','|'),'@',''),';',''),')',''),'(',''),'|')) AS MEMO
        from {tempdb}APS_TMP_TYPE21_PARMODE_DUMMY
        WHERE PROCESS_END IS NOT NULL and PROCESS_START is not null 
        order by PROCESS_END desc
    ),
    dis_data as (
      select   distinct ppid,qty,
               PROCESS_START,
               PROCESS_END,
               CLAIM_MEMO,
               EQP_ID,
               TOOLG_ID,
               PRODG1, 
               LAYER,
               CTRL_JOB,
               RECIPE,
               MEMO
        from split_data
    ),
    xin_data as (
        SELECT LAG(PROCESS_START,1,NULL) OVER(PARTITION BY PT.EQP_ID,MEMO ORDER BY PROCESS_START) as NEXT_PROCESS_START,
               LAG(PROCESS_END,1,NULL) OVER(PARTITION BY PT.EQP_ID,MEMO ORDER BY PROCESS_END)  as NEXT_PROCESS_END,
               PROCESS_START,PROCESS_END,
               TOOLG_ID,PRODG1, 
               LAYER,
               CTRL_JOB,
               ppid,qty, CLAIM_MEMO,MEMO,EQP_ID,RECIPE
        from dis_data PT
    ),
    result_data as (
        select  '{uuid}' AS GUID, 
                    CASE WHEN PROCESS_START > NEXT_PROCESS_START              
                                     AND PROCESS_START < NEXT_PROCESS_END AND NEXT_PROCESS_END < PROCESS_END 
                                     THEN NEXT_PROCESS_END        
                                     ELSE PROCESS_START END AS PROCESS_START, 
                   PROCESS_END,
                   TOOLG_ID,
                   PRODG1, 
                   LAYER,
                   CTRL_JOB,
                   ppid,qty, CLAIM_MEMO,MEMO,EQP_ID,RECIPE
        from xin_data   
    )                                                                                                                                             
     SELECT  GUID,                                                                                                                                                                                                              
             TOOLG_ID,                                                                                                      
             EQP_ID,
             PPID,                                                                                                  
             PROCESS_START,                                                                                                                                                                                                    
             PROCESS_END,                                                                                                                                                                                              
             QTY,  
             CLAIM_MEMO,
             MEMO,                                                                                                         
             PROCESS_START AS PARTKEY,
             PRODG1, 
             LAYER,
             CTRL_JOB,
             RECIPE
     FROM  result_data T    
     WHERE                                                                                                              
        NOT EXISTS (                                                                                                          
            SELECT 1  FROM LAST_APS_TR_TYPE21_PARMODE.APS_TR_TYPE21_PARMODE T21 WHERE 1=1                                        
            AND T21.TOOL_ID= T.EQP_ID AND T21.TOOLG_ID = T.TOOLG_ID                                                          
            AND T21.PPID = T.PPID AND T21.PROCESS_START = T.PROCESS_START                                                     
            AND COALESCE(T21.PRODG1,'*') =  COALESCE(T.PRODG1,'*') 
            AND T21.LAYER = T.LAYER   
            AND T21.PROCESS_END = T.PROCESS_END AND T21.QTY = T.QTY 
            AND T21.CTRL_JOB = T.CTRL_JOB                                                          
        )                                                                                                                                                                                                                                                                                        
    """.format(uuid=uuid, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE21_PARMODE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE21_PARMODE")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_TR_TYPE21_PARMODE_30M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_TR_TYPE21_PARMODE"
    used_table_list = ['APS_SYNC_ETL_TOOL',
                       'APS_MID_FHOPEHS_VIEW',
                       'APS_SYNC_PRODUCT',
                       'APS_SYNC_OCS_AUTODUMMY_COMPLETE',
                       'APS_TMP_LOTHISTORY_VIEW',
                       'APS_ETL_LOTHISTORY']
    target_table_sql = """
        create table {}APS_TR_TYPE21_PARMODE
        (
          toolg_id           VARCHAR(60) not null,
          tool_id            VARCHAR(60) not null,
          ppid               VARCHAR(60) not null,
          process_start      VARCHAR(60) not null,
          process_end        VARCHAR(60),
          qty                VARCHAR(60),
          guid               VARCHAR(60) not null,
          partkey            VARCHAR(60) not null,
          CLAIM_MEMO         VARCHAR(64),
          MEMO               VARCHAR(64),
          PRODG1             VARCHAR(64),
          LAYER              VARCHAR(64),
          CTRL_JOB           VARCHAR(64),
          RECIPE             VARCHAR(64)
        )
    """.format("")
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
#       # 获取Hist的数据
        getEtlToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取Chamber Rule规则
        getHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取最终的成型数据
        getHistResultSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 新加逻辑，recipe及dummy数据过滤
        getNewTimeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 取上一次的APS_TR_TYPE21版本
        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_TR_TYPE21_PARMODE")
        # if 取不到
        if file_name is None:
            GetType21FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # else 取到的情况下
        # 手动 Attach APS_TR_TYPE21 AS LAST_APS_TR_TYPE21
        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_TR_TYPE21_PARMODE",
                                 target_db_file=file_name)

            GetType21OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            last_table_name = 'LAST_APS_TR_TYPE21_PARMODE.APS_TR_TYPE21_PARMODE'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_TR_TYPE21_PARMODE DATA IS NULL")
                return
            sql = """insert into APS_TR_TYPE21_PARMODE select * from {last_table_name} 
                      WHERE CAST(PARTKEY AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '180 day'""".format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)

            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_TR_TYPE21_PARMODE")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_TR_TYPE21_PARMODE DATA IS NULL")
            return
        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        # 导出到目标文件中
        target_db_file = my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                                   in_process_db_file=in_process_db_file,
                                                                                   target_table=target_table,
                                                                                   current_time=current_time_short)
        # 写版本号
        my_oracle.HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, 'XETL035')
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
