import gc
import logging
import os
import shutil

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, my_file


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
        这里在内存中创建使用到的临时表
        '''
    sql = """
        create {table_type} table APS_ETL_TMP_TR_TYPE31_LOT_HIST
        (
          eqp_id     VARCHAR(64),
          ctrl_job   varchar(64),
          CLAIM_MEMO varchar(64),
          claim_time VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_ETL_TMP_TR_TYPE31_LOT_HIST_V1
        (
          eqp_id     VARCHAR(64),
          ctrl_job   varchar(64),
          CLAIM_MEMO varchar(64),
          claim_time VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_ETL_TMP_TR_TYPE31_TOOL
          (
            tool_id   VARCHAR(64),
            toolg_id  VARCHAR(64),
            module_id VARCHAR(64),
            eqp_category VARCHAR(64),
            EQP_CHAMBER_FLAG VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_ETL_TMP_TR_TYPE31_OCS_HIST
          (
            module_id       VARCHAR(64),
            toolg_id        VARCHAR(64),
            tool_id         VARCHAR(64),
            plan_id         VARCHAR(64),
            qty             decimal,
            process_start   VARCHAR(64),
            process_st_time VARCHAR(64),
            process_end     VARCHAR(64),
            BATCH_FLAG      VARCHAR(64),
            CJ_ID           VARCHAR(64),
            sub_name        VARCHAR(64),
            job_spec_id     VARCHAR(64)
          )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_ETL_TMP_TR_TYPE31_OCS_QTY
        (
            cj_id   VARCHAR(64),
            foup_id VARCHAR(64),
            BATCH_FLAG VARCHAR(64),
            qty     decimal
        )
            """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_ETL_TMP_TR_TYPE31_SUB_NAME
       (
           cj_id   VARCHAR(64),
           MTOOL_ID  VARCHAR(64),
           JOB_SPEC_ID VARCHAR(64),
           sub_name VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_ETL_TMP_TR_TYPE31_MAIN
    (
            TOOL_ID   VARCHAR(64),
            CJ_ID VARCHAR(64),
            PLAN_ID VARCHAR(64),
            JOB_SPEC_ID VARCHAR(64),
            recipe_count VARCHAR(64),
            qty VARCHAR(64)
    )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_ETL_TMP_TR_TYPE31_VIEW
       (
               CJ_ID   VARCHAR(64),
               FOUP_ID VARCHAR(64),
               JOB_SPEC_GROUP_ID VARCHAR(64),
               JOB_SPEC_ID VARCHAR(64),
               MTOOL_ID VARCHAR(64),
               SUB_NAME VARCHAR(64),
               SLOT   VARCHAR(64),
               PROCESS_ST_TIME VARCHAR(64),
               PROCESS_ED_TIME VARCHAR(64),
               STATUS_TIME VARCHAR(64),
               STATUS VARCHAR(64),
               recipe varchar(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_ETL_TMP_TR_TYPE31_TIME
       (
               MODULE_ID   VARCHAR(64),
               TOOLG_ID VARCHAR(64),
               TOOL_ID VARCHAR(64),
               PLAN_ID VARCHAR(64),
               QTY VARCHAR(64),
               SUB_NAME VARCHAR(64),
               CJ_ID   VARCHAR(64),
               PROCESS_START VARCHAR(64),
               PROCESS_END VARCHAR(64),
               job_spec_id VARCHAR(64),
               BATCH_FLAG VARCHAR(64),
               PARTKEY     VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)


    # 只有式chamber式机台 才需要考量连run
def GetDFRunHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_LOT_HIST_V1(                               
         EQP_ID, CTRL_JOB, CLAIM_TIME, CLAIM_MEMO                                            
     )                                                                  
      SELECT DISTINCT S.EQP_ID,
            s.CTRL_JOB,                                                                                                                     
            CASE WHEN OPE_CATEGORY='ProcessEnd' THEN s.CLAIM_TIME ELSE NULL END AS CLAIM_TIME,                     
            CASE WHEN s.CLAIM_MEMO is not null THEN replace(replace(s.CLAIM_MEMO,'(','') ,')@;','')  END AS CLAIM_MEMO                                
      FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW S                                            
      WHERE 1=1--position('F' in S.EQP_ID)=1                                       
      AND S.CLAIM_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 day'                                   
      AND S.OPE_CATEGORY in ('ProcessEnd' ,'OperationComplete' )                                                                
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_LOT_HIST_V1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_LOT_HIST_V1")

    sql = """
    INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_LOT_HIST(                               
        EQP_ID, CTRL_JOB, CLAIM_TIME, CLAIM_MEMO                                            
        )                                                                  
         SELECT max(S.EQP_ID) as eqp_id,
               s.CTRL_JOB,                                                                                                                     
               max(S.CLAIM_TIME) as CLAIM_TIME,                     
               max(S.CLAIM_MEMO) as claim_memo                                
         FROM {tempdb}APS_ETL_TMP_TR_TYPE31_LOT_HIST_V1 S
         inner join {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL T ON S.EQP_ID = T.TOOL_ID
         AND T.EQP_CHAMBER_FLAG ='Chamber'                                              
         group by S.CTRL_JOB  
         UNION ALL
         SELECT distinct MTOOL_ID AS EQP_ID,
                CJ_ID,
                PROCESS_ED_TIME AS CLAIM_TIME,
                (   SELECT  STRING_AGG(distinct SUB_NAME,';' ORDER BY SUB_NAME) 
                    FROM APS_TMP_OCS_MAIN_H_VIEW.APS_TMP_OCS_MAIN_H_VIEW OO 
                    WHERE OO.CJ_ID = OP.CJ_ID
               )  
         from APS_TMP_OCS_MAIN_H_VIEW.APS_TMP_OCS_MAIN_H_VIEW OP                                                         
   """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_LOT_HIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_LOT_HIST")


# 把ETL_TOOL拆分出来
def GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL(                                
         TOOL_ID, TOOLG_ID, MODULE_ID,EQP_CATEGORY,EQP_CHAMBER_FLAG                                              
     )                                                                   
      SELECT EQP_ID AS TOOL_ID,MAX(EQP_G) AS TOOLG_ID,                                                     
             MAX(REAL_MODULE) AS MODULE_ID,
             MAX(EQP_CATEGORY) AS EQP_CATEGORY,
             MAX(EQP_CHAMBER_FLAG) AS EQP_CHAMBER_FLAG                                                                             
      FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL                                                                   
      WHERE EQP_CATEGORY <> 'Measurement'                          
      GROUP BY EQP_ID                                                                   
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_TOOL")


def GetOcsHistQtySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW(                                                                     
           CJ_ID,FOUP_ID, JOB_SPEC_GROUP_ID, MTOOL_ID, SUB_NAME,SLOT, PROCESS_ST_TIME, PROCESS_ED_TIME, STATUS_TIME, STATUS, recipe,JOB_SPEC_ID
        )                                                                                                     
        SELECT CJ_ID,FOUP_ID, JOB_SPEC_GROUP_ID, MTOOL_ID, SUB_NAME,SLOT, PROCESS_ST_TIME, PROCESS_ED_TIME, STATUS_TIME, STATUS, recipe,JOB_SPEC_ID                                                                              
        FROM  APS_TMP_OCS_MAIN_H_VIEW.APS_TMP_OCS_MAIN_H_VIEW H                                                                                           
        WHERE H.PROCESS_ED_TIME IS NOT NULL AND H.PROCESS_ED_TIME <> '' AND H.STATUS_TIME >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 day'                              
       AND H.PROCESS_ST_TIME IS NOT NULL AND H.PROCESS_ST_TIME <> ''  AND H.STATUS_TIME <= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '1 day' 
       AND STATUS='OpeComplete'                                                                                                                                
       """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_VIEW",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_VIEW")

    sql = """
        INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_SUB_NAME(                                                                     
           CJ_ID,MTOOL_ID,JOB_SPEC_ID,SUB_NAME
        )                                                                                                     
        SELECT CJ_ID,MTOOL_ID,JOB_SPEC_ID,STRING_AGG(distinct H.SUB_NAME,',' ORDER BY H.SUB_NAME) AS SUB_NAME                                                                              
        FROM  {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW H 
        inner join {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL T ON h.MTOOL_ID = T.TOOL_ID
        AND T.EQP_CHAMBER_FLAG ='Chamber' 
        GROUP BY CJ_ID,MTOOL_ID,JOB_SPEC_ID                                                                                                                                                                                                                      
       """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_SUB_NAME",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_SUB_NAME")

    sql = """
        INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_OCS_QTY(                                                                     
           CJ_ID, FOUP_ID, BATCH_FLAG, QTY
           )                                                                                                     
         WITH SLOT_DATA AS (                                                                                                                                               
            SELECT H.CJ_ID,H.FOUP_ID,                                                                  
                  CASE WHEN MAX(T.TOOL_ID) IS NOT NULL AND MAX(T.TOOL_ID) <> '' THEN 'Y' ELSE 'N' END AS BATCH_FLAG,            
                  STRING_AGG(H.SLOT,',' ORDER BY H.SLOT) AS SLOT                  
            FROM  {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW H                                                                 
            LEFT JOIN {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL T                                                          
            ON T.TOOL_ID = H.MTOOL_ID  AND T.EQP_CATEGORY = 'Internal Buffer'                                                      
           GROUP BY H.CJ_ID,H.FOUP_ID                                                             
         )                                                                                                  
         SELECT CJ_ID,FOUP_ID,BATCH_FLAG,                                                                              
                CASE WHEN BATCH_FLAG = 'Y' THEN 1                                                      
               ELSE LENGTH(SLOT) - LENGTH(REPLACE(SLOT,',',''))+1 END AS QTY                                
         FROM SLOT_DATA                                                                                                                                 
       """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_OCS_QTY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_OCS_QTY")

    # QTY 要按JOB_SPEC_GROUP_ID、MTOOL_ID,CJ_ID 去看
    # 按CJ_ID,MTOOL_ID 去看RECIPE有多少个
    sql = """
       INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_MAIN(                                                                     
          TOOL_ID, CJ_ID, PLAN_ID, JOB_SPEC_ID, recipe_count, qty
          )  
       WITH SLOT_DATA AS (                                                                                                                                               
            SELECT H.CJ_ID,H.FOUP_ID,h.JOB_SPEC_GROUP_ID,H.MTOOL_ID, H.JOB_SPEC_ID,                                                                 
                  CASE WHEN MAX(T.TOOL_ID) IS NOT NULL AND MAX(T.TOOL_ID) <> '' THEN 'Y' ELSE 'N' END AS BATCH_FLAG,            
                  STRING_AGG(H.SLOT,',' ORDER BY H.SLOT) AS SLOT                  
            FROM  {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW H                                                                 
            LEFT JOIN {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL T                                                          
            ON T.TOOL_ID = H.MTOOL_ID  AND T.EQP_CATEGORY = 'Internal Buffer'                                                       
           GROUP BY H.CJ_ID,H.FOUP_ID, H.JOB_SPEC_GROUP_ID,H.MTOOL_ID, H.JOB_SPEC_ID                                                             
        ),
        recipe_sub as (
            select cj_id,mtool_id,sub_name,JOB_SPEC_ID,JOB_SPEC_GROUP_ID,
                   count(1) over(partition by cj_id,mtool_id,sub_name order by cj_id)  as row_num
            from {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW H
        ) ,
        recipe_count as (
            select cj_id,mtool_id,JOB_SPEC_ID,JOB_SPEC_GROUP_ID,
                   max(row_num) as row_num
            from recipe_sub
            group by cj_id,mtool_id,JOB_SPEC_ID,JOB_SPEC_GROUP_ID
        )                                                                                                
        SELECT distinct H.MTOOL_ID AS TOOL_ID,                                                                  
               H.CJ_ID,                                                                                
               H.JOB_SPEC_GROUP_ID AS PLAN_ID,
               H.JOB_SPEC_ID,
               CASE WHEN BATCH_FLAG = 'Y' THEN 1 else r.row_num end as recipe_count,                                                                            
               CASE WHEN BATCH_FLAG = 'Y' THEN 1                                                      
                    ELSE LENGTH(SLOT) - LENGTH(REPLACE(SLOT,',',''))+1 END AS QTY                                
        FROM SLOT_DATA h   
        left join recipe_count r
        on r.cj_id = h.cj_id and r.mtool_id = h.mtool_id 
        and r.JOB_SPEC_ID = h.JOB_SPEC_ID and r.JOB_SPEC_GROUP_ID = h.JOB_SPEC_GROUP_ID                                                                                                                                                                                             
      """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_MAIN",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_MAIN")


# 获取OCS的HIST
def GetOcsHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_OCS_HIST(                                                                     
         MODULE_ID, TOOLG_ID, TOOL_ID, PLAN_ID, QTY, PROCESS_START, PROCESS_ST_TIME, PROCESS_END,BATCH_FLAG,CJ_ID,sub_name,job_spec_id
        )                                                                                                     
         WITH MAIN_DATA AS (                                                                                  
               SELECT H.MTOOL_ID AS TOOL_ID,                                                                  
                      H.CJ_ID,                                                                                
                      H.JOB_SPEC_GROUP_ID AS PLAN_ID, 
                      T.BATCH_FLAG,
                      H.job_spec_id,  
                      n.sub_name,                                                      
                      CASE WHEN T.QTY > 25 THEN 25 ELSE T.QTY END AS QTY,                 
                      H.PROCESS_ST_TIME AS PROCESS_START,                 
                      H.PROCESS_ST_TIME  AS PROCESS_ST_TIME,           
                      H.PROCESS_ED_TIME AS PROCESS_END                 
               FROM {tempdb}APS_ETL_TMP_TR_TYPE31_VIEW H  
               INNER JOIN {tempdb}APS_ETL_TMP_TR_TYPE31_OCS_QTY T 
               ON T.CJ_ID = H.CJ_ID AND T.FOUP_ID = H.FOUP_ID  
               left join {tempdb}APS_ETL_TMP_TR_TYPE31_SUB_NAME n
               on n.cj_id = h.cj_id and n.MTOOL_ID = h.MTOOL_ID and n.JOB_SPEC_ID = h.JOB_SPEC_ID                                                                                                   
         ),                                                                                                   
         GROUP_MAIN_DATA AS (                                                                                 
               SELECT MAX(TOOL_ID) AS TOOL_ID,                                                                
                      MAX(PLAN_ID) AS PLAN_ID,                                                                
                      MAX(QTY) AS QTY,
                      CJ_ID,
                      job_spec_id,
                      max(BATCH_FLAG) as BATCH_FLAG, 
                      sub_name,                                                                       
                      MAX(PROCESS_START) AS PROCESS_START,                                                    
                      MIN(PROCESS_ST_TIME) AS PROCESS_ST_TIME,                                                
                      MAX(PROCESS_END) AS PROCESS_END                                                         
               FROM MAIN_DATA                                                                                 
               GROUP BY CJ_ID,job_spec_id,sub_name                                                                                 
         )                                                                                                    
         SELECT DISTINCT T.MODULE_ID,                                                                                  
                T.TOOLG_ID,                                                                                   
                M.TOOL_ID,                                                                                    
                M.PLAN_ID,                                                                                    
                M.QTY,                                                                                        
                M.PROCESS_START,                                                                              
                M.PROCESS_ST_TIME,                                                                            
                M.PROCESS_END,
                m.BATCH_FLAG,
                m.CJ_ID ,
                m.sub_name,
                m.job_spec_id                                                                               
         FROM GROUP_MAIN_DATA M                                                                               
         INNER JOIN {tempdb}APS_ETL_TMP_TR_TYPE31_TOOL T                                                                    
         ON T.TOOL_ID = M.TOOL_ID                                                                             
         WHERE M.PROCESS_START IS NOT NULL AND M.PROCESS_START <> ''                                                                                                                                  
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_OCS_HIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_OCS_HIST")

# 把ETL_TOOL拆分出来
def GetProcessStartDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT INTO {tempdb}APS_ETL_TMP_TR_TYPE31_TIME(                                
         MODULE_ID, TOOLG_ID, TOOL_ID,PLAN_ID,QTY,CJ_ID,job_spec_id, sub_name,PROCESS_START, PROCESS_END, PARTKEY, BATCH_FLAG                                              
         ) 
         with source_data as (                                                                  
           SELECT T.MODULE_ID,                                                                                           
                   T.TOOLG_ID,                                                                                            
                   T.TOOL_ID,                                                                                             
                   T.PLAN_ID,                                                                                             
                   T.QTY, 
                   t.CJ_ID,
                   t.job_spec_id,  
                   t.sub_name,
                   CASE WHEN L.EQP_ID IS NOT NULL AND L.EQP_ID <> '' THEN STRFTIME(cast(L.CLAIM_TIME as timestamp), '%Y-%m-%d %H:%M:%S')            
                                       ELSE STRFTIME(cast(T.PROCESS_ST_TIME as timestamp), '%Y-%m-%d %H:%M:%S') END AS PROCESS_START,                                                     
                   --TO_CHAR(T.PROCESS_END, 'YYYY-MM-DD HH24:MI:SS') AS PROCESS_END, 
                   STRFTIME(cast(T.PROCESS_END as timestamp), '%Y-%m-%d %H:%M:%S') AS PROCESS_END,                                       
                   CASE WHEN L.EQP_ID IS NOT NULL AND L.EQP_ID <> '' THEN L.CLAIM_TIME ELSE T.PROCESS_ST_TIME END AS PARTKEY,
                   T.BATCH_FLAG AS BATCH_FLAG                                                                         
            FROM {tempdb}APS_ETL_TMP_TR_TYPE31_OCS_HIST T                                                                                    
            LEFT JOIN {tempdb}APS_ETL_TMP_TR_TYPE31_LOT_HIST L                                                                           
            ON L.EQP_ID = T.TOOL_ID  and l.CLAIM_MEMO like '%'|| t.sub_name || '%'                                                                                       
            AND CAST(L.CLAIM_TIME AS timestamp) > CAST(T.PROCESS_START AS timestamp)                                                                            
            AND CAST(L.CLAIM_TIME AS timestamp) < CAST(T.PROCESS_END AS timestamp)                                                                                      
            WHERE  1=1 AND T.PROCESS_START IS NOT NULL AND T.PROCESS_START <> '' 
        ),
        row_data as (
            select  *,
                    row_number() over(partition by TOOLG_ID,TOOL_ID,PLAN_ID, QTY, CJ_ID, process_end,job_spec_id,sub_name order by process_start desc) as rnt
            from SOURCE_DATA
         ) 
         select MODULE_ID, TOOLG_ID, TOOL_ID,PLAN_ID,QTY,CJ_ID,job_spec_id, sub_name,PROCESS_START, PROCESS_END, PARTKEY, BATCH_FLAG 
         from row_data d
         where d.rnt = 1                                                                
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TMP_TR_TYPE31_TIME",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TMP_TR_TYPE31_TIME")

def GetType31FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_TR_TYPE31 (                                               
       GUID, MODULE_ID, TOOLG_ID, TOOL_ID, PLAN_ID, JOB_SPEC_ID, QTY, PROCESS_START, PROCESS_END, PARTKEY,BATCH_FLAG, recipe_count                                                                                       
    )                                                                                                     
    with result_data as (
            select  D.MODULE_ID,                                                                                                
                    D.TOOLG_ID,                                                                                                 
                    D.TOOL_ID,                                                                                                  
                    M.PLAN_ID, 
                    M.JOB_SPEC_ID,                                                                                                
                    m.QTY, 
                    m.recipe_count,                                                                                                     
                    max(d.process_start) as process_start,                                                                                            
                    D.PROCESS_END,                                                                                              
                    D.PARTKEY,
                    D.BATCH_FLAG
            from {tempdb}APS_ETL_TMP_TR_TYPE31_MAIN m
            inner join {tempdb}APS_ETL_TMP_TR_TYPE31_TIME D
            ON D.CJ_ID = M.CJ_ID and  M.JOB_SPEC_ID = D.JOB_SPEC_ID 
            group by D.MODULE_ID,                                                                                                
                    D.TOOLG_ID,                                                                                                 
                    D.TOOL_ID,                                                                                                  
                    M.PLAN_ID, 
                    M.JOB_SPEC_ID,                                                                                                 
                    m.QTY,  
                    m.recipe_count,                                                                                                                                                                                                
                    D.PROCESS_END,                                                                                              
                    D.PARTKEY,
                    D.BATCH_FLAG,
                    d.cj_id
     )                                                                                                             
     SELECT '{uuid}' AS GUID,                                                                                       
            MODULE_ID,                                                                                                
            TOOLG_ID,                                                                                                 
            TOOL_ID,                                                                                                  
            PLAN_ID, 
            JOB_SPEC_ID,                                                                                                 
            QTY,                                                                                                      
            PROCESS_START,                                                                                            
            PROCESS_END,                                                                                              
            PARTKEY,
            BATCH_FLAG,
            recipe_count                                                                                                  
     FROM result_data                                                                                                                                                                    
    """.format(uuid=uuid, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE31",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE31")


def GetType31OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT  /*+ append */  INTO APS_TR_TYPE31 (                                               
       GUID, MODULE_ID, TOOLG_ID, TOOL_ID, PLAN_ID, JOB_SPEC_ID, QTY, PROCESS_START, PROCESS_END, PARTKEY,BATCH_FLAG,RECIPE_COUNT                                                                                       
    )                                                                                                     
       WITH result_data as (
            select  D.MODULE_ID,                                                                                                
                    D.TOOLG_ID,                                                                                                 
                    D.TOOL_ID,                                                                                                  
                    M.PLAN_ID, 
                    M.JOB_SPEC_ID,                                                                                                 
                    m.QTY, 
                    m.recipe_count,                                                                                                     
                    max(d.process_start) as process_start,                                                                                            
                    D.PROCESS_END,                                                                                              
                    D.PARTKEY,
                    D.BATCH_FLAG
            from {tempdb}APS_ETL_TMP_TR_TYPE31_MAIN m
            inner join {tempdb}APS_ETL_TMP_TR_TYPE31_TIME D
            ON D.CJ_ID = M.CJ_ID and  M.JOB_SPEC_ID = D.JOB_SPEC_ID 
            group by D.MODULE_ID,                                                                                                
                    D.TOOLG_ID,                                                                                                 
                    D.TOOL_ID,                                                                                                  
                    M.PLAN_ID,  
                    M.JOB_SPEC_ID,                                                                                                
                    m.QTY,  
                    m.recipe_count,                                                                                                                                                                                                
                    D.PROCESS_END,                                                                                              
                    D.PARTKEY,
                    D.BATCH_FLAG,
                    d.cj_id
         )                                                                                                             
         SELECT '{uuid}' AS GUID,                                                                                       
                MODULE_ID,                                                                                                
                TOOLG_ID,                                                                                                 
                TOOL_ID,                                                                                                  
                PLAN_ID, 
                JOB_SPEC_ID,                                                                                                 
                QTY,                                                                                                      
                PROCESS_START,                                                                                            
                PROCESS_END,                                                                                              
                PARTKEY,
                BATCH_FLAG,
                recipe_count                                                                                                  
         FROM result_data T                                                                                         
         WHERE  1=1                                                                                                 
         AND NOT EXISTS                                                                                             
            (                                                                                                          
                SELECT MODULE_ID FROM LAST_APS_TR_TYPE31.APS_TR_TYPE31 T31                                                                
                WHERE T31.MODULE_ID = T.MODULE_ID                                                                      
                AND T31.TOOLG_ID = T.TOOLG_ID                                                                          
                AND T31.TOOL_ID = T.TOOL_ID                                                                            
                AND T31.PLAN_ID = T.PLAN_ID
                AND T31.JOB_SPEC_ID = T.JOB_SPEC_ID                                                                            
                AND T31.QTY = T.QTY                                                                                    
                AND T31.PROCESS_START = T.PROCESS_START                                                                
                AND T31.PROCESS_END = T.PROCESS_END                                                                    
            )                                                                                                                                                                   
    """.format(uuid=uuid, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TR_TYPE31",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TR_TYPE31")


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
    ETL_Proc_Name = "APS_TR_BR.APS_TR_TYPE31_30M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_TR_TYPE31"
    used_table_list = ['APS_MID_FHOPEHS_VIEW', 'APS_SYNC_ETL_TOOL', 'APS_TMP_OCS_MAIN_H_VIEW']
    target_table_sql = """
        create table {}APS_TR_TYPE31
        (
          module_id     VARCHAR(60),
          toolg_id      VARCHAR(60),
          tool_id       VARCHAR(60),
          plan_id       VARCHAR(60),
          job_spec_id   VARCHAR(60),
          qty           VARCHAR(60),
          process_start VARCHAR(60),
          process_end   VARCHAR(60),
          guid          VARCHAR(60) not null,
          partkey       VARCHAR(60) not null,
          BATCH_FLAG    VARCHAR(60),
          RECIPE_COUNT  VARCHAR(60)
        )
    """.format("") # 注意:这里一定要这么写 [create table 表名] => [create table {}表名]
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
        # 把ETL_TOOL拆分出来
        GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 先把针对DF的RUN货历史找出来
        GetDFRunHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取OCS的数量按CTRL_JOB+FOUP
        GetOcsHistQtySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取OCS的HIST
        GetOcsHistSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 单独拎出来
        GetProcessStartDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 取上一次的APS_TR_TYPE31版本
        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_TR_TYPE31")
        # if 取不到
        if file_name is None:
            GetType31FirstSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # else 取到的情况下
        # 手动 Attach APS_TR_TYPE31 AS LAST_APS_TR_TYPE31
        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_TR_TYPE31",
                                 target_db_file=file_name)

            GetType31OtherSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            last_table_name = 'LAST_APS_TR_TYPE31.APS_TR_TYPE31'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_TR_TYPE31 DATA IS NULL")
                return
            sql = """insert into APS_TR_TYPE31 select * from {last_table_name}
                    WHERE CAST(partkey AS TIMESTAMP) > DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '180 day'""".format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)

            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_TR_TYPE31")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_TR_TYPE31 DATA IS NULL")
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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file, 'XETL037')
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