import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons, my_postgres, my_file, my_runner, my_cmder
from xinxiang.util.oracle_to_duck_his import delete_over_three_version

import pandas as pd

def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    '''
    这里在内存中创建使用到的临时表
    '''
    sql = """
    create {table_type} table APS_TMP_ETL_LOTHISTROY
    (
        lot_id             VARCHAR(64),
        prodspec_id        VARCHAR(64),
        mainpd_id          VARCHAR(64),
        priority           VARCHAR(64),
        ope_no             VARCHAR(64),
        wafer_qty          VARCHAR(64),
        reticle_id         VARCHAR(128),
        prev_op_comp_time  VARCHAR(64),
        op_start_date_time VARCHAR(64),
        claim_time         VARCHAR(64),
        pass_count         VARCHAR(64),
        ctrl_job           VARCHAR(64),
        process_start_time VARCHAR(64),
        eqp_id             VARCHAR(64),
        toolg_id           VARCHAR(64),
        real_module        VARCHAR(64),
        ope_seq            VARCHAR(64),
        chamber_set        VARCHAR(512),
        batch_id           VARCHAR(64),
        sche_toolg_id      VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_LOTHISTROY_FHOPEH
    (
        ctrl_job     VARCHAR(64),
        lot_id       VARCHAR(64),
        tech_id      VARCHAR(64),
        ph_recipe_id VARCHAR(64),
        customer_id  VARCHAR(64),
        lot_type     VARCHAR(64),
        claim_time   VARCHAR(64),
        ope_category VARCHAR(64),
        eqp_id       VARCHAR(64),
        ope_no       VARCHAR(64),
        plan_id      VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_LOTHISTROY_RECIPE
    (
        lcrecipe_id VARCHAR(64),
        ope_no      VARCHAR(64),
        prodspec_id VARCHAR(64),
        mainpd_id   VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create table APS_TMP_ETL_LOTHISTROY_RECIPE1
    (
        lcrecipe_id VARCHAR(64),
        ope_no      VARCHAR(64),
        prodspec_id VARCHAR(64),
        mainpd_id   VARCHAR(64)
    )
    """
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_FLOW
     (
        prod_id VARCHAR(64),
        plan_id VARCHAR(64),
        layer   VARCHAR(64),
        stage   VARCHAR(64),
        step_id VARCHAR(64),
        ope_no  VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_PO_N1
     (
        pos_pdid  VARCHAR(64) not null,
        mainpd_id VARCHAR(64) not null,
        ope_no    VARCHAR(10) not null
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_EQP_LOAD
     (
        eqp_id      VARCHAR(10),
        lcrecipe_id VARCHAR(40),
        pd_id       VARCHAR(40) not null,
        prodspec_id VARCHAR(30) not null
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_RECIPE_GROUP
     (
        tool_id       VARCHAR(64),
        ppid          VARCHAR(64),
        rtd_groupname VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_PROCESSDATA
     (
        ctrl_job      VARCHAR(64),
        lot_id        VARCHAR(64),
        tech_id       VARCHAR(64),
        ph_recipe_id  VARCHAR(64),
        customer_id   VARCHAR(64),
        lot_type      VARCHAR(64),
        rtd_groupname VARCHAR(64),
        reserve_time  VARCHAR(64),
        process_end   VARCHAR(64),
        scanner_end   VARCHAR(64),
        scanner_start VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_SPLITDATA
     (
        ope_no     VARCHAR(64),
        lot_id     VARCHAR(64),
        split_time VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHISTROY_TOOLDATA
     (
        toolg_id     VARCHAR(64),
        tool_id      VARCHAR(64),
        module       VARCHAR(64),
        eqp_category VARCHAR(64),
        eqp_chamber_flag VARCHAR(64),
        SCHE_TOOLG_ID   VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHS_PRODG
     (
        prodg_id    VARCHAR(64),
        prodg_tech  VARCHAR(64),
        prodspec_id VARCHAR(64) not null,
        PLAT_FORM   VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_LOTHS_V1
     (
        lot_id             VARCHAR(64),
        eqp_g              VARCHAR(64),
        prodspec_id        VARCHAR(64),
        mainpd_id          VARCHAR(64),
        step_id            VARCHAR(64),
        priority           VARCHAR(64),
        ope_no             VARCHAR(64),
        wafer_qty          VARCHAR(64),
        reticle_id         VARCHAR(128),
        prev_op_comp_time  VARCHAR(64),
        op_start_date_time VARCHAR(64),
        claim_time         VARCHAR(64),
        pass_count         VARCHAR(64),
        real_module        VARCHAR(64),
        ctrl_job           VARCHAR(64),
        process_start_time VARCHAR(64),
        eqp_id             VARCHAR(64),
        chamber_set        VARCHAR(512),
        batch_id           VARCHAR(64),
        sche_toolg_id       VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE
       (
            line              VARCHAR(60),
            prod_id           VARCHAR(64),
            ope_no            VARCHAR(64),
            eqp_id            VARCHAR(64),
            recipe_id         VARCHAR(128),
            chamber_user_flag VARCHAR(64),
            ch_id             VARCHAR(512)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_LOTHISTROY_EQP_CHAMBER_RULE
       (
            eqp_id VARCHAR(64),
            ch_id  VARCHAR(512)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_LOTHISTROY_ONLY_EQP_CHAMBER_RULE
       (
         eqp_id VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_LOTHISTROY_RDS_RECIPEINFO
           (
                tool_id          VARCHAR(64),
                runtimegroupname VARCHAR(128),
                ppid             VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP
           (
                tool_id   VARCHAR(64),
                recipe_id VARCHAR(64),
                group_id  VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create {table_type} table APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP1
      (
           tool_id   VARCHAR(64),
           recipe_id VARCHAR(64),
           group_id  VARCHAR(64)
      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_LOTHISTROY_IMP_SOURCE
           (
                mainpd_id VARCHAR(64),
                ope_no    VARCHAR(64),
                source    VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_LOTHS_V2
        (
              lot_id         VARCHAR(64),
              toolg_id       VARCHAR(64),
              tool_id        VARCHAR(64),
              prod_id        VARCHAR(64),
              plan_id        VARCHAR(64),
              step_id        VARCHAR(64),
              lot_type       VARCHAR(64),
              pty            VARCHAR(64),
              shr_flag      VARCHAR(64),
              layer          VARCHAR(256),
              stage          VARCHAR(256),
              recipe         VARCHAR(64),
              ppid           VARCHAR(64),
              qty            VARCHAR(64),
              reticle_id     VARCHAR(128),
              tech_id        VARCHAR(64),
              customer       VARCHAR(64),
              arrival        VARCHAR(19),
              job_prepare    VARCHAR(19),
              track_in       VARCHAR(19),
              process_start  VARCHAR(19),
              scanner_start  VARCHAR(19),
              scanner_end    VARCHAR(19),
              process_end    VARCHAR(19),
              track_out      VARCHAR(19),
              rework_flag    VARCHAR(19),
              backup_flag    VARCHAR(19),
              module         VARCHAR(64),
              prodg_id       VARCHAR(64),
              prodg_tech     VARCHAR(12),
              update_time    VARCHAR(19),
              rtd_groupname  VARCHAR(64),
              chamber_set    VARCHAR(512),
              plat_form      VARCHAR(64),
              batch_id       VARCHAR(64),
              sche_toolg_id  VARCHAR(64)
        )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_LOTHISTORY_OCS_HIST
        (
              cj_id                VARCHAR(64),
              qty                  VARCHAR(64),
              toolg_id             VARCHAR(64),
              mtool_id             VARCHAR(64),
              job_spec_group_id    VARCHAR(64),
              recipe               VARCHAR(64),
              reticle              VARCHAR(64),
              process_st_time      VARCHAR(64),
              process_ed_time      VARCHAR(64),
              module               VARCHAR(64),
              sche_toolg_id        VARCHAR(64)
        )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
             create {table_type} table APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1
            (
                  cj_id                VARCHAR(64),
                  slot                 VARCHAR(128),
                  mtool_id             VARCHAR(64),
                  job_spec_group_id    VARCHAR(64),
                  recipe               VARCHAR(64),
                  reticle              VARCHAR(64),
                  process_st_time      VARCHAR(64),
                  process_ed_time      VARCHAR(64)
            )
               """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
            create {table_type} table APS_TMP_ETL_LOTHISTORY_OCS_HIST_V2
           (
                 cj_id                VARCHAR(64),
                 mtool_id             VARCHAR(64),
                 job_spec_group_id    VARCHAR(64),
                 recipe               VARCHAR(64),
                 reticle              VARCHAR(64),
                 process_st_time      VARCHAR(64),
                 process_ed_time      VARCHAR(64)
           )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
            create {table_type} table APS_TMP_ETL_LOTHISTORY_OCS_HIST_V3
           (
                 cj_id                VARCHAR(64),
                 slot                 VARCHAR(128)
           )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_ETL_LOTHISTORY_OCS_HIST_V4
       (
             cj_id                VARCHAR(64),
             slot                 VARCHAR(128),
             mtool_id             VARCHAR(64),
             job_spec_group_id    VARCHAR(64),
             recipe               VARCHAR(64),
             reticle              VARCHAR(64),
             process_st_time      VARCHAR(64),
             process_ed_time      VARCHAR(64)
       )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_ETL_LOTHS_PLAT_FORM_DATA
       (
             prodg1               VARCHAR(64),
             plat_form            VARCHAR(64)
       )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY
        (
             LOT_ID               VARCHAR(64),
             GROUP_ID             VARCHAR(64),
             LOT_PRIORITY         VARCHAR(64),
             SHR_FLAG             VARCHAR(64)
        )
          """.format(table_type=table_type)
    duck_db.sql(sql)


def GetRecipeGroupSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE_GROUP(      
          TOOL_ID, PPID, RTD_GROUPNAME                                           
        )                                                        
         SELECT DISTINCT P.TOOL_ID,                              
                P.PPID,                                          
                MAX(P.RTD_GROUPNAME) AS RTD_GROUPNAME            
         FROM APS_SYNC_RTD_RECIPEGROUP.APS_SYNC_RTD_RECIPEGROUP P 
         WHERE P.RTD_GROUPNAME <> '*'                             
         GROUP BY P.TOOL_ID,P.PPID                                                                                                  
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_RECIPE_GROUP",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_RECIPE_GROUP")


def getNpwRecipeGroupData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP(               
            TOOL_ID, RECIPE_ID, GROUP_ID
            )                                                               
             SELECT P.TOOL_ID,P.RECIPE_ID,                                  
                    REPLACE(MAX(P.GROUP_ID),'%','*') AS GROUP_ID            
             FROM APS_SYNC_NPW_RECIPE_GROUP.APS_SYNC_NPW_RECIPE_GROUP P       
             WHERE P.GROUP_ID <> '*'                                           
             GROUP BY P.TOOL_ID,P.RECIPE_ID                                                                                                                    
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP")


def getRtdImpSourceData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_IMP_SOURCE(                     
            MAINPD_ID, OPE_NO, SOURCE
            )                                                               
             SELECT S.MAINPD_ID,S.OPE_NO,                                   
                    MAX(S.SOURCE) AS SOURCE                                 
             FROM APS_SYNC_RTD_IMP_SOURCE.APS_SYNC_RTD_IMP_SOURCE S         
             --WHERE S.PARTCODE= ''   
             GROUP BY S.MAINPD_ID,S.OPE_NO                                                                                                                                   
          """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_IMP_SOURCE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_IMP_SOURCE")


def getRdsRecipeInfoData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
      INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_RDS_RECIPEINFO(                 
            TOOL_ID, RUNTIMEGROUPNAME, PPID
      )                                                               
     SELECT F.TOOLID,F.RUNTIMEGROUPNAME,                                                          
     SUBSTR(REPLACE(F.DKEY,'.AA',''), position('.' in REPLACE(F.DKEY,'.AA',''))+1) AS PPID           
     FROM APS_SYNC_RDS_RECIPEINFO.APS_SYNC_RDS_RECIPEINFO F                                       
     WHERE F.RUNTIMEGROUPNAME IS NOT NULL and  F.RUNTIMEGROUPNAME <> '' AND position('C' in F.TOOLID)=1                                                                                                                                                                                        
          """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_RDS_RECIPEINFO",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_RDS_RECIPEINFO")


def GetPoN1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_PO_N1(                          
         POS_PDID, MAINPD_ID, OPE_NO                                            
        )                                                                    
         SELECT N.POS_PDID,N.MAINPD_ID,N.OPE_NO                                                                    
         FROM APS_SYNC_PF_PO_N1.APS_SYNC_PF_PO_N1 N                               
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_PO_N1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_PO_N1")


def GetRecipeEqpLoadSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_EQP_LOAD(                         
         EQP_ID, LCRECIPE_ID, PD_ID, PRODSPEC_ID                                                        
        )                                                                         
         SELECT L.EQP_ID,L.LCRECIPE_ID,                                                            
                L.PD_ID,L.PRODSPEC_ID                                                                      
         FROM APS_SYNC_PD_RECIPE_EQP_MAIN.APS_SYNC_PD_RECIPE_EQP_MAIN L                                                                                                  
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_EQP_LOAD",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_EQP_LOAD")


def GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_TOOLDATA( 
           TOOLG_ID, TOOL_ID, MODULE, EQP_CATEGORY, EQP_CHAMBER_FLAG, SCHE_TOOLG_ID                              
        )                                                 
         SELECT MAX(EQP_G) AS TOOLG_ID,                  
                EQP_ID AS TOOL_ID,                       
                MAX(REAL_MODULE) AS MODULE,              
                MAX(EQP_CATEGORY) AS EQP_CATEGORY,
                MAX(EQP_CHAMBER_FLAG) AS EQP_CHAMBER_FLAG,
                MAX(SCHE_EQP_G) AS SCHE_TOOLG_ID      
         FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL    
         WHERE HOST_EQP_FLAG ='Y'                           
         GROUP BY EQP_ID                                                                   
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_TOOLDATA",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_TOOLDATA")


def InsertChamberRuleDataView2Temp(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # 把机台对应所有的CH信息列出来
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_EQP_CHAMBER_RULE(                                                       
             EQP_ID, CH_ID                                                                                
             )                                                                                                   
              WITH EQP_CH_ID AS (                                                                                
                   SELECT SUBSTRING(T.EQP_ID, 1, position('.' in T.EQP_ID) -1) AS EQP_ID,                               
                   EQP_ID AS CH_ID                                                                               
                   FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T WHERE T.EQP_CHAMBER_FLAG='Chamber'           
                   AND T.HOST_EQP_FLAG ='N'              
              )                                                                                                  
              SELECT DISTINCT EQP_ID,                                                                            
                     STRING_AGG(CH_ID,';') OVER (PARTITION BY EQP_ID ORDER BY CH_ID) AS EQP_CH_ID    
              FROM EQP_CH_ID                                                                                                                                                              
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_EQP_CHAMBER_RULE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_EQP_CHAMBER_RULE")

    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE(                                                                                      
             LINE, EQP_ID, RECIPE_ID, CH_ID                                                                                                          
           )                                                                                                                                  
            WITH  CHAMBER_RULE_DATA AS (                                                                                                                                                                                         
               SELECT CASE WHEN position('|' in CHAMBER_RULE)=0 AND position('&' in CHAMBER_RULE)=0 THEN CHAMBER_RULE                                     
                           ELSE REPLACE(REPLACE(REPLACE(REPLACE(CHAMBER_RULE,'(',''),')',''),'|',',@'),'&',';@')                                   
                      END AS RULE_NAME,                                                                                                       
                      R.LINE,R.EQP_ID,                                                                                                        
                      R.RECIPE_ID                                                                                                             
               FROM APS_SYNC_CSCMBMRCPRULE.APS_SYNC_CSCMBMRCPRULE R                                                                                                                                           
            ),                                                                                                                                
            ALL_DATA AS (                                                                                                                     
               SELECT                                                                                                                         
                   R.LINE, R.EQP_ID,                                                                                                          
                   R.RECIPE_ID,                                                                                                               
                   R.EQP_ID||'.'|| REPLACE(RULE_NAME,'@',R.EQP_ID||'.') AS CH_ID                                                              
               FROM                                                                                                                           
               CHAMBER_RULE_DATA R                                                                                                            
            )                                                                                                                                 
            SELECT                                                                                                                            
                  R.LINE,R.EQP_ID,                                                                                                            
                  R.RECIPE_ID,                                                                                                                
                  CASE WHEN LENGTH(R.CH_ID) > 512 THEN SUBSTRING(R.CH_ID,1,512) ELSE R.CH_ID END AS CH_ID                                        
            FROM                                                                                                                              
            ALL_DATA R                                                                                                                                                                                                                                         
            """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE")

    # 存放由ChamberRule机台信息
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_ONLY_EQP_CHAMBER_RULE(                           
            EQP_ID                                                            
        )                                                                           
         SELECT DISTINCT R.EQP_ID                                                                                       
         FROM  {tempdb}APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE R                                                                                                                                                                                                                                  
                """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_ONLY_EQP_CHAMBER_RULE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_ONLY_EQP_CHAMBER_RULE")


def GetViewFirstLotHistorySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY(                           
             LOT_ID,PRODSPEC_ID,MAINPD_ID,PRIORITY,OPE_NO,OPE_SEQ,WAFER_QTY,
             RETICLE_ID,PREV_OP_COMP_TIME,OP_START_DATE_TIME,CLAIM_TIME,PASS_COUNT,
             CTRL_JOB,PROCESS_START_TIME,EQP_ID,TOOLG_ID,REAL_MODULE,CHAMBER_SET,BATCH_ID,SCHE_TOOLG_ID                                              
       )                                                                         
        SELECT HS.LOT_ID,                                                                        
             HS.PRODSPEC_ID,                                                                     
             HS.MAINPD_ID,                                                                                                                                                                        
             HS.PRIORITY_CLASS AS PRIORITY,                                                                       
             HS.OPE_NO,                                                                          
             HS.OPE_SEQ,                                                                             
             HS.WAFER_QTY,HS.RETICLE_ID,                                                         
             HS.PREV_OP_COMP_TIME,                                                               
             HS.OP_START_DATE_TIME,                                                              
             HS.CLAIM_TIME,                                                                      
             HS.PASS_COUNT,                                                                      
             HS.CTRL_JOB,                                                                        
             HS.PROCESS_START_TIME,                                                              
             HS.EQP_ID,                                                                          
             TD.TOOLG_ID,TD.MODULE,
             '' AS CHAMBER_SET,
             HS.BATCH_ID,
             TD.SCHE_TOOLG_ID                                                              
        FROM APS_TMP_LOTHISTORY_VIEW.APS_TMP_LOTHISTORY_VIEW HS                                      
        INNER JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_TOOLDATA TD                                               
        ON TD.TOOL_ID = HS.EQP_ID                                                                 
        WHERE 1=1                                                                                 
             AND HS.OPE_CATEGORY ='OperationComplete'                                             
             AND CAST(HS.CLAIM_TIME AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '40 day'
             AND hs.OP_START_DATE_TIME is not null                                                                                                                                                            
     """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY")


def GetViewOtherLotHistorySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY(                           
             LOT_ID,PRODSPEC_ID,MAINPD_ID,PRIORITY,OPE_NO,OPE_SEQ,WAFER_QTY,
             RETICLE_ID,PREV_OP_COMP_TIME,OP_START_DATE_TIME,CLAIM_TIME,PASS_COUNT,
             CTRL_JOB,PROCESS_START_TIME,EQP_ID,TOOLG_ID,REAL_MODULE,CHAMBER_SET,BATCH_ID,SCHE_TOOLG_ID                                              
       )                                                                         
        SELECT HS.LOT_ID,                                                                        
             HS.PRODSPEC_ID,                                                                     
             HS.MAINPD_ID,                                                                                                                                                                        
             HS.PRIORITY_CLASS AS PRIORITY,                                                                       
             HS.OPE_NO,                                                                          
             HS.OPE_SEQ,                                                                             
             HS.WAFER_QTY,HS.RETICLE_ID,                                                         
             HS.PREV_OP_COMP_TIME,                                                               
             HS.OP_START_DATE_TIME,                                                              
             HS.CLAIM_TIME,                                                                      
             HS.PASS_COUNT,                                                                      
             HS.CTRL_JOB,                                                                        
             HS.PROCESS_START_TIME,                                                              
             HS.EQP_ID,                                                                          
             TD.TOOLG_ID,TD.MODULE,
             CASE WHEN TD.EQP_CHAMBER_FLAG='Chamber' AND TRIM(TME.EQP_ID) IS NOT NULL and TRIM(TME.EQP_ID) <> '' THEN TM.CH_ID
                  WHEN TD.EQP_CHAMBER_FLAG='Chamber' AND TRIM(TME.EQP_ID) IS NULL THEN ETN.CH_ID END CHAMBER_SET,
             HS.BATCH_ID,
             TD.SCHE_TOOLG_ID                                                             
        FROM APS_TMP_LOTHISTORY_VIEW.APS_TMP_LOTHISTORY_VIEW HS                                      
        INNER JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_TOOLDATA TD                                               
        ON TD.TOOL_ID = HS.EQP_ID  
        LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_CHAMBER_RULE TM ON  TM.RECIPE_ID = HS.RECIPE_ID   AND TM.EQP_ID = HS.EQP_ID   
        LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_ONLY_EQP_CHAMBER_RULE TME ON TME.EQP_ID = HS.EQP_ID  
        LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_EQP_CHAMBER_RULE ETN ON ETN.EQP_ID = HS.EQP_ID                                                                 
        WHERE 1=1                                                                                 
             AND HS.OPE_CATEGORY ='OperationComplete'                                             
             AND CAST(HS.CLAIM_TIME AS TIMESTAMP) >=  DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '6 hour' 
             AND hs.OP_START_DATE_TIME is not null                                                                                                                                                
     """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY")


def GetViewFirstFhopehSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_FHOPEH(                         
       CTRL_JOB, LOT_ID, TECH_ID, PH_RECIPE_ID, CUSTOMER_ID, LOT_TYPE, CLAIM_TIME, OPE_CATEGORY, EQP_ID, OPE_NO,plan_id                                                                            
     )                                                                                      
      SELECT F.CTRL_JOB,F.LOT_ID,                                                        
             F.TECH_ID,                                                                  
             F.PH_RECIPE_ID,                                                             
             F.CUSTOMER_ID,                                                              
             F.LOT_TYPE,                                                                 
             F.CLAIM_TIME,                                                               
             F.OPE_CATEGORY,                                                             
             F.EQP_ID,                                                                   
             F.OPE_NO,MAINPD_ID                                                                 
      FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW F                                                       
      WHERE 1=1                                                                          
      AND F.OPE_CATEGORY IN ('Reserve','ProcessEnd','ScannerEnd','ScannerStart','Split','OperationComplete')   
        AND CAST(F.CLAIM_TIME AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '50 day'                           
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_FHOPEH",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_FHOPEH")


def GetViewOtherFhopehSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_FHOPEH(                         
       CTRL_JOB, LOT_ID, TECH_ID, PH_RECIPE_ID, CUSTOMER_ID, LOT_TYPE, CLAIM_TIME, OPE_CATEGORY, EQP_ID, OPE_NO,plan_id                                                                            
     )                                                                                      
      SELECT F.CTRL_JOB,F.LOT_ID,                                                        
             F.TECH_ID,                                                                  
             F.PH_RECIPE_ID,                                                             
             F.CUSTOMER_ID,                                                              
             F.LOT_TYPE,                                                                 
             F.CLAIM_TIME,                                                               
             F.OPE_CATEGORY,                                                             
             F.EQP_ID,                                                                   
             F.OPE_NO,MAINPD_ID                                                                 
      FROM APS_MID_FHOPEHS_VIEW.APS_MID_FHOPEHS_VIEW F                                                       
      WHERE 1=1                                                                          
      AND F.OPE_CATEGORY IN ('Reserve','ProcessEnd','ScannerEnd','ScannerStart','Split', 'OperationComplete')   
        AND CAST(F.CLAIM_TIME AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '1 day'                           
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_FHOPEH",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_FHOPEH")


def GetFlowRecipeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE1(                                          
               LCRECIPE_ID, OPE_NO, PRODSPEC_ID, MAINPD_ID                                                                              
         )                                                                                               
         SELECT MAX(BB.LCRECIPE_ID) AS LCRECIPE_ID,                                                                      
                AA.OPE_NO,BB.PRODSPEC_ID,AA.MAINPD_ID                                                                            
         FROM  {tempdb}APS_TMP_ETL_LOTHISTROY_PO_N1 AA,  
          APS_SYNC_PD_RECIPE_EQP_MAIN.APS_SYNC_PD_RECIPE_EQP_MAIN BB,                                 
          {tempdb}APS_TMP_ETL_LOTHISTROY LL                                                               
         WHERE 1=1                                                                                                   
         AND AA.POS_PDID = BB.PD_ID                                                                    
         AND LL.PRODSPEC_ID = BB.PRODSPEC_ID                                                           
         AND LL.OPE_NO = AA.OPE_NO                                                                     
         AND LL.MAINPD_ID = AA.MAINPD_ID                                                               
         GROUP BY AA.OPE_NO,BB.PRODSPEC_ID ,AA.MAINPD_ID                                                                                                                                                
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_RECIPE1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_RECIPE1")


def GetRealFlowRecipeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE(                                          
               LCRECIPE_ID, OPE_NO, PRODSPEC_ID, MAINPD_ID                                                                              
         )                                                                                               
        SELECT LCRECIPE_ID,OPE_NO,PRODSPEC_ID,MAINPD_ID
         FROM  {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE1                                                                                                                                           
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_RECIPE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_RECIPE")


def GetFlowDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_FLOW(                                                    
          PROD_ID, PLAN_ID, LAYER, STAGE, STEP_ID,OPE_NO                                                                                
         )                                                                                                
          SELECT PROD_ID,PLAN_ID,                                                                                          
                 LAYER,STAGE,STEP_ID,
                 LAYER||'.'||STAGE AS OPE_NO                                                                                                 
          FROM APS_ETL_FLOW.APS_ETL_FLOW                                                                                                                                        
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_FLOW",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_FLOW")

def GetShrFlagData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT /*+ append */ INTO {tempdb}APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY
         (LOT_ID, GROUP_ID, LOT_PRIORITY, SHR_FLAG)                            
         SELECT P.LOT_ID, D.GROUP_ID, P.LOT_PRIORITY,
           CASE WHEN D.GROUP_ID = 1 THEN 0
                WHEN D.GROUP_ID = 2 THEN 1
                WHEN D.GROUP_ID = 3 THEN 2
                WHEN D.GROUP_ID = 4 THEN 3
                WHEN CAST(D.GROUP_ID AS DECIMAL) >= 5 AND CAST(D.GROUP_ID AS DECIMAL) <= 9 THEN 4
                ELSE -1 END AS SHR_FLAG
        FROM APS_SYNC_LOT_PRIORITY.APS_SYNC_LOT_PRIORITY P
        INNER JOIN APS_SYNC_PRIORITY_DEFINE.APS_SYNC_PRIORITY_DEFINE D
        ON CAST(D.PRI_FROM AS DECIMAL) <= CAST(P.LOT_PRIORITY AS DECIMAL)
        AND CAST(D.PRI_TO AS DECIMAL) >= CAST(P.LOT_PRIORITY AS DECIMAL)
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY")


def GetProcessDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT /*+ append */ INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP1                                                                                                                 
       (TOOL_ID, RECIPE_ID, GROUP_ID) 
       WITH RESULT_DATA AS (
           SELECT DISTINCT EQP_ID,PH_RECIPE_ID AS PPID
           FROM {tempdb}APS_TMP_ETL_LOTHISTROY_FHOPEH
       )                                                                                                                                                                          
       SELECT D.EQP_ID,D.PPID,
              CASE WHEN MAX(G.TOOL_ID) IS NOT NULL THEN MAX(G.GROUP_ID) ELSE MAX(G1.GROUP_ID) END AS GROUP_ID 
       FROM RESULT_DATA D                                    
       LEFT JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP G
       ON G.TOOL_ID = D.EQP_ID AND D.PPID = G.RECIPE_ID
       INNER JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP G1
       ON G1.TOOL_ID = D.EQP_ID AND INSTR(D.PPID,G1.RECIPE_ID) = 1 
       GROUP BY  D.EQP_ID,D.PPID 
      """.format(tempdb=my_duck.get_temp_table_mark(), current_time=current_time)
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP1")

    sql = """
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_PROCESSDATA(                                                     
               CTRL_JOB, LOT_ID, TECH_ID, PH_RECIPE_ID, CUSTOMER_ID, LOT_TYPE,
               RTD_GROUPNAME, RESERVE_TIME, PROCESS_END, SCANNER_END, SCANNER_START                                                                                                   
       )                                                                                                                 
        SELECT CTRL_JOB,LOT_ID,MAX(TECH_ID) AS TECH_ID,                                                                                                                   
                 MAX(PH_RECIPE_ID) AS PH_RECIPE_ID,                                                                      
                 MAX(CUSTOMER_ID) AS CUSTOMER_ID,                                                                        
                 MAX(LOT_TYPE) AS LOT_TYPE,                                                                              
                 MAX(RTD_GROUPNAME) AS RTD_GROUPNAME,                                                                    
                 MAX(RESERVE_TIME) AS RESERVE_TIME,                                                                      
                 MAX(PROCESS_END) AS PROCESS_END,                                                                        
                 MAX(SCANNER_END) AS SCANNER_END,                                                                        
                 MAX(SCANNER_START) AS SCANNER_START                                                                     
        FROM (                                                                                                           
        SELECT CTRL_JOB,LOT_ID,PH_RECIPE_ID,TECH_ID,CUSTOMER_ID,LOT_TYPE,    
         CASE WHEN ETR.TOOL_ID IS NOT NULL and ETR.TOOL_ID <> '' THEN ETR.RTD_GROUPNAME 
              WHEN RTS.MAINPD_ID IS NOT NULL and RTS.MAINPD_ID <> ''  THEN RTS.SOURCE         
              WHEN NRG.TOOL_ID IS NOT NULL and NRG.TOOL_ID <> '' THEN NRG.GROUP_ID
              WHEN RRI.TOOL_ID IS NOT NULL THEN RRI.RUNTIMEGROUPNAME   
         END AS RTD_GROUPNAME,                            
             CASE WHEN OPE_CATEGORY='Reserve' THEN CLAIM_TIME ELSE NULL END AS RESERVE_TIME,                        
             CASE WHEN OPE_CATEGORY='ProcessEnd' THEN CLAIM_TIME ELSE NULL END AS PROCESS_END,                      
             CASE WHEN OPE_CATEGORY='ScannerEnd' THEN CLAIM_TIME ELSE NULL END AS SCANNER_END,                      
             CASE WHEN OPE_CATEGORY='ScannerStart' THEN CLAIM_TIME ELSE NULL END AS SCANNER_START                   
          FROM {tempdb}APS_TMP_ETL_LOTHISTROY_FHOPEH LH                                                                          
          LEFT JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE_GROUP ETR ON ETR.TOOL_ID = LH.EQP_ID AND ETR.PPID = LH.PH_RECIPE_ID   
          LEFT JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_NPW_RECIPE_GROUP1 NRG ON NRG.TOOL_ID = LH.EQP_ID AND NRG.RECIPE_ID = LH.PH_RECIPE_ID       
          LEFT JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_IMP_SOURCE RTS ON RTS.MAINPD_ID = LH.PLAN_ID AND RTS.OPE_NO = LH.OPE_NO
          LEFT JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_RDS_RECIPEINFO RRI ON RRI.TOOL_ID = LH.EQP_ID AND RRI.PPID = LH.PH_RECIPE_ID      
          WHERE OPE_CATEGORY IN ('Reserve','ProcessEnd','ScannerEnd','ScannerStart','OperationComplete')                                                                          
        ) T                                                                                                              
        GROUP BY T.CTRL_JOB,T.LOT_ID                                                                                                                       
       """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_PROCESSDATA",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_PROCESSDATA")


def GetSplitDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT INTO  {tempdb}APS_TMP_ETL_LOTHISTROY_SPLITDATA(                                                        
               OPE_NO, LOT_ID, SPLIT_TIME                                                                                                     
         )                                                                                                                  
         SELECT OPE_NO,LOT_ID,MAX(SPLIT_TIME) AS SPLIT_TIME                                                                   
         FROM (                                                                                                 
              SELECT OPE_NO,LOT_ID,                                                                              
                     CLAIM_TIME AS SPLIT_TIME                                                                             
                     FROM {tempdb}APS_TMP_ETL_LOTHISTROY_FHOPEH                                                             
              WHERE OPE_CATEGORY IN ('Split')                                                                                                                             
         ) T                                                                                                    
         GROUP BY T.OPE_NO,T.LOT_ID                                               
          """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTROY_SPLITDATA",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTROY_SPLITDATA")

    sql = """      
        INSERT INTO  {tempdb}APS_TMP_ETL_LOTHS_PRODG
        ( PRODG_ID,PRODG_TECH,PRODSPEC_ID, PLAT_FORM)
        SELECT MAX(P.PRODG1) AS PRODG_ID, MAX(GROUP_TECH) AS PRODG_TECH,PRODSPEC_ID
               ,MAX(M.PLAT_FORM) AS PLAT_FORM                                                                             
        FROM APS_SYNC_PRODUCT.APS_SYNC_PRODUCT P
        LEFT JOIN APS_SYNC_PRODUCT_PLATFORM.APS_SYNC_PRODUCT_PLATFORM M
        ON M.PRODG1 = P.PRODG1                                                                                                              
        GROUP BY PRODSPEC_ID                                                                                              
        """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHS_PRODG",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHS_PRODG")

    sql = """      
           INSERT INTO  {tempdb}APS_TMP_ETL_LOTHS_V1(
              LOT_ID,EQP_G,PRODSPEC_ID,MAINPD_ID,STEP_ID,PRIORITY,OPE_NO,WAFER_QTY,RETICLE_ID,                                       
              PREV_OP_COMP_TIME,OP_START_DATE_TIME,CLAIM_TIME,PASS_COUNT,REAL_MODULE,CTRL_JOB,
              PROCESS_START_TIME,EQP_ID,CHAMBER_SET,BATCH_ID,SCHE_TOOLG_ID
              )                                                 
             SELECT HS.LOT_ID,MAX(HS.TOOLG_ID) AS EQP_G,HS.PRODSPEC_ID,                                                                                                  
                  HS.MAINPD_ID,                                                                                                                                          
                  FL.STEP_ID AS STEP_ID,                                                                                                                                 
                  MAX(HS.PRIORITY) AS PRIORITY, HS.OPE_NO,                                                                                                               
                  MAX(HS.WAFER_QTY) AS WAFER_QTY,MAX(HS.RETICLE_ID) AS RETICLE_ID,                                                                                       
                  MAX(HS.PREV_OP_COMP_TIME) AS PREV_OP_COMP_TIME,HS.OP_START_DATE_TIME,                                                                                  
                  MAX(HS.CLAIM_TIME) AS CLAIM_TIME,MAX(HS.PASS_COUNT) AS PASS_COUNT,                                                                                     
                  MAX(HS.REAL_MODULE) AS REAL_MODULE, MAX(HS.CTRL_JOB) AS CTRL_JOB,                                                                                      
                  MAX(HS.PROCESS_START_TIME) AS PROCESS_START_TIME,HS.EQP_ID,
                  MAX(HS.CHAMBER_SET) AS CHAMBER_SET,
                  MAX(HS.BATCH_ID) AS BATCH_ID,
                  MAX(HS.SCHE_TOOLG_ID) AS SCHE_TOOLG_ID                                                                                            
                  FROM {tempdb}APS_TMP_ETL_LOTHISTROY HS                                                                                                                 
                  INNER JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_FLOW FL                                                                                                                  
                  ON FL.PROD_ID = HS.PRODSPEC_ID AND FL.PLAN_ID = HS.MAINPD_ID                                                                                      
                  AND FL.OPE_NO = HS.OPE_NO                                                                                      
                  GROUP BY HS.LOT_ID,HS.PRODSPEC_ID, HS.MAINPD_ID,   
                  HS.OPE_NO,HS.EQP_ID, HS.OP_START_DATE_TIME,FL.STEP_ID                                                                                            
                  UNION ALL                                                                                                                                             
                  SELECT HS.LOT_ID,MAX(HS.TOOLG_ID) AS EQP_G,HS.PRODSPEC_ID,                                                                                             
                  HS.MAINPD_ID,                                                                                                                                          
                  '.9999.9999' AS STEP_ID,                                                                                                                               
                  MAX(HS.PRIORITY) AS PRIORITY, HS.OPE_NO,                                                                                                               
                  MAX(HS.WAFER_QTY) AS WAFER_QTY,MAX(HS.RETICLE_ID) AS RETICLE_ID,                                                                                       
                  MAX(HS.PREV_OP_COMP_TIME) AS PREV_OP_COMP_TIME,HS.OP_START_DATE_TIME,                                                                                  
                  MAX(HS.CLAIM_TIME) AS CLAIM_TIME,MAX(HS.PASS_COUNT) AS PASS_COUNT,                                                                                     
                  MAX(HS.REAL_MODULE) AS REAL_MODULE, MAX(HS.CTRL_JOB) AS CTRL_JOB,                                                                                      
                  MAX(HS.PROCESS_START_TIME) AS PROCESS_START_TIME,HS.EQP_ID,
                  MAX(HS.CHAMBER_SET) AS CHAMBER_SET,
                  MAX(HS.BATCH_ID) AS BATCH_ID,
                  MAX(HS.SCHE_TOOLG_ID) AS SCHE_TOOLG_ID                                                                                            
                  FROM  {tempdb}APS_TMP_ETL_LOTHISTROY HS                                                                                                                 
                  WHERE POSITION('DYB' IN HS.MAINPD_ID) =1                                                                                                                                                                                                                                             
                  GROUP BY HS.LOT_ID,HS.PRODSPEC_ID, HS.MAINPD_ID,                                                                                                      
                  HS.OPE_NO,HS.EQP_ID, HS.OP_START_DATE_TIME                                                                                                 
           """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHS_V1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHS_V1")


def OtherSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
      INSERT INTO  {tempdb}APS_TMP_ETL_LOTHS_V2(LOT_ID,TOOLG_ID,TOOL_ID,PROD_ID,PLAN_ID,STEP_ID,LOT_TYPE,PTY,SHR_FLAG,                                              
         LAYER,STAGE,RECIPE,PPID,QTY,RETICLE_ID,TECH_ID,CUSTOMER,ARRIVAL,JOB_PREPARE,TRACK_IN,PROCESS_START,SCANNER_START,                                         
         SCANNER_END,PROCESS_END,TRACK_OUT,REWORK_FLAG,BACKUP_FLAG,MODULE,PRODG_ID,PRODG_TECH,UPDATE_TIME,RTD_GROUPNAME,
         CHAMBER_SET, PLAT_FORM, BATCH_ID, SCHE_TOOLG_ID)                               
             SELECT                                                                                                                                                
             LH.LOT_ID,                                                                                                                                            
             LH.EQP_G AS TOOLG_ID,                                                                                                                                 
             LH.EQP_ID AS TOOL_ID,                                                                                                                                 
             LH.PRODSPEC_ID AS PROD_ID,                                                                                                                            
             LH.MAINPD_ID AS PLAN_ID,                                                                                                                              
             LH.STEP_ID,                                                                                                                                           
             FH.LOT_TYPE AS LOT_TYPE,                                                                                                                              
             CASE WHEN PT.LOT_PRIORITY IS NULL THEN 500 ELSE PT.LOT_PRIORITY END AS PTY,                                                                                         
             CASE WHEN PT.SHR_FLAG IS NULL THEN -1 ELSE PT.SHR_FLAG END AS SHR_FLAG,                                                                                                                         
             SUBSTRING(LH.OPE_NO, 1, POSITION('.' IN LH.OPE_NO)-1 ) AS LAYER,                                                                                           
             SUBSTRING(LH.OPE_NO, POSITION('.' IN LH.OPE_NO)+1 ) AS STAGE,                                                                                              
             FO.LCRECIPE_ID AS RECIPE,                                                                                                                             
             FH.PH_RECIPE_ID AS PPID,                                                                                                                              
             LH.WAFER_QTY AS QTY,                                                                                                                                  
             LH.RETICLE_ID,                                                                                                                                        
             FH.TECH_ID AS TECH_ID,                                                                                                                                
             FH.CUSTOMER_ID AS CUSTOMER,                                                                                                                           
             CASE WHEN SA.LOT_ID IS NOT NULL AND CAST(SA.SPLIT_TIME AS TIMESTAMP) > CAST(LH.PREV_OP_COMP_TIME AS TIMESTAMP) AND CAST(SA.SPLIT_TIME AS TIMESTAMP) < CAST(LH.OP_START_DATE_TIME AS TIMESTAMP) THEN                                    
             SA.SPLIT_TIME ELSE  LH.PREV_OP_COMP_TIME END AS ARRIVAL,                          
             FH.RESERVE_TIME AS JOB_PREPARE,                                                                                      
             LH.OP_START_DATE_TIME AS TRACK_IN,                                                                                   
             COALESCE(LH.PROCESS_START_TIME,LH.OP_START_DATE_TIME) AS PROCESS_START,                  
             FH.SCANNER_START AS SCANNER_START,                                                                                   
             FH.SCANNER_END AS SCANNER_END,                                                                                       
             COALESCE(FH.PROCESS_END,LH.CLAIM_TIME) AS PROCESS_END,                                   
             LH.CLAIM_TIME AS TRACK_OUT,                                                                                          
             CASE WHEN SUBSTRING(LH.MAINPD_ID,1,1)='Y' THEN 'Y' ELSE 'N' END AS REWORK_FLAG,                                                                          
             'N' AS BACKUP_FLAG,                                                                                                                                   
             LH.REAL_MODULE AS MODULE, P.PRODG_ID, P.PRODG_TECH,                                                                                                   
             '{current_time}' AS UPDATE_TIME,                                                                                                                
             FH.RTD_GROUPNAME AS RTD_GROUPNAME, LH.CHAMBER_SET,
             P.PLAT_FORM, LH.BATCH_ID, LH.SCHE_TOOLG_ID                                                                                                   
             FROM  {tempdb}APS_TMP_ETL_LOTHS_V1 LH                                                                                                                   
             LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_PROCESSDATA FH ON LH.LOT_ID = FH.LOT_ID AND LH.CTRL_JOB = FH.CTRL_JOB                                                     
             LEFT JOIN APS_SYNC_SHR_PRI.APS_SYNC_SHR_PRI SP ON SP.LOT_ID = LH.LOT_ID                                                                                    
             LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHS_PRODG P ON P.PRODSPEC_ID = LH.PRODSPEC_ID                                                                           
             LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE FO ON FO.PRODSPEC_ID = LH.PRODSPEC_ID AND FO.MAINPD_ID = LH.MAINPD_ID AND FO.OPE_NO = LH.OPE_NO                
             LEFT JOIN  {tempdb}APS_TMP_ETL_LOTHISTROY_SPLITDATA SA ON LH.LOT_ID = SA.LOT_ID AND LH.OPE_NO = SA.OPE_NO 
             LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY PT ON PT.LOT_ID = LH.LOT_ID    
    """.format(current_time=current_time,tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHS_V2",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHS_V2")

    sql = """
        INSERT  /*+ append */  INTO APS_ETL_LOTHISTORY(                                                                                                           
             LOT_ID, TOOLG_ID, TOOL_ID, PROD_ID, PLAN_ID, STEP_ID, LOT_TYPE,PTY,SHR_FLAG, 
              LAYER, STAGE, RECIPE, PPID, QTY, RETICLE_ID, TECH_ID, CUSTOMER, ARRIVAL, JOB_PREPARE,
              TRACK_IN, PROCESS_START, PROCESS_END, TRACK_OUT, REWORK_FLAG, BACKUP_FLAG, MODULE,
              PRODG_ID, PRODG_TECH, UPDATE_TIME, PARTKEY, SCANNER_START, SCANNER_END, RECIPE_SETUP,
              CH_SET, WIP_ATTR2, BATCH_ID, SCHE_TOOLG_ID                                                                                                                                         
        )                                                                                                                                                          
         SELECT distinct LOT_ID,                                                                                                                                             
             LH.TOOLG_ID,                                                                                                                                           
             LH.TOOL_ID,                                                                                                                                            
             LH.PROD_ID,                                                                                                                                            
             LH.PLAN_ID,                                                                                                                                            
             LH.STEP_ID,                                                                                                                                            
             LH.LOT_TYPE,                                                                                                                                           
             LH.PTY,                                                                                                                                                
             LH.SHR_FLAG,                                                                                                                                           
             LH.LAYER,                                                                                                                                              
             LH.STAGE,                                                                                                                                              
             LH.RECIPE,                                                                                                                                             
             LH.PPID,                                                                                                                                               
             LH.QTY,                                                                                                                                                
             LH.RETICLE_ID,                                                                                                                                         
             LH.TECH_ID,                                                                                                                                            
             LH.CUSTOMER,                                                                                                                                           
             LH.ARRIVAL,                                                                                                                                            
             LH.JOB_PREPARE,                                                                                                                                        
             LH.TRACK_IN,                                                                                                                                           
             LH.PROCESS_START,                                                                                                                                     
             LH.PROCESS_END,                                                                                                                                        
             LH.TRACK_OUT,                                                                                                                                          
             LH.REWORK_FLAG,                                                                                                                                        
             LH.BACKUP_FLAG,                                                                                                                                        
             LH.MODULE,                                                                                                                                             
             LH.PRODG_ID,                                                                                                                                           
             LH.PRODG_TECH,                                                                                                                                         
             LH.UPDATE_TIME,                                                                                                                                        
             LH.TRACK_IN AS PARTKEY,                                                                                             
             LH.SCANNER_START,                                                                                                                                      
             LH.SCANNER_END,                                                                                                                                        
             LH.RTD_GROUPNAME AS RECIPE_SETUP,
             LH.CHAMBER_SET, LH.PLAT_FORM,
             LH.BATCH_ID,
             LH.SCHE_TOOLG_ID                                                                                                       
          FROM  {tempdb}APS_TMP_ETL_LOTHS_V2 LH                                                                                                                        
          WHERE 1=1                                                                                                                                                 
          AND NOT EXISTS (                                                                                                                                          
                    SELECT H.LOT_ID  FROM LAST_APS_ETL_LOTHISTORY.APS_ETL_LOTHISTORY H                                                                                                      
                    WHERE H.LOT_ID = LH.LOT_ID                                                                                                                      
                    AND H.TOOL_ID = LH.TOOL_ID AND H.PROD_ID = LH.PROD_ID                                                                                           
                    AND H.PLAN_ID = LH.PLAN_ID                                                                                                                      
                    AND H.TRACK_IN = LH.TRACK_IN
                    AND CAST(H.TRACK_IN AS TIMESTAMP)>= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '10 day')      
     """.format(current_time=current_time,tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_LOTHISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_LOTHISTORY")


def OtherMonitorSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
           INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1(                                        
                CJ_ID, SLOT, MTOOL_ID, JOB_SPEC_GROUP_ID, RECIPE, RETICLE, PROCESS_ST_TIME,PROCESS_ED_TIME                                                             
           ) 
            SELECT H.CJ_ID, H.SLOT,                                                                                                                                                                   
                   H.MTOOL_ID,                                                                                      
                   H.JOB_SPEC_GROUP_ID,                                                                             
                   H.RECIPE,   
                   H.RETICLE,                                                                                       
                   H.PROCESS_ST_TIME,                                                                               
                   H.PROCESS_ED_TIME
           FROM APS_TMP_OCS_MAIN_H_VIEW.APS_TMP_OCS_MAIN_H_VIEW H                                                                                                                                      
           WHERE H.PROCESS_ED_TIME IS NOT NULL AND H.PROCESS_ED_TIME <> ''
                  AND H.PROCESS_ST_TIME IS NOT NULL AND H.PROCESS_ST_TIME <> ''                                   
           AND H.STATUS = 'OpeComplete'                                                                  
           AND CAST(H.STATUS_TIME AS TIMESTAMP) >= date_add(CURRENT_TIMESTAMP, INTERVAL '-1 day')                                                                                                              
                  """.format(current_time=current_time, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1")

    sql = """
           INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V2(                                        
                CJ_ID, MTOOL_ID, JOB_SPEC_GROUP_ID, RECIPE, RETICLE, PROCESS_ST_TIME,PROCESS_ED_TIME                                                             
           ) 
            SELECT H.CJ_ID,                                                                                                                                                                     
               MAX(H.MTOOL_ID) AS MTOOL_ID,                                                                                      
               MAX(H.JOB_SPEC_GROUP_ID) AS JOB_SPEC_GROUP_ID,                                                                             
               MAX(H.RECIPE) AS RECIPE,   
               MAX(H.RETICLE) AS RETICLE,                                                                                       
               MAX(H.PROCESS_ST_TIME) AS PROCESS_ST_TIME,                                                                               
               MAX(H.PROCESS_ED_TIME) AS PROCESS_ED_TIME 
            FROM {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1 H                                                                                                                                                                                                
            GROUP BY H.CJ_ID                                                    
          """.format(current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTORY_OCS_HIST_V2",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTORY_OCS_HIST_V2")

    sql = """
           INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V3(                                        
                CJ_ID, SLOT                                                             
           ) 
            SELECT H.CJ_ID,
               string_agg(DISTINCT H.SLOT,',' order by H.SLOT ) AS SLOT                                                                                                                                                                     
           FROM {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V1 H                                                                                                                                                                                                
            GROUP BY H.CJ_ID                                                    
          """.format(current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTORY_OCS_HIST_V3",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTORY_OCS_HIST_V3")

    sql = """
              INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V4(                                        
                   CJ_ID, SLOT, MTOOL_ID, JOB_SPEC_GROUP_ID, RECIPE, RETICLE, PROCESS_ST_TIME,PROCESS_ED_TIME                                                             
              ) 
               SELECT V2.CJ_ID, V3.SLOT,                                                                                                                                                                   
                      V2.MTOOL_ID,                                                                                      
                      V2.JOB_SPEC_GROUP_ID,                                                                             
                      V2.RECIPE,   
                      V2.RETICLE,                                                                                       
                      V2.PROCESS_ST_TIME,                                                                               
                      V2.PROCESS_ED_TIME
              FROM {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V2 V2
              INNER JOIN  {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V3 V3                                                                                                                                     
              ON V2.CJ_ID = V3.CJ_ID                                                                                                             
             """.format(current_time=current_time, tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTORY_OCS_HIST_V4",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTORY_OCS_HIST_V4")

    sql = """
            INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST(                                        
                 CJ_ID, QTY, TOOLG_ID, MTOOL_ID, JOB_SPEC_GROUP_ID, RECIPE, RETICLE, PROCESS_ST_TIME,PROCESS_ED_TIME,MODULE,SCHE_TOOLG_ID                                                             
            ) 
            SELECT H.CJ_ID,
                LENGTH(SLOT) - LENGTH(REPLACE(SLOT,',',''))+1 AS QTY,                                                                                  
                T.TOOLG_ID,                                                                                      
                H.MTOOL_ID,                                                                                      
                H.JOB_SPEC_GROUP_ID,                                                                             
                H.RECIPE,   
                H.RETICLE,                                                                                       
                H.PROCESS_ST_TIME,                                                                               
                H.PROCESS_ED_TIME,                                                                               
                T.MODULE,
                T.SCHE_TOOLG_ID  
            FROM {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST_V4 H                                                        
            INNER JOIN {tempdb}APS_TMP_ETL_LOTHISTROY_TOOLDATA T                                                             
            ON T.TOOL_ID = H.MTOOL_ID AND T.EQP_CATEGORY IN ('Internal Buffer','Process','Wafer Bonding')                                                                                                                                   
           """.format(current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_LOTHISTORY_OCS_HIST",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_LOTHISTORY_OCS_HIST")
    sql = """
              INSERT  /*+ append */  INTO APS_ETL_LOTHISTORY(                                        
             LOT_ID, TOOLG_ID, TOOL_ID, PROD_ID, PLAN_ID, STEP_ID, LOT_TYPE,PTY,SHR_FLAG, 
             LAYER, STAGE, RECIPE, PPID, QTY, RETICLE_ID, TECH_ID, CUSTOMER, ARRIVAL, JOB_PREPARE,
             TRACK_IN, PROCESS_START, PROCESS_END, TRACK_OUT, REWORK_FLAG, BACKUP_FLAG, MODULE,
             PRODG_ID, PRODG_TECH, UPDATE_TIME, PARTKEY, SCANNER_START, SCANNER_END, RECIPE_SETUP,CH_SET,SCHE_TOOLG_ID                                                               
        )                                                                                                        
         SELECT H.CJ_ID AS LOT_ID,                                                                               
                H.TOOLG_ID,                                                                            
                H.MTOOL_ID AS TOOL_ID,                                                                      
                H.JOB_SPEC_GROUP_ID AS PROD_ID,                                                             
                H.JOB_SPEC_GROUP_ID AS PLAN_ID,                                                             
                '0' AS STEP_ID,                                                                                  
                'Monitor' AS LOT_TYPE,                                                                           
                '-1' AS PTY, '-1' AS SHR_FLAG,                                                                   
                '' AS LAYER, '' AS STAGE,                                                                        
                H.RECIPE,                                                                              
                H.RECIPE AS PPID,                                                                           
                CASE WHEN CAST(H.QTY AS DECIMAL) > 25 THEN '25' ELSE H.QTY END AS QTY,                                                                                 
                H.RETICLE AS RETICLE_ID,                                                                    
                '' AS TECH_ID,                                                                                   
                '' AS CUSTOMER,                                                                                  
                H.PROCESS_ST_TIME AS ARRIVAL,                             
                H.PROCESS_ST_TIME AS JOB_PREPARE,                         
                H.PROCESS_ST_TIME AS TRACK_IN,                            
                H.PROCESS_ST_TIME AS PROCESS_START,                       
                H.PROCESS_ED_TIME AS PROCESS_END,                         
                H.PROCESS_ED_TIME AS TRACK_OUT,                           
                '' AS REWORK_FLAG,                                                                               
                '' AS BACKUP_FLAG,                                                                               
                H.MODULE,                                                                              
                '' AS PRODG_ID,                                                                                  
                '' AS PRODG_TECH,                                                                                
                '{current_time}' AS UPDATE_TIME,                                                         
                H.PROCESS_ST_TIME AS PARTKEY, 
                '' AS SCANNER_START,                                                                   
                '' AS SCANNER_END,                                                                     
                '' AS RECIPE_SETUP,'' AS CH_SET,
                H.SCHE_TOOLG_ID                                                       
         FROM {tempdb}APS_TMP_ETL_LOTHISTORY_OCS_HIST H                                                                                     
         WHERE 1=1                                                                                               
         AND NOT EXISTS (                                                                                        
                    SELECT HL.LOT_ID  FROM LAST_APS_ETL_LOTHISTORY.APS_ETL_LOTHISTORY HL                                                 
                    WHERE HL.LOT_ID = H.CJ_ID                                                                    
                    AND HL.TOOL_ID = H.MTOOL_ID AND HL.PROD_ID = H.JOB_SPEC_GROUP_ID                             
                    AND HL.PLAN_ID = H.JOB_SPEC_GROUP_ID AND HL.STEP_ID = '0'                                    
                    AND HL.TRACK_IN = H.PROCESS_ST_TIME
                   --AND CAST(HL.TRACK_IN AS TIMESTAMP)>= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '5 day'
                   )                                            
       """.format(current_time=current_time,tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_LOTHISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_LOTHISTORY")


def FirstSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT  /*+ append */  INTO APS_ETL_LOTHISTORY(                                        
             LOT_ID, TOOLG_ID, TOOL_ID, PROD_ID, PLAN_ID, STEP_ID, LOT_TYPE,PTY,SHR_FLAG, 
             LAYER, STAGE, RECIPE, PPID, QTY, RETICLE_ID, TECH_ID, CUSTOMER, ARRIVAL, JOB_PREPARE,
             TRACK_IN, PROCESS_START, PROCESS_END, TRACK_OUT, REWORK_FLAG, BACKUP_FLAG, MODULE,
             PRODG_ID, PRODG_TECH, UPDATE_TIME, PARTKEY, SCANNER_START, SCANNER_END, RECIPE_SETUP,CH_SET,
             WIP_ATTR2, BATCH_ID, SCHE_TOOLG_ID                                                                
        )                                                                                                         
          SELECT                                                                                                      
          LH.LOT_ID,                                                                                                  
          LH.EQP_G AS TOOLG_ID,                                                                                       
          LH.EQP_ID AS TOOL_ID,                                                                                       
          LH.PRODSPEC_ID AS PROD_ID,                                                                                  
          LH.MAINPD_ID AS PLAN_ID,                                                                                    
          LH.STEP_ID,                                                                                                 
          FH.LOT_TYPE AS LOT_TYPE,                                                                                    
          CASE WHEN PT.LOT_PRIORITY IS NULL THEN 500 ELSE PT.LOT_PRIORITY END AS PTY,                                                                                         
          CASE WHEN PT.SHR_FLAG IS NULL THEN -1 ELSE PT.SHR_FLAG END AS SHR_FLAG,                                                                               
          SUBSTRING(LH.OPE_NO, 1, POSITION('.' IN LH.OPE_NO)-1 ) AS LAYER,                                                                                           
          SUBSTRING(LH.OPE_NO, POSITION('.' IN LH.OPE_NO)+1 ) AS STAGE,                                                                        
          FO.LCRECIPE_ID AS RECIPE,                                                                                   
          FH.PH_RECIPE_ID AS PPID,                                                                                    
          LH.WAFER_QTY AS QTY,                                                                                        
          LH.RETICLE_ID,                                                                                              
          FH.TECH_ID AS TECH_ID,                                                                                      
          FH.CUSTOMER_ID AS CUSTOMER,                                                                                 
          CASE WHEN SA.LOT_ID IS NOT NULL and SA.LOT_ID <> '' AND CAST(SA.SPLIT_TIME AS TIMESTAMP) > CAST(LH.PREV_OP_COMP_TIME AS TIMESTAMP) AND CAST(SA.SPLIT_TIME AS TIMESTAMP) < CAST(LH.OP_START_DATE_TIME AS TIMESTAMP) THEN      
          SA.SPLIT_TIME ELSE COALESCE(LH.PREV_OP_COMP_TIME, LH.OP_START_DATE_TIME)  END AS ARRIVAL,    
          FH.RESERVE_TIME AS JOB_PREPARE,                                            
          LH.OP_START_DATE_TIME AS TRACK_IN,                              
          COALESCE(LH.PROCESS_START_TIME,LH.OP_START_DATE_TIME) AS PROCESS_START,                        
          COALESCE(FH.PROCESS_END,LH.CLAIM_TIME) AS PROCESS_END,         
          LH.CLAIM_TIME AS TRACK_OUT,                                              
          CASE WHEN SUBSTRING(LH.MAINPD_ID,1,1)='Y' THEN 'Y' ELSE 'N' END AS REWORK_FLAG,                                
          'N' AS BACKUP_FLAG,                                                                                         
          LH.REAL_MODULE AS MODULE, P.PRODG_ID, P.PRODG_TECH,                                                         
          '{current_time}' AS UPDATE_TIME,                                                                      
          LH.OP_START_DATE_TIME AS PARTKEY,        
          FH.SCANNER_START AS SCANNER_START,                                         
          FH.SCANNER_END AS SCANNER_END,                                             
          FH.RTD_GROUPNAME AS RECIPE_SETUP,'' AS CH_SET,
          P.PLAT_FORM,
          LH.BATCH_ID,
          LH.SCHE_TOOLG_ID                                                        
          FROM   {tempdb}APS_TMP_ETL_LOTHS_V1 LH                                                                                                              
          LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_PROCESSDATA FH ON LH.LOT_ID = FH.LOT_ID AND LH.CTRL_JOB = FH.CTRL_JOB         
          LEFT JOIN APS_SYNC_SHR_PRI.APS_SYNC_SHR_PRI SP ON SP.LOT_ID = LH.LOT_ID                                                                                    
          LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHS_PRODG P ON P.PRODSPEC_ID = LH.PRODSPEC_ID                              
          LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE FO ON FO.PRODSPEC_ID = LH.PRODSPEC_ID AND FO.MAINPD_ID = LH.MAINPD_ID AND FO.OPE_NO = LH.OPE_NO                
          LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_SPLITDATA SA ON LH.LOT_ID = SA.LOT_ID AND LH.OPE_NO = SA.OPE_NO
          LEFT JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_LOT_PRIORITY PT ON PT.LOT_ID = LH.LOT_ID                                                           
       """.format(current_time=current_time,tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_LOTHISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_LOTHISTORY")


def FirstMonitorSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
          INSERT  /*+ append */  INTO APS_ETL_LOTHISTORY(                                 
                LOT_ID, TOOLG_ID, TOOL_ID, PROD_ID, PLAN_ID, STEP_ID, LOT_TYPE,PTY,SHR_FLAG, 
                LAYER, STAGE, RECIPE, PPID, QTY, RETICLE_ID, TECH_ID, CUSTOMER, ARRIVAL, JOB_PREPARE,
                TRACK_IN, PROCESS_START, PROCESS_END, TRACK_OUT, REWORK_FLAG, BACKUP_FLAG, MODULE,
                PRODG_ID, PRODG_TECH, UPDATE_TIME, PARTKEY, SCANNER_START, SCANNER_END, RECIPE_SETUP,CH_SET,sche_toolg_id                                                                                      
        )      
        with LOTHISTORY_ss as (
            SELECT  H.CJ_ID AS LOT_ID,
                    (LENGTH( string_agg(DISTINCT H.SLOT,',' order by H.SLOT ) ) - 
                    LENGTH(REPLACE(string_agg(DISTINCT H.SLOT,',' order by H.SLOT),',',''))+1) AS QTY,  
                    MAX(T.TOOLG_ID) AS TOOLG_ID,                                                   
                    MAX(H.MTOOL_ID) AS TOOL_ID,                                                    
                    MAX(H.JOB_SPEC_GROUP_ID) AS PROD_ID,                                           
                    MAX(H.JOB_SPEC_GROUP_ID) AS PLAN_ID,      
                    MAX(H.RECIPE) AS RECIPE,                                                       
                    MAX(H.RECIPE) AS PPID,  
                    MAX(H.RETICLE) AS RETICLE_ID,    
                    STRFTIME(MIN(H.PROCESS_ST_TIME), '%Y-%m-%d %H:%M:%S') AS ARRIVAL,           
                    STRFTIME(MIN(H.PROCESS_ST_TIME), '%Y-%m-%d %H:%M:%S') AS JOB_PREPARE,       
                    STRFTIME(MIN(H.PROCESS_ST_TIME), '%Y-%m-%d %H:%M:%S') AS TRACK_IN,          
                    STRFTIME(MIN(H.PROCESS_ST_TIME), '%Y-%m-%d %H:%M:%S') AS PROCESS_START,     
                    STRFTIME(MAX(H.PROCESS_ED_TIME), '%Y-%m-%d %H:%M:%S') AS PROCESS_END,       
                    STRFTIME(MAX(H.PROCESS_ED_TIME), '%Y-%m-%d %H:%M:%S') AS TRACK_OUT, 
                    MAX(T.MODULE) AS MODULE,             
                    STRFTIME(MIN(H.PROCESS_ST_TIME), '%Y-%m-%d %H:%M:%S') AS PARTKEY,
                    MAX(T.SCHE_TOOLG_ID) AS SCHE_TOOLG_ID
            FROM APS_TMP_OCS_MAIN_H_VIEW.APS_TMP_OCS_MAIN_H_VIEW H                                                                
            INNER JOIN   {tempdb}APS_TMP_ETL_LOTHISTROY_TOOLDATA T                                             
            ON T.TOOL_ID = H.MTOOL_ID                                                             
            WHERE H.PROCESS_ED_TIME IS NOT NULL AND H.PROCESS_ED_TIME <> ''  
            AND H.PROCESS_ST_TIME IS NOT NULL AND H.PROCESS_ST_TIME <> ''                                                
            AND H.STATUS = 'OpeComplete'                                                          
            AND T.EQP_CATEGORY IN ('Internal Buffer','Process','Wafer Bonding')                   
            GROUP BY H.CJ_ID        
        )                                                                                  
         SELECT ss.LOT_ID,                                                             
                ss.TOOLG_ID,                                                   
                ss.TOOL_ID,                                                    
                ss.PROD_ID,                                           
                ss.PLAN_ID,                                           
                '0' AS STEP_ID,                                                                
                'Monitor' AS LOT_TYPE,                                                         
                '-1' AS PTY, '-1' AS SHR_FLAG,                                                 
                '' AS LAYER, '' AS STAGE,                                                      
                ss.RECIPE,                                                       
                ss.PPID,                                                       
                ss.QTY,                                                   
                ss.RETICLE_ID,                                                  
                '' AS TECH_ID,                                                                 
                '' AS CUSTOMER,                                                                
                ss.ARRIVAL,           
                ss.JOB_PREPARE,       
                ss.TRACK_IN,          
                ss.PROCESS_START,     
                ss.PROCESS_END,       
                ss.TRACK_OUT,         
                '' AS REWORK_FLAG,                                                             
                '' AS BACKUP_FLAG,                                                             
                ss.MODULE,                                                       
                '' AS PRODG_ID,                                                                
                '' AS PRODG_TECH,                                                              
                '{current_time}' AS UPDATE_TIME,                                         
                ss.PARTKEY,      
                '' AS SCANNER_START,                                                           
                '' AS SCANNER_END,                                                             
                '' AS RECIPE_SETUP,'' AS CH_SET,
                SS.SCHE_TOOLG_ID                                                
         FROM LOTHISTORY_ss ss                                                                                                                              
       """.format(current_time=current_time,tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_LOTHISTORY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_LOTHISTORY")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_LOTHISTORY_60M"
    current_time = my_date.date_time_second_str()
    current_time_min = my_date.date_time_min_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_LOTHISTORY"
    used_table_list = ['APS_SYNC_PRODUCT',
                       'APS_SYNC_RTD_RECIPEGROUP',
                       'APS_SYNC_PF_PO_N1',
                       'APS_SYNC_PD_RECIPE_EQP_MAIN',
                       'APS_SYNC_ETL_TOOL',
                       'APS_ETL_FLOW',
                       'APS_TMP_OCS_MAIN_H_VIEW',
                       'APS_SYNC_SHR_PRI',
                       'APS_TMP_LOTHISTORY_VIEW',
                       'APS_MID_FHOPEHS_VIEW',
                       'APS_SYNC_CSCMBMRCPRULE',
                       'APS_SYNC_NPW_RECIPE_GROUP',
                       'APS_SYNC_RTD_IMP_SOURCE',
                       'APS_SYNC_RDS_RECIPEINFO',
                       'APS_SYNC_PRODUCT_PLATFORM',
                       'APS_SYNC_LOT_PRIORITY',
                       'APS_SYNC_PRIORITY_DEFINE'
                       ]
    target_table_sql = """
        create table {}APS_ETL_LOTHISTORY
        (
            lot_id        VARCHAR(60) not null,
            toolg_id      VARCHAR(60) not null,
            tool_id       VARCHAR(60) not null,
            prod_id       VARCHAR(60) not null,
            plan_id       VARCHAR(60) not null,
            step_id       VARCHAR(60),
            lot_type      VARCHAR(30),
            pty           INTEGER not null,
            shr_flag      INTEGER not null,
            layer         VARCHAR(60),
            stage         VARCHAR(60),
            recipe        VARCHAR(60),
            ppid          VARCHAR(60),
            qty           DECIMAL not null,
            reticle_id    VARCHAR(60),
            tech_id       VARCHAR(60),
            customer      VARCHAR(60),
            arrival       TIMESTAMP  not null,
            job_prepare   TIMESTAMP,
            track_in      TIMESTAMP  not null,
            process_start TIMESTAMP  not null,
            process_end   TIMESTAMP  not null,
            track_out     TIMESTAMP  not null,
            rework_flag   VARCHAR(1),
            backup_flag   VARCHAR(2),
            update_time   TIMESTAMP not null,
            module        VARCHAR(60),
            prodg_id      VARCHAR(60),
            prodg_tech    VARCHAR(60),
            partkey       VARCHAR(60),
            scanner_start VARCHAR(64),
            scanner_end   VARCHAR(64),
            recipe_setup  VARCHAR(64),
            ch_set        VARCHAR(512),
            wip_attr2     VARCHAR(64),
            batch_id      VARCHAR(64),
            sche_toolg_id VARCHAR(64),
            PRIMARY KEY (LOT_ID, TOOL_ID, PROD_ID, PLAN_ID, TRACK_IN, PARTKEY)
        )
    """.format("")  # 注意:这里一定要这么写 [create   table 表名] => [create   table {}表名]
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
        res_dict = my_duck.attach_used_table(oracle_conn, duck_db_memory, used_table_list)
        ################################################################################################################
        # 方便看出问题，先对数据进行保存，执行完后再进行删除
        # file_full_path_hist = my_oracle.get_last_create_file(oracle_conn, 'APS_TMP_OCS_MAIN_H_VIEW')
        # target_full_path_hist = "D:\XinXiang\ErrorData\APS_TMP_OCS_MAIN_H_VIEW".format(other_ip=config.my_ip)
        # copy_cmder = "xcopy {source} {target} /Y".format(source=file_full_path_hist, target=target_full_path_hist)
        # my_cmder.exec(copy_cmder)
        ## 以下为业务逻辑
        ################################################################################################################
        # 先把V_RTD_RECIPEGROUP保存
        GetRecipeGroupSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 把V_NPW_RECIPE_GROUP 单独拿出来执行
        getNpwRecipeGroupData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 把V_RTD_IMP_SOURCE 单独拿出来执行
        getRtdImpSourceData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 把V_RDS_RECIPEINFO 单独拿出来执行
        getRdsRecipeInfoData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 先把V_PF_PO_N1保存
        GetPoN1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 先把V_PD_RECIPE_EQP_MAIN/V_PD_RECIPE_EQP_REWORK保存
        # GetRecipeEqpLoadSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 先把V_ETL_TOOL保存
        GetToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 获取Ch_Set信息
        InsertChamberRuleDataView2Temp(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        file_name = my_file.get_last_db_file(conn=oracle_conn, table_name="APS_ETL_LOTHISTORY")
        # if 取不到
        if file_name is None:
            # 插入前40天的数据
            GetViewFirstLotHistorySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            # 插入前50天的数据
            GetViewFirstFhopehSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # else 取到的情况下
        # 手动 Attach APS_ETL_LOTHISTORY AS LAST_APS_ETL_LOTHISTORY
        # 后面的Join均为 LAST_APS_ETL_LOTHISTORY.APS_ETL_LOTHISTORY
        # 将Java版本SQL反应进来
        else:

            # 插入前1小时的数据
            GetViewOtherLotHistorySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            # 插入前5天的数据
            GetViewOtherFhopehSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Recipe先获取出来
        GetFlowRecipeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        if my_duck.get_row_count_in_duckdb(duck_db_memory, "{tempdb}APS_TMP_ETL_LOTHISTROY_RECIPE1".format(
            tempdb=my_duck.get_temp_table_mark())):
            # 覆盖ETL_TempTable_Recipe的数据
            GetRealFlowRecipeSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 把最新版的Flow先存着
        GetFlowDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 找出PROCESS DATA 信息
        GetProcessDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 找出SPLIT DATA 信息
        GetSplitDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        # 三码逻辑
        GetShrFlagData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
        if file_name is None:

            FirstSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

            FirstMonitorSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        else:
            my_duck.attach_table(duck_db_memory=duck_db_memory, table_name="LAST_APS_ETL_LOTHISTORY",
                                 target_db_file=file_name)
            OtherSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

            OtherMonitorSlq(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)
            # 正常结束 需要进行删除
            # temp_process_file = target_full_path_hist +'\\'+file_full_path_hist.split('\\')[4]
            # if os.path.exists(temp_process_file):
            #     os.remove(temp_process_file)
            last_table_name = 'LAST_APS_ETL_LOTHISTORY.APS_ETL_LOTHISTORY'
            # 判断attach前一个版本的数据是否为0，如果是0，则return
            rown = my_duck.get_row_count_in_duckdb(duck_db_memory, last_table_name)
            if rown == 0:
                logging.info("LAST_APS_ETL_LOTHISTORY DATA IS NULL")
                return
            sql = """insert into APS_ETL_LOTHISTORY select * from {last_table_name} 
                     WHERE CAST(TRACK_OUT AS TIMESTAMP) >= DATE_TRUNC('second', CURRENT_TIMESTAMP)- INTERVAL '30 day' """.format(last_table_name=last_table_name)
            duck_db_memory.execute(sql)
            my_duck.detach_table(duck_db=duck_db_memory, db_name="LAST_APS_ETL_LOTHISTORY")
        # 最后在判断最终表的数据是否为0，如果是0，则return
        rown = my_duck.get_row_count_in_duckdb(duck_db_memory, target_table)
        if rown == 0:
            logging.info("APS_ETL_LOTHISTORY DATA IS NULL")
            return
        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        my_duck.detach_all_used_table(duck_db_memory, res_dict)
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            pg_conn = my_postgres.decide_postgres_db_connection(export_to_pg_prod_test=False,
                                                                pg_table_name="etl_lothistory")

            sql_key_in_pg = """
                            SELECT DISTINCT LOT_ID, TOOLG_ID, TOOL_ID, PROD_ID,PLAN_ID,TRACK_IN
                            FROM yth.etl_lothistory
                            where track_out >= timestamp '{current_time}' - INTERVAL '1 day'
                        """.format(current_time=current_time_min)
            pg_exist_result = pd.read_sql(sql_key_in_pg, pg_conn)
            print(pg_exist_result)

            select_sql_in_duck = """select  lot_id, toolg_id, tool_id, prod_id, plan_id,
                                        CASE WHEN step_id='' THEN NULL ELSE step_id END AS step_id, 
                                        CASE WHEN prodg_id='' THEN NULL ELSE prodg_id END AS prodg_id,
                                        CASE WHEN prodg_tech='' THEN NULL ELSE prodg_tech END AS prodg_tech,
                                        CASE WHEN lot_type='' THEN NULL ELSE lot_type END AS lot_type, 
                                        pty,shr_flag,
                                        CASE WHEN layer='' THEN NULL ELSE layer END AS layer,
                                        CASE WHEN stage='' THEN NULL ELSE stage END AS stage,
                                        CASE WHEN recipe='' THEN NULL ELSE recipe END AS recipe,
                                        CASE WHEN ppid='' THEN NULL ELSE ppid END AS ppid,qty,
                                        CASE WHEN reticle_id='' THEN NULL ELSE reticle_id END AS reticle_id,
                                        CASE WHEN tech_id='' THEN NULL ELSE tech_id END AS tech_id, 
                                        CASE WHEN customer='' THEN NULL ELSE customer END AS customer,
                                        arrival,job_prepare,track_in,process_start, process_end,track_out,
                                        CASE WHEN rework_flag='' THEN NULL ELSE rework_flag END AS rework_flag,
                                        CASE WHEN backup_flag='' THEN NULL ELSE backup_flag END AS backup_flag,
                                        update_time,
                                        CASE WHEN scanner_start='' THEN NULL ELSE scanner_start END AS scanner_start,
                                        CASE WHEN scanner_end='' THEN NULL ELSE scanner_end END AS scanner_end, 
                                        CASE WHEN recipe_setup='' THEN NULL ELSE recipe_setup END AS recipe_setup,
                                        CASE WHEN ch_set='' THEN NULL ELSE ch_set END AS ch_set,
                                        CASE WHEN wip_attr2='' THEN NULL ELSE wip_attr2 END AS wip_attr2,
                                        case when sche_toolg_id='' then null else sche_toolg_id end as sche_toolg_id
                                        from APS_ETL_LOTHISTORY t
                                        WHERE CAST(track_out AS TIMESTAMP) >= DATE_TRUNC('second', CAST('{current_time}' AS TIMESTAMP)) - INTERVAL '1 day'
                                AND NOT EXISTS(
                                    select 1 
                                    from pg_exist_result pd
                                    where t.LOT_ID = pd.LOT_ID
                                    and   t.TOOLG_ID = pd.TOOLG_ID
                                    and   t.TOOL_ID = pd.TOOL_ID
                                    and   t.PROD_ID = pd.PROD_ID
                                    and   t.PLAN_ID = pd.PLAN_ID
                                    and   t.TRACK_IN = pd.TRACK_IN
                                )
                            """.format(current_time=current_time_min)

            postgres_table_define = """etl_lothistory(lot_id, toolg_id, tool_id, prod_id, plan_id,step_id, prodg_id,prodg_tech,lot_type, pty,shr_flag,layer, stage,recipe,ppid,
                                                                  qty,reticle_id,tech_id, customer,arrival,job_prepare,track_in,process_start, process_end,track_out, rework_flag,
                                                                  backup_flag,update_time, scanner_start,scanner_end, recipe_setup, ch_set, wip_attr2,sche_toolg_id )
                                                 """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_lothistory",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                oracle_conn=oracle_conn,
                                                ETL_Proc_Name=ETL_Proc_Name,
                                                copy_to_yto=False)
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
        # 嘗試導出到目標文件中
        try:
            my_duck.export_result_duck_file_and_close_duck_db_memory2(duck_db_memory,
                                                                      in_process_db_file=in_process_db_file,
                                                                      target_table=target_table,
                                                                      current_time=current_time_short)
        except Exception as export_err:
            logging.warning("清理 DuckDB 資源失敗: {export_err}".format(export_err=export_err))
        # 嘗試寫警告日志
        try:
            my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                       cons_error_code.APS_ETL_LOTHISTORY_CODE_XX_ETL)
        except Exception as log_err:
            logging.warning("寫入錯誤日誌失敗: {log_err}".format(log_err=log_err))
        raise e
    finally:
        delete_over_three_version(table_name=target_table)
        oracle_conn.commit()
        oracle_conn.close()
        # if config.g_thread_and_memory_limit:  # 是否手动管理内存和进程
        #     # 删除TMP目录:LQN:2023/08/21
        #     if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
        #         os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        if os.path.exists(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid)):
            os.remove(os.path.join(config.g_mem_etl_output_path, 'duck_temp', uuid))
        if os.path.exists(temp_db_file) and not config.g_debug_mode:
            os.remove(temp_db_file)
        gc.collect()  # 内存释放


if __name__ == '__main__':
    # 单JOB测试用
    print("start")
    execute()
