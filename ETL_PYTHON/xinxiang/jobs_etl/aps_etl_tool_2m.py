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
    create {table_type} table APS_TMP_ETL_TOOL_MANUAL_LOCATION
    (
        tool_id  VARCHAR(64),
        location VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_TOOL_SOURCE
    (
        eqp_area            VARCHAR(64),
        fab_phase           VARCHAR(64),
        real_module         VARCHAR(64),
        eqp_g               VARCHAR(64),
        eqp_id              VARCHAR(64),
        eqp_chamber_flag    VARCHAR(64),
        min_unit_flag       VARCHAR(64),
        host_eqp_flag       VARCHAR(64),
        prev_e10_state_code VARCHAR(64),
        prev_state          VARCHAR(64),
        equipment_key       VARCHAR(64),
        e10chg_starttmst    VARCHAR(64),
        fab_id              VARCHAR(64),
        e10chg_endtmst      VARCHAR(64),
        host_eqp_id         VARCHAR(64),
        SCHE_EQP_G          VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_TOOL_MAIN_PART1
    (
        eqp_id    VARCHAR(64),
        ch_eqp_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_TMP_ETL_TOOL_MAIN_PART2
    (
         EQP_ID      VARCHAR(64), 
         CH_EQP_ID   VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_MAIN_PART3
     (
         EQP_ID      VARCHAR(64), 
         CH_EQP_ID   VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_MAIN_PART4
     (
         EQP_ID      VARCHAR(64), 
         CH_EQP_ID   VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_MAIN_TOOL
     (
         EQP_ID      VARCHAR(64), 
         CH_EQP_ID   VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_RUNING_LOT
     (
        EQP_ID      VARCHAR(64), 
        CH_EQP_ID   VARCHAR(64),
        LOT_ID      VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_PORT_NUM
     (
        eqp_id    VARCHAR(64),
        ch_eqp_id VARCHAR(64),
        lot_id    VARCHAR(64),
        port_num  VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_FOUP_NUM
     (
        eqp_id    VARCHAR(64),
        ch_eqp_id VARCHAR(64),
        lot_id    VARCHAR(64),
        port_num  VARCHAR(64),
        foup_num  VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_LOCATION
     (
        eqp_id    VARCHAR(64),
        ch_eqp_id VARCHAR(64),
        lot_id    VARCHAR(64),
        port_num  VARCHAR(64),
        foup_num  VARCHAR(64),
        location  VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create table APS_TMP_ETL_TOOL_CROSS
    (
      eqp_id               VARCHAR(64),
      ch_eqp_id            VARCHAR(64),
      lot_id               VARCHAR(64),
      port_num             VARCHAR(64),
      foup_num             VARCHAR(64),
      location             VARCHAR(64),
      cross_fab_flag       VARCHAR(64),
      cross_fab_hour_limit VARCHAR(64),
      std_wip              VARCHAR(64),
      std_cap              VARCHAR(64)
    )
     """
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_RUN_MODE
    (
        eqp_id               VARCHAR(64),
        ch_eqp_id            VARCHAR(64),
        lot_id               VARCHAR(64),
        port_num             VARCHAR(64),
        foup_num             VARCHAR(64),
        location             VARCHAR(64),
        cross_fab_flag       VARCHAR(64),
        cross_fab_hour_limit VARCHAR(64),
        std_wip              VARCHAR(64),
        process_mode         VARCHAR(64),
        std_cap              VARCHAR(64)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_MULTI_LOT
    (
        eqp_id VARCHAR(64)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_BOTTLENECK_EQPG
    (
         eqp_g VARCHAR(64)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_PURGE
    (
        eqp_id     VARCHAR(64),
        purge_time VARCHAR(64)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_PURGE_CU
    (
        eqp_id     VARCHAR(64),
        purge_time VARCHAR(64)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_TOOL_STRIP_CHAMBER
    (
      eqp_g         VARCHAR(64),
      strip_chamber VARCHAR(4000)
    )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_TOOL_QUEUE
        (
          EQP_ID VARCHAR(64),
          QUEUE_COUNT VARCHAR(64),
          QUEUE_MINS  VARCHAR(64)
        )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_TOOL_CAPACITY
        (
            EQP_G VARCHAR(64),
            SETTING_OPER VARCHAR(64),
            DEFAULT_VAL VARCHAR(64),
            SETTING_VALUE VARCHAR(64)
        )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_ETL_TOOL_DOWN_PM
        (
            EQP_ID VARCHAR(64),
            DOWN_TIME VARCHAR(64)
        )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_TOOL_V1
       (
            parentid         VARCHAR(60) not null,
            toolg_id         VARCHAR(60) not null,
            tool_id          VARCHAR(60) not null,
            ch_id            VARCHAR(60) not null,
            tool_status      VARCHAR(60) not null,
            tool_status_time VARCHAR(60) not null,
            ch_status        VARCHAR(60),
            ch_status_time   VARCHAR(60),
            coat             DECIMAL,
            lot_id           VARCHAR(60),
            down_back_time   DECIMAL,
            port_num         INTEGER,
            reticle_num      INTEGER,
            location         VARCHAR(20),
            update_time      VARCHAR(60) not null,
            module_id           VARCHAR(60),
            partcode         VARCHAR(60) not null,
            cross_fab_flag   VARCHAR(1),
            std_hr           VARCHAR(60),
            std_wip          VARCHAR(60),
            foup_num         VARCHAR(60),
            process_mode     VARCHAR(64),
            multi_flag       VARCHAR(1),
            entity           VARCHAR(64),
            purge_time       VARCHAR(60),
            MINOR_CH_FLAG    VARCHAR(1),
            QUEUE_BATCH_LIMIT VARCHAR(60),
            REVERSE_QUEUE_TIME DECIMAL(18,3),
            CAPACITY_COE    VARCHAR(60),
            STD_MOVE        DECIMAL,

       )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_TOOL_V2
       (
            parentid         VARCHAR(60) not null,
            toolg_id         VARCHAR(60) not null,
            tool_id          VARCHAR(60) not null,
            ch_id            VARCHAR(60) not null,
            tool_status      VARCHAR(60) not null,
            tool_status_time VARCHAR(60) not null,
            ch_status        VARCHAR(60),
            ch_status_time   VARCHAR(60),
            coat             DECIMAL,
            lot_id           VARCHAR(60),
            down_back_time   DECIMAL,
            port_num         INTEGER,
            reticle_num      INTEGER,
            location         VARCHAR(20),
            update_time      VARCHAR(60) not null,
            module_id           VARCHAR(60),
            partcode         VARCHAR(60) not null,
            cross_fab_flag   VARCHAR(1),
            std_hr           VARCHAR(60),
            std_wip          VARCHAR(60),
            foup_num         VARCHAR(60),
            process_mode     VARCHAR(64),
            multi_flag       VARCHAR(1),
            entity           VARCHAR(64),
            purge_time       VARCHAR(60),
            MINOR_CH_FLAG    VARCHAR(1),
            QUEUE_BATCH_LIMIT VARCHAR(60),
            REVERSE_QUEUE_TIME DECIMAL(18,3),
            CAPACITY_COE    VARCHAR(60),
            STD_MOVE        DECIMAL,
            REMAIN_PROCESS_TIME   VARCHAR(64), 
            ignore_ps_flag   INTEGER, 
            SCHE_TOOLG_ID    VARCHAR(64), 
            SCHE_ENTITY      VARCHAR(64)
       )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_TOOL_V3
       (
            parentid         VARCHAR(60) not null,
            toolg_id         VARCHAR(60) not null,
            tool_id          VARCHAR(60) not null,
            ch_id            VARCHAR(60) not null,
            tool_status      VARCHAR(60) not null,
            tool_status_time VARCHAR(60) not null,
            ch_status        VARCHAR(60),
            ch_status_time   VARCHAR(60),
            coat             DECIMAL,
            lot_id           VARCHAR(60),
            down_back_time   DECIMAL,
            port_num         INTEGER,
            reticle_num      INTEGER,
            location         VARCHAR(20),
            update_time      VARCHAR(60) not null,
            module_id           VARCHAR(60),
            partcode         VARCHAR(60) not null,
            cross_fab_flag   VARCHAR(1),
            std_hr           VARCHAR(60),
            std_wip          VARCHAR(60),
            foup_num         VARCHAR(60),
            process_mode     VARCHAR(64),
            multi_flag       VARCHAR(1),
            entity           VARCHAR(64),
            purge_time       VARCHAR(60),
            MINOR_CH_FLAG    VARCHAR(1),
            QUEUE_BATCH_LIMIT VARCHAR(60),
            REVERSE_QUEUE_TIME DECIMAL(18,3),
            CAPACITY_COE    VARCHAR(60),
            STD_MOVE        DECIMAL,
            REMAIN_PROCESS_TIME   VARCHAR(64), 
            ignore_ps_flag   INTEGER, 
            SCHE_TOOLG_ID    VARCHAR(64), 
            SCHE_ENTITY      VARCHAR(64),
            bn_flag          VARCHAR(64)
       )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_TOOL_LP_NUM
       (
            eqp_id         VARCHAR(64),
            port_num       VARCHAR(64)
       )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create {table_type} table APS_TMP_ETL_TOOL_FORMBATCH_TIME
      (
           eqp_id               VARCHAR(64),
           formbatch_time       VARCHAR(64)
      )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create {table_type} table APS_TMP_ETL_TOOL_PS_TUNNING
      (
           eqp_id               VARCHAR(64)
      )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create table APS_TMP_ETL_TOOL_ERROR
      (
           PARENTID               VARCHAR(64),
           TOOL_ID                VARCHAR(64),
           CH_ID                  VARCHAR(64),
           PARTCODE               VARCHAR(64)
      )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
    create {table_type} table APS_TMP_ETL_TOOL_EMPTY_PORT_GROUP
    (
        eqp_id VARCHAR(64),
        port_port_grp VARCHAR(64),
        has_available_port VARCHAR(1)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)


# 获取ToolSql信息
def getToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_V1(   
                --modify by xiecheng for version up                                                      
                PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE                                                                                          
        )                                                                                                             
         WITH STATUS_CONVERT AS (                                                                                      
              SELECT E10_STATUS,E10_CODE,MOUDLE                                                                        
              FROM  APS_SYNC_EQP_STATUS_CONVERT_AVAILABLE.APS_SYNC_EQP_STATUS_CONVERT_AVAILABLE                                                                                                             
         )                                                                                                             
         SELECT '{uuid}' AS PARENTID,                                                                    
         TL.EQP_G AS TOOLG_ID,                                                                                         
         EQ.EQP_ID AS TOOL_id,                                                                                         
         EQ.CH_EQP_ID AS CH_ID,                                                                                        
          (CASE WHEN                                                                                                     
            (SELECT COUNT(*)  FROM STATUS_CONVERT T                                                                    
                 WHERE T.MOUDLE = TL.REAL_MODULE                                                                       
                 AND T.E10_STATUS = TL.PREV_E10_STATE_CODE                                                             
                 AND T.E10_CODE = TL.PREV_STATE)>0                                                                     
         THEN 'IDLE'                                                                                                   
         ELSE                                                                                                          
         --DECODE(TL.PREV_E10_STATE_CODE,'PRD','RUN','SBY','IDLE',TL.PREV_E10_STATE_CODE) END AS TOOL_STATUS,
        (CASE WHEN TL.PREV_E10_STATE_CODE ='PRD'
              THEN 'RUN'
              WHEN TL.PREV_E10_STATE_CODE ='SBY' 
              THEN 'IDLE'
              ELSE  TL.PREV_E10_STATE_CODE END)  END)  AS TOOL_STATUS,                 
        CAST(STRFTIME(cast(TL.E10CHG_STARTTMST as TIMESTAMP) , '%Y-%m-%d %H:%M:%S') AS TIMESTAMP) AS TOOL_STATUS_TIME,                    
            ( CASE WHEN                                                                                                     
            (SELECT COUNT(*)  FROM STATUS_CONVERT T                                                                    
                 WHERE T.MOUDLE = TL2.REAL_MODULE                                                                      
                 AND T.E10_STATUS = TL2.PREV_E10_STATE_CODE                                                            
                 AND T.E10_CODE = TL2.PREV_STATE)>0                                                                    
         THEN 'IDLE'                                                                                                   
         ELSE                                                                                                          
         --DECODE(TL2.PREV_E10_STATE_CODE,'PRD','RUN','SBY','IDLE',TL2.PREV_E10_STATE_CODE) END AS TOOL_STATUS,     
         (CASE WHEN TL2.PREV_E10_STATE_CODE ='PRD'
               THEN 'RUN'
               WHEN TL2.PREV_E10_STATE_CODE ='SBY' 
               THEN 'IDLE'
               ELSE  TL2.PREV_E10_STATE_CODE END) END) AS TOOL_STATUS,               
         CAST(STRFTIME(cast(TL2.E10CHG_STARTTMST as TIMESTAMP) , '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)AS CH_STATUS_TIME,                                         
         CAST('0' AS DECIMAL) AS COAT,                                                                                                  
         EQ.LOT_ID,                                                                                                   
         --CASE WHEN NVL(CEIL((TL2.E10CHG_ENDTMST - '{current_time}')*24*60*60)/3600, 0) < 0 
         --THEN 0.1667 ELSE NVL(CEIL((TL2.E10CHG_ENDTMST - '{current_time}')*24*60*60/3600, 0) END AS DOWN_BACK_TIME, 
         -- TODO 暂时这样,后续再测试  
         CASE 
          WHEN
             COALESCE((extract(epoch from(cast( case when TL2.E10CHG_ENDTMST = '' then cast('{current_time}' as TIMESTAMP) else TL2.E10CHG_ENDTMST END as TIMESTAMP) 
            - cast('{current_time}' as TIMESTAMP) ))), 0) < 0
          THEN CAST('0.1667' AS DECIMAL) 
           ELSE 
            CAST(COALESCE((extract(epoch from(cast( case when TL2.E10CHG_ENDTMST = '' then cast('{current_time}' as TIMESTAMP) else TL2.E10CHG_ENDTMST END as TIMESTAMP) 
            - cast('{current_time}' as TIMESTAMP) ))), 0) AS DECIMAL) end AS DOWN_BACK_TIME,  
         CAST(EQ.PORT_NUM AS INTEGER),                                                                                                
         CASE WHEN G.EQP_G IS NOT NULL AND G.EQP_G <> '' THEN CAST(G.AVAILABLE_MASK_LIBRARY AS INTEGER) ELSE CAST('0' AS INTEGER) END AS RETICLE_NUM,                        
         EQ.LOCATION,                                                                                                  
         TL.REAL_MODULE AS MODULE,                                                                                     
         CAST('{current_time}' AS TIMESTAMP) AS UPDATE_TIME,                                                                        
         ''AS PARTCODE,                                                            
         EQ.CROSS_FAB_FLAG,                                                                                            
         CAST(EQ.CROSS_FAB_HOUR_LIMIT AS DECIMAL),                                                                                      
         CAST(EQ.STD_WIP AS DECIMAL),
         CAST(EQ.STD_CAP AS DECIMAL) AS STD_MOVE,                                                                                                  
         CAST(EQ.FOUP_NUM AS INTEGER)
         --modify by xiecheng for version up
         ,EQ.PROCESS_MODE, 
         CASE WHEN MTN.EQP_ID IS NOT NULL AND MTN.EQP_ID <> '' THEN 'Y' END AS MULTI_FLAG,
         TL2.EQP_G AS ENTITY,   
         COALESCE(CAST(PCTN.PURGE_TIME AS TIMESTAMP), CAST(PTN.PURGE_TIME AS TIMESTAMP)),
         CASE WHEN STN.STRIP_CHAMBER IS NOT NULL AND STN.STRIP_CHAMBER <> '' AND INSTR(STN.STRIP_CHAMBER,EQ.CH_EQP_ID) > 0 THEN '1' END AS MINOR_CH_FLAG,
         QUEUE_COUNT AS QUEUE_BATCH_LIMIT,
         QUEUE_MINS  AS REVERSE_QUEUE_TIME,
         CASE WHEN TC.EQP_G IS NULL THEN '1' ELSE COALESCE(TC.SETTING_VALUE,TC.DEFAULT_VAL,1) END AS CAPACITY_COE                                                                                                 
         FROM {tempdb}APS_TMP_ETL_TOOL_RUN_MODE EQ                                                                              
         LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE TL ON TL.EQP_ID = EQ.EQP_ID                                              
         LEFT JOIN APS_SYNC_SCHE_UMT_MODULE_EQPG.APS_SYNC_SCHE_UMT_MODULE_EQPG G ON TL.EQP_G = G.EQP_G                                                      
         LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE TL2 ON TL2.EQP_ID = EQ.CH_EQP_ID  
          --modify by xiecheng for version up
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_MULTI_LOT MTN ON  MTN.EQP_ID = EQ.EQP_ID                                                            
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_PURGE PTN ON  PTN.EQP_ID = EQ.EQP_ID 
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_PURGE_CU PCTN ON  PCTN.EQP_ID = EQ.EQP_ID       
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_STRIP_CHAMBER STN ON  STN.EQP_G = TL2.EQP_G   
                                                        AND TL2.EQP_CHAMBER_FLAG ='Chamber'
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_QUEUE  TQ ON EQ.EQP_ID = TQ.EQP_ID
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_CAPACITY TC ON TC.EQP_G = TL.EQP_G                                                             
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_V1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_V1")


def getViewToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_SOURCE (                 
            EQP_AREA, FAB_PHASE, REAL_MODULE, EQP_G, EQP_ID, EQP_CHAMBER_FLAG, MIN_UNIT_FLAG, HOST_EQP_FLAG, PREV_E10_STATE_CODE,
            PREV_STATE, EQUIPMENT_KEY, E10CHG_STARTTMST, FAB_ID, E10CHG_ENDTMST,HOST_EQP_ID,SCHE_EQP_G                                                
    )                                                                    
      SELECT T.EQP_AREA,                
            T.FAB_PHASE,                
            T.REAL_MODULE,              
            T.EQP_G,                    
            T.EQP_ID,                   
            T.EQP_CHAMBER_FLAG,         
            T.MIN_UNIT_FLAG,            
            T.HOST_EQP_FLAG,            
            T.PREV_E10_STATE_CODE,      
            T.PREV_STATE,               
            T.EQUIPMENT_KEY,            
            T.E10CHG_STARTTMST,         
            T.FAB_ID,                   
            T.E10CHG_ENDTMST,           
            T.HOST_EQP_ID,
            T.SCHE_EQP_G               
      FROM APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T                    
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_SOURCE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_SOURCE")


def getToolLocationSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MANUAL_LOCATION
        (TOOL_ID, LOCATION)                                                                                                                                                        
         SELECT T.EQP_ID,                                                                  
                MAX(M.LOCATION) AS LOCATION                                                
         FROM APS_SYNC_SETTING_MCS_LOCATION_MANUAL.APS_SYNC_SETTING_MCS_LOCATION_MANUAL M               
         INNER JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE T                                            
         ON T.HOST_EQP_FLAG ='Y'                 
         AND POSITION(REPLACE(M.TOOL_ID,'*','') IN T.EQP_ID) > 0                                                 
         GROUP BY T.EQP_ID                          
        """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MANUAL_LOCATION",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MANUAL_LOCATION")


def getMainToolPart1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MAIN_PART1(                                                           
            EQP_ID, CH_EQP_ID                                                                                                 
        )                                                                                                              
         SELECT                                                                                                   
           A.EQP_ID                                                                                           
           ,B.EQP_ID AS CH_EQP_ID                                                                             
         FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE A, {tempdb}APS_TMP_ETL_TOOL_SOURCE B                                            
         WHERE 1=1                                                                                                
               AND A.HOST_EQP_FLAG = 'Y'                                                                          
               AND A.HOST_EQP_ID = B.HOST_EQP_ID                                                              
               AND A.EQP_CHAMBER_FLAG='Chamber'                                                                   
         GROUP BY A.EQP_ID, B.EQP_ID                                                                              
         HAVING A.EQP_ID <>B.EQP_ID                              
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MAIN_PART1",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MAIN_PART1")


def getMainToolPart2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
     INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MAIN_PART2(                                                      
        EQP_ID, CH_EQP_ID                                                                                            
        )                                                                                                         
         WITH ONLY_CHAMBER_DATA AS (                                                                                   
              SELECT HOST_EQP_ID,MAX(EQP_CHAMBER_FLAG)                                                                   
              FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE                                                                      
              WHERE  EQP_CHAMBER_FLAG='Chamber'                                                                   
              GROUP BY HOST_EQP_ID                                                                              
              HAVING COUNT(*) < 2                                                                                 
        )                                                                                                         
         SELECT A.EQP_ID, A.EQP_ID AS CH_EQP_ID                                                                   
         FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE A                                                                         
         INNER JOIN  ONLY_CHAMBER_DATA B                                                                          
         ON A.HOST_EQP_ID = B.HOST_EQP_ID                                  
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MAIN_PART2",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MAIN_PART2")


def getMainToolPart4Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
       INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MAIN_PART4(                                                            
        EQP_ID, CH_EQP_ID                                                                                    
       )                                                                                                          
        WITH CHAMBER_SETTING AS (                                                            
        SELECT DISTINCT EQP_ID,                                                              
               TRIM(CHAMBER_NAME) AS CHAMBER_NAME                                            
        FROM APS_SYNC_UNIT_EQP_CHAMBER_SETTING.APS_SYNC_UNIT_EQP_CHAMBER_SETTING                                                             
        ),                                                                                   
        EQP_INFO AS (                                                                        
        SELECT T.EQP_ID FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE T                                     
        WHERE T.EQP_CHAMBER_FLAG = 'EQP'                                                     
        AND EXISTS (SELECT 1 FROM CHAMBER_SETTING S WHERE S.EQP_ID = T.EQP_ID)               
        AND EXISTS (SELECT 1 FROM APS_SYNC_UMT_WET_RECIPE_TANK_RULE.APS_SYNC_UMT_WET_RECIPE_TANK_RULE S 
        WHERE S.EQP_ID = T.EQP_ID)               
        )                                                                                    
        SELECT T.EQP_ID,                                                                     
              T.EQP_ID||'.'||B.CHAMBER_NAME AS CH_ID                                         
        FROM  EQP_INFO T                                                                     
        LEFT JOIN CHAMBER_SETTING B                                                          
        ON T.EQP_ID = B.EQP_ID                                                                                                
     """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MAIN_PART4",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MAIN_PART4")


def getMainToolPart3Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT /*+ append */
        INTO {tempdb}APS_TMP_ETL_TOOL_MAIN_PART3
        (EQP_ID, CH_EQP_ID)
        SELECT A.EQP_ID, A.EQP_ID AS CH_EQP_ID
        FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE A
        WHERE A.EQP_CHAMBER_FLAG = 'EQP'
        AND MIN_UNIT_FLAG = 'Y'
        AND NOT EXISTS (SELECT 1
            FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_PART4 F
           WHERE F.EQP_ID = A.EQP_ID)                                
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MAIN_PART3",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MAIN_PART3")


def getMainToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MAIN_TOOL(         
        EQP_ID, CH_EQP_ID                                 
        )                                                      
          SELECT EQP_ID,CH_EQP_ID                             
          FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_PART1               
          UNION                                               
          SELECT EQP_ID,CH_EQP_ID                             
          FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_PART2              
          UNION                                               
          SELECT EQP_ID,CH_EQP_ID                             
          FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_PART3              
          UNION                                               
          SELECT EQP_ID,CH_EQP_ID                             
          FROM  {tempdb}APS_TMP_ETL_TOOL_MAIN_PART4                                               
        """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MAIN_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MAIN_TOOL")


def getRuningLotDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT /*+ append */
        INTO {tempdb}APS_TMP_ETL_TOOL_RUNING_LOT
        (EQP_ID, CH_EQP_ID, LOT_ID)
        WITH LAST_RUNING_LOT AS
        (SELECT EQP_ID,LOT_ID,
        ROW_NUMBER() OVER(PARTITION BY EQP_ID ORDER BY OPE_NO_START_TIME DESC,LOT_ID) AS ROW_NUMBER
        FROM APS_SYNC_WIP.APS_SYNC_WIP
        WHERE LOT_PROC_STATE = 'Processing'
        AND EQP_ID IS NOT NULL AND EQP_ID <> '')
        SELECT T.EQP_ID, T.CH_EQP_ID, W.LOT_ID
        FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_TOOL T
        LEFT JOIN LAST_RUNING_LOT W
        ON W.ROW_NUMBER = 1
        AND W.EQP_ID = T.EQP_ID                       
    """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_RUNING_LOT",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_RUNING_LOT")


def getPortNumDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT /*+ append */
        INTO {tempdb}APS_TMP_ETL_TOOL_PORT_NUM
        (EQP_ID, CH_EQP_ID, LOT_ID, PORT_NUM)
        WITH EMPTY_CASSETTE_GROUPS AS (
            -- 找出所有有 Empty Cassette 的 PORT_PORT_GRP
            SELECT DISTINCT EQP_ID, PORT_PORT_GRP
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT 
            WHERE LOAD_PURPOSE_TYPE = 'Empty Cassette'
        ),
        PORT_NUM AS (
            SELECT COUNT(*) AS PORT_NUM, EQP_ID 
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT F
            WHERE NOT EXISTS (
                -- 排除所有与 Empty Cassette 同 PORT_PORT_GRP 的 port
                SELECT 1 FROM EMPTY_CASSETTE_GROUPS E
                WHERE E.EQP_ID = F.EQP_ID 
                AND E.PORT_PORT_GRP = F.PORT_PORT_GRP
            )
            GROUP BY EQP_ID
        )
        SELECT T.EQP_ID, T.CH_EQP_ID, T.LOT_ID, COALESCE(P.PORT_NUM, 0) AS PORT_NUM
        FROM {tempdb}APS_TMP_ETL_TOOL_RUNING_LOT T
        LEFT JOIN PORT_NUM P
        ON P.EQP_ID = T.EQP_ID               
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_PORT_NUM",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_PORT_NUM")


def createChamberSideMapping(duck_db_memory):
    sql = """
        CREATE TEMP TABLE APS_TMP_CHAMBER_SIDE_MAPPING AS
        WITH EMPTY_CASSETTE_INFO AS (
            -- 找出有 Empty Cassette 的机台和对应的 PORT_PORT_GRP，以及是否不可用
            SELECT 
                EQP_ID,
                PORT_PORT_GRP,
                MAX(CASE WHEN MCOPEMODE_ID NOT IN ('FULL-A','FULL-M') THEN 1 ELSE 0 END) AS HAS_UNAVAILABLE_EMPTY
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT 
            WHERE LOAD_PURPOSE_TYPE = 'Empty Cassette'
            GROUP BY EQP_ID, PORT_PORT_GRP
        ),
        CHAMBER_TOOLS AS (
            -- 找出chamber式机台
            SELECT DISTINCT T.EQP_ID, T.CH_EQP_ID
            FROM {tempdb}APS_TMP_ETL_TOOL_MAIN_TOOL T
            WHERE T.CH_EQP_ID LIKE T.EQP_ID || '.L%' OR T.CH_EQP_ID LIKE T.EQP_ID || '.R%'
        )
        SELECT 
            C.EQP_ID,
            C.CH_EQP_ID,
            CASE 
                WHEN C.CH_EQP_ID LIKE C.EQP_ID || '.L%' THEN 'L'
                WHEN C.CH_EQP_ID LIKE C.EQP_ID || '.R%' THEN 'R'
                ELSE 'OTHER'
            END AS CHAMBER_SIDE,
            CASE 
                -- L側：如果 PORT_PORT_GRP = 'A' 有 Empty Cassette 且不可用，則 L側 chamber 需要 NLP
                WHEN C.CH_EQP_ID LIKE C.EQP_ID || '.L%' 
                     AND EXISTS (SELECT 1 FROM EMPTY_CASSETTE_INFO E 
                                WHERE E.EQP_ID = C.EQP_ID 
                                AND E.PORT_PORT_GRP = 'A' 
                                AND E.HAS_UNAVAILABLE_EMPTY = 1)
                     THEN 1
                -- R側：如果所有 PORT_PORT_GRP != 'A' 都有 Empty Cassette 且不可用，則 R側 chamber 需要 NLP
                WHEN C.CH_EQP_ID LIKE C.EQP_ID || '.R%' 
                     AND EXISTS (SELECT 1 FROM EMPTY_CASSETTE_INFO E WHERE E.EQP_ID = C.EQP_ID AND E.PORT_PORT_GRP != 'A')
                     AND NOT EXISTS (SELECT 1 FROM EMPTY_CASSETTE_INFO E 
                                    WHERE E.EQP_ID = C.EQP_ID 
                                    AND E.PORT_PORT_GRP != 'A' 
                                    AND E.HAS_UNAVAILABLE_EMPTY = 0)
                     THEN 1
                ELSE 0
            END AS SHOULD_BE_NLP
        FROM CHAMBER_TOOLS C
        -- 只处理有 Empty Cassette 的机台
        WHERE EXISTS (SELECT 1 FROM EMPTY_CASSETTE_INFO E WHERE E.EQP_ID = C.EQP_ID)
    """.format(tempdb=my_duck.get_temp_table_mark())

    duck_db_memory.sql(sql)

def getEmptyPortGroupDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT INTO {tempdb}APS_TMP_ETL_TOOL_EMPTY_PORT_GROUP
        (EQP_ID, PORT_PORT_GRP, HAS_AVAILABLE_PORT)
        WITH EMPTY_CASSETTE_PORTS AS (
            SELECT EQP_ID, PORT_PORT_GRP,
                   -- 检查该组内是否有可用的非Empty Cassette端口
                   CASE WHEN COUNT(CASE WHEN MCOPEMODE_ID IN ('FULL-A','FULL-M') 
                                             AND COALESCE(LOAD_PURPOSE_TYPE, '') != 'Empty Cassette' 
                                        THEN 1 END) > 0 
                        THEN 'Y' 
                        ELSE 'N' 
                   END AS HAS_AVAILABLE_PORT
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT
            WHERE LOAD_PURPOSE_TYPE = 'Empty Cassette'
            GROUP BY EQP_ID, PORT_PORT_GRP
        )
        SELECT EQP_ID, PORT_PORT_GRP, HAS_AVAILABLE_PORT
        FROM EMPTY_CASSETTE_PORTS
    """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_EMPTY_PORT_GROUP",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_EMPTY_PORT_GROUP")


def getFoupNumDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_FOUP_NUM(          
         EQP_ID, CH_EQP_ID, LOT_ID, PORT_NUM, FOUP_NUM                                         
         )                                                       
          WITH FOUP_DATA AS (                                                 
              SELECT EQP_ID,                                             
                     MIN(BATCH_SIZE) AS BATCH_SIZE                       
              FROM APS_SYNC_UMT_EQP_BATCH_SIZE_SETTING.APS_SYNC_UMT_EQP_BATCH_SIZE_SETTING                      
              GROUP BY EQP_ID                                            
          ),                                                             
          FVEQP AS (                                                         
              SELECT EQP_ID,       
                  --modify by xiecheng for version up                                          
                     MAX(P.BATCH_SIZE_MAX_PROD) AS BATCH_SIZE                     
              FROM APS_SYNC_FVEQP.APS_SYNC_FVEQP P                               
              GROUP BY EQP_ID                                                
          )                                                                  
          SELECT T.EQP_ID,T.CH_EQP_ID, T.LOT_ID, T.PORT_NUM,                        
          CASE WHEN FDA.EQP_ID IS NOT NULL AND FDA.EQP_ID <> '' THEN FDA.BATCH_SIZE ELSE COALESCE(FVA.BATCH_SIZE,0) END AS FOUP_NUM                
          FROM {tempdb}APS_TMP_ETL_TOOL_PORT_NUM T                        
          LEFT JOIN FOUP_DATA FDA                                           
          ON FDA.EQP_ID = T.EQP_ID                                        
          LEFT JOIN FVEQP FVA                                           
          ON FVA.EQP_ID = T.EQP_ID                                                    
       """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_FOUP_NUM",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_FOUP_NUM")


def getLoactionDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT /*+ append */
        INTO {tempdb}APS_TMP_ETL_TOOL_LOCATION
        (EQP_ID, CH_EQP_ID, LOT_ID, PORT_NUM, FOUP_NUM, LOCATION)
        SELECT T.EQP_ID,
        T.CH_EQP_ID,
        T.LOT_ID,
        T.PORT_NUM,
        T.FOUP_NUM,
        COALESCE(FV.LOCATION, VSML.LOCATION) AS LOCATION
        FROM {tempdb}APS_TMP_ETL_TOOL_FOUP_NUM T
        LEFT JOIN APS_SYNC_MCS_SCHED_LOCATION_INFO.APS_SYNC_MCS_SCHED_LOCATION_INFO FV
        ON T.EQP_ID like replace(FV.TOOL_ID, '*', '_' )
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_MANUAL_LOCATION VSML
        ON VSML.TOOL_ID = T.EQP_ID                                                  
          """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_LOCATION",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_LOCATION")


def getCrossDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT /*+ append */
        INTO {tempdb}APS_TMP_ETL_TOOL_CROSS
        (EQP_ID,
        CH_EQP_ID,
        LOT_ID,
        PORT_NUM,
        FOUP_NUM,
        LOCATION,
        CROSS_FAB_FLAG,
        CROSS_FAB_HOUR_LIMIT,
        STD_WIP,STD_CAP)
        SELECT T.EQP_ID,
             T.CH_EQP_ID,
             T.LOT_ID,
             T.PORT_NUM,
             T.FOUP_NUM,
             T.LOCATION,
             US.CROSS_FAB_FLAG,
             US.CROSS_FAB_HOUR_LIMIT,
             US.STD_WIP,
             US.STD_CAP
        FROM {tempdb}APS_TMP_ETL_TOOL_LOCATION T
        LEFT JOIN APS_SYNC_UMT_EQP_CAP_STD.APS_SYNC_UMT_EQP_CAP_STD US
          ON US.EQP_ID = T.EQP_ID                                                 
          """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_CROSS",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_CROSS")


def getChamberRunDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
             INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_RUN_MODE(        
                   EQP_ID, CH_EQP_ID, LOT_ID, PORT_NUM, FOUP_NUM, LOCATION, CROSS_FAB_FLAG,
                   CROSS_FAB_HOUR_LIMIT, STD_WIP, STD_CAP, PROCESS_MODE                                       
       )                                                     
        WITH CASE_1 AS (                                                                                          
            SELECT S.MODULE,                                                                                      
                   S.EQP_ID,                                                                                      
                   CASE WHEN S.EQP_MODE = 'PARALLEL MODE' THEN 'parallel_mode' ELSE 'lot_mode' END AS EQP_MODE    
            FROM APS_SYNC_EQP_MODE_SETTING.APS_SYNC_EQP_MODE_SETTING S                                              
            WHERE S.EQP_MODE IN ('PARALLEL MODE','SERIAL MODE')                                                   
            AND position('*' in EQP_ID) = 0                                    
        ),                                                                                                        
        CASE_2 AS (                                                                                               
            SELECT S.MODULE,                                                                                      
                   REPLACE(S.FAB_EQP_G,'*','%') AS EQP_G,                                                             
                   REPLACE(S.EQP_ID,'*','%') AS EQP_ID,                                                           
                   CASE WHEN S.EQP_MODE = 'PARALLEL MODE' THEN 'parallel_mode' ELSE 'lot_mode' END AS EQP_MODE    
            FROM APS_SYNC_EQP_MODE_SETTING.APS_SYNC_EQP_MODE_SETTING S                                              
            WHERE S.EQP_MODE IN ('PARALLEL MODE','SERIAL MODE')                                                   
            AND position('*' in EQP_ID) > 1                                     
        ),                                                                                                        
        CASE_3 AS (                                                                                               
            SELECT S.MODULE,                                                                                      
                   REPLACE(S.FAB_EQP_G,'*','%') AS EQP_G,                                                             
                   CASE WHEN S.EQP_MODE = 'PARALLEL MODE' THEN 'parallel_mode' ELSE 'lot_mode' END AS EQP_MODE    
            FROM APS_SYNC_EQP_MODE_SETTING.APS_SYNC_EQP_MODE_SETTING S                                              
            WHERE S.EQP_MODE IN ('PARALLEL MODE','SERIAL MODE')                                                   
             AND position('*' in EQP_ID) = 1                                   
        )                                                                                                         
        SELECT DISTINCT                                                                                           
             R.EQP_ID, R.CH_EQP_ID, R.LOT_ID,                                                                     
             R.PORT_NUM, R.FOUP_NUM, R.LOCATION,                                                                  
             R.CROSS_FAB_FLAG,R.CROSS_FAB_HOUR_LIMIT, 
             R.STD_WIP, R.STD_CAP,                                                 
             CASE WHEN TL.EQP_CHAMBER_FLAG ='EQP' THEN ''                                                         
                  WHEN C1.EQP_ID IS NOT NULL　AND　C1.EQP_ID　<>　'' THEN C1.EQP_MODE                                                     
                  WHEN C2.EQP_ID IS NOT NULL AND　C2.EQP_ID <> ''　THEN C2.EQP_MODE                                                     
                  WHEN C3.EQP_G IS NOT NULL AND C3.EQP_G <> '' THEN C3.EQP_MODE                                                      
             END AS PROCESS_MODE                                                                                  
        FROM {tempdb}APS_TMP_ETL_TOOL_CROSS R                                                                         
        INNER JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE TL ON  TL.EQP_ID = R.EQP_ID                                          
        LEFT JOIN CASE_1 C1                                                                                       
        ON C1.EQP_ID = R.EQP_ID                                                                                   
        LEFT JOIN CASE_2 C2                                                                                       
        ON R.EQP_ID LIKE C2.EQP_ID                                                                                
        AND TL.SCHE_EQP_G LIKE C2.EQP_G                                                                                
        LEFT JOIN CASE_3 C3                                                                                       
        ON TL.SCHE_EQP_G LIKE C3.EQP_G                                                    
              """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_RUN_MODE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_RUN_MODE")


def getMultiLotDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
              INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_MULTI_LOT(       
          EQP_ID                                    
         )                                                     
          SELECT DISTINCT T.EQP_ID                                    
          FROM APS_SYNC_MLIF_EQP_SETTING.APS_SYNC_MLIF_EQP_SETTING S      
          INNER JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE T                
          ON (case when S.EQP_G = '*' then T.EQP_G else S.EQP_G end) = T.EQP_G     
          AND position(REPLACE(S.EQP_ID,'*','') in T.EQP_ID)=1        
          AND T.HOST_EQP_FLAG = 'Y'                                                        
              """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_MULTI_LOT",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_MULTI_LOT")


def getBottleneckEqpgDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_BOTTLENECK_EQPG(        
            EQP_G                                      
       )                                                      
        SELECT DISTINCT S.EQP_G                      
        FROM APS_SYNC_UMT_BOTTLENECK_EQPG_SETTING.APS_SYNC_UMT_BOTTLENECK_EQPG_SETTING S         
        WHERE CURRENT_TIMESTAMP >= COALESCE(CAST(S.START_DAY as TIMESTAMP),CAST(CURRENT_TIMESTAMP as TIMESTAMP))    
        AND  CURRENT_TIMESTAMP <= COALESCE(cast(S.END_DAY as TIMESTAMP),CAST(CURRENT_TIMESTAMP as TIMESTAMP))                                                     
                 """.format(tempdb=my_duck.get_temp_table_mark())
    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_BOTTLENECK_EQPG",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_BOTTLENECK_EQPG")


def getPurgeDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_PURGE(         
         EQP_ID, PURGE_TIME                              
         )                                                       
          WITH SOURCE_DATA AS (                                                                                  
              SELECT T.EQP_G,C.EQP_ID,                                                                           
                   --ROUND(CEIL((SYSDATE - TO_DATE(TO_CHAR(C.EVENT_TIME_ST,'YYYY-MM-DD HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS')  
                    -- )*24*60*60)/3600,3) AS OVER_TIME,  
                     -- ROUND((extract(epoch from( CURRENT_TIMESTAMP  - CAST(STRFTIME(cast(C.EVENT_TIME_ST as TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP) ))) / 3600.0, 3) AS OVER_TIME,      
                     round((extract(epoch from(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - (CAST(C.EVENT_TIME_ST AS TIMESTAMP))))) / 3600.0, 3) AS OVER_TIME,                                                                                                                                         
                   --TO_CHAR(C.EVENT_TIME_ST,'YYYY-MM-DD HH24:MI:SS') AS EVENT_TIME_ST 
                   STRFTIME(cast(C.EVENT_TIME_ST as TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS EVENT_TIME_ST                            
              FROM APS_SYNC_OUT_DF_PURGE_COMBINE.APS_SYNC_OUT_DF_PURGE_COMBINE C                                       
              INNER JOIN APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T                                             
              ON T.EQP_ID = C.EQP_ID                                                                             
              WHERE EVENT_TIME_END IS NULL                                                                                                                                  
          ),                                                                                                     
          GROUP_DATA AS (                                                                                        
              SELECT EQP_G,EQP_ID,MAX(EVENT_TIME_ST) AS EVENT_TIME_ST,MIN(OVER_TIME) AS OVER_TIME                
              FROM SOURCE_DATA GROUP BY EQP_G,EQP_ID                                                             
          )                                                                                                      
          SELECT D.EQP_ID,                                                                                       
                 CASE WHEN P11.TOOL_ID IS NOT NULL AND P11.TOOL_ID <> '' AND P11.PT*2 > D.OVER_TIME THEN D.EVENT_TIME_ST               
                      WHEN P12.TOOLG_ID IS NOT NULL AND P12.TOOLG_ID <> '' AND P12.PT*2 > D.OVER_TIME THEN D.EVENT_TIME_ST              
                 END AS EVENT_TIME_ST                                                                            
          FROM GROUP_DATA D                                                                                     
          LEFT JOIN APS_MID_PURGE11.APS_MID_PURGE11 P11                                                 
          ON P11.TOOL_ID = D.EQP_ID                                 
          LEFT JOIN APS_MID_PURGE12.APS_MID_PURGE12 P12                                                 
          ON P12.TOOLG_ID = D.EQP_G                                                               
                 """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_PURGE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_PURGE")


def getCuPurgeDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_PURGE_CU(         
         EQP_ID, PURGE_TIME                              
         ) 
         WITH SOURCE_DATA AS (
              SELECT P.TOOLID,P.STARTTIME,
                     ROW_NUMBER() OVER(PARTITION BY P.TOOLID ORDER BY P.STARTTIME DESC) AS RNT
              FROM APS_SYNC_NPW_PROCESSACTION.APS_SYNC_NPW_PROCESSACTION P
              INNER JOIN APS_SYNC_ETL_TOOL.APS_SYNC_ETL_TOOL T  
              ON T.EQP_ID = P.TOOLID AND T.HOST_EQP_FLAG='Y'
              AND T.EQP_G='MA_AL(Cu)'
         )  
         SELECT TOOLID, STARTTIME
         FROM SOURCE_DATA
         WHERE RNT = 1                                                                                                               
                 """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_PURGE_CU",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_PURGE_CU")


def getStripChamberDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_STRIP_CHAMBER(          
         EQP_G,STRIP_CHAMBER                                         
         )                                                        
          SELECT DISTINCT EQP_G, 
               (
               SELECT  STRING_AGG(DISTINCT STRIP_CHAMBER,',' ORDER BY STRIP_CHAMBER) 
               FROM APS_SYNC_STRIP_CHAMBER.APS_SYNC_STRIP_CHAMBER D
               WHERE D.EQP_G = C.EQP_G
               ) AS STRIP_CHAMBER
          FROM APS_SYNC_STRIP_CHAMBER.APS_SYNC_STRIP_CHAMBER C                                                            
                   """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_STRIP_CHAMBER",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_STRIP_CHAMBER")


def getQueueSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_QUEUE(          
           EQP_ID,QUEUE_COUNT,QUEUE_MINS                                         
        )                                                        
        WITH QUEUE_DATA AS (
            SELECT EQP_ID,
                   QUEUE_COUNT,
                   QUEUE_MINS,
                   ROW_NUMBER() OVER(PARTITION BY EQP_ID ORDER BY QUEUE_COUNT DESC) AS RTN
            FROM APS_SYNC_UMT_EQP_QUEUE_SETTING.APS_SYNC_UMT_EQP_QUEUE_SETTING
        )
        SELECT EQP_ID,
               QUEUE_COUNT,
               CAST(CAST(QUEUE_MINS AS DECIMAL)/60 AS DECIMAL(18,3)) AS QUEUE_MINS
        FROM QUEUE_DATA 
        WHERE RTN = 1                                                            
                   """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_QUEUE",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_QUEUE")


def getCapacitySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_CAPACITY(          
           EQP_G,SETTING_OPER,DEFAULT_VAL,SETTING_VALUE                                         
        )                                                        
        WITH CAPACITY_DATA AS (
            SELECT EQP_G,SETTING_OPER,
                   DEFAULT_VAL,SETTING_VALUE,
                   ROW_NUMBER() OVER(PARTITION BY EQP_G,SETTING_OPER ORDER BY UPDATE_TIME DESC) AS RNT
            FROM APS_SYNC_RTD_CAPACITY_SETTING.APS_SYNC_RTD_CAPACITY_SETTING
            WHERE SETTING_OPER = '3' OR SETTING_OPER IS NULL OR SETTING_OPER = ''
        )
        SELECT EQP_G,SETTING_OPER,
               DEFAULT_VAL,
               SETTING_VALUE
        FROM CAPACITY_DATA
        WHERE RNT = 1                                         
       """.format(tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_CAPACITY",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_CAPACITY")


def getDownBackTimePMSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """      
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_DOWN_PM(          
           EQP_ID,DOWN_TIME                                         
        )                                                        
        WITH SOURCE_DATA AS (
            SELECT  EQP_ID,END_TIME,
                    ROW_NUMBER() OVER(PARTITION BY EQP_ID ORDER BY END_TIME ASC) AS RNT
            FROM APS_SYNC_EQP_STATUS_BOOKING_SETTING.APS_SYNC_EQP_STATUS_BOOKING_SETTING
            WHERE  CAST(END_TIME AS TIMESTAMP) > CURRENT_TIMESTAMP
            AND CAST(START_TIME AS TIMESTAMP) <= CURRENT_TIMESTAMP
        )
        SELECT EQP_ID,
               ROUND((EXTRACT(EPOCH FROM(CAST(END_TIME AS TIMESTAMP) 
                - CAST('{current_time}' AS TIMESTAMP))))/3600.0, 2) AS DOWN_TIME
        FROM SOURCE_DATA
        WHERE RNT = 1                                      
       """.format(tempdb=my_duck.get_temp_table_mark(), current_time=current_time, )

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_DOWN_PM",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_DOWN_PM")


def getLPPortNum(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_LP_NUM(                                                       
                EQP_ID, PORT_NUM                                                                                          
        )                                                                                                             
        WITH CHAMBER_TOOL AS (
            SELECT DISTINCT EQP_ID
            FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE T
            WHERE 1=1
            AND HOST_EQP_FLAG = 'Y'
            AND NOT EXISTS
            (
             SELECT 1
             FROM APS_SYNC_EQP_PORT_NOCONCERN.APS_SYNC_EQP_PORT_NOCONCERN N
             WHERE INSTR(T.EQP_ID,REPLACE(N.EQP_TYPE,'*','')) = 1
            )
        ),
        -- 找出所有有 Empty Cassette 的 PORT_PORT_GRP
        EMPTY_CASSETTE_GROUPS AS (
            SELECT DISTINCT EQP_ID, PORT_PORT_GRP
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT 
            WHERE LOAD_PURPOSE_TYPE = 'Empty Cassette'
        ),
        -- 计算可用的 LP port 数量
        LP_PROT_NUMBER AS (
            SELECT COUNT(*) AS PORT_NUM, EQP_ID 
            FROM APS_SYNC_FVPORT.APS_SYNC_FVPORT F
            WHERE MCOPEMODE_ID IN ('FULL-A','FULL-M')
            -- 排除所有与 Empty Cassette 同 PORT_PORT_GRP 的 port
            AND NOT EXISTS (
                SELECT 1 FROM EMPTY_CASSETTE_GROUPS E
                WHERE E.EQP_ID = F.EQP_ID 
                AND E.PORT_PORT_GRP = F.PORT_PORT_GRP
            )
            GROUP BY EQP_ID
        )
        SELECT T.EQP_ID, COALESCE(N.PORT_NUM,0) AS PORT_NUM
        FROM CHAMBER_TOOL T
        LEFT JOIN LP_PROT_NUMBER N
        ON T.EQP_ID = N.EQP_ID                                                       
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_LP_NUM",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_LP_NUM")


def getFromBatchTime(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_FORMBATCH_TIME(                                                       
                EQP_ID, FORMBATCH_TIME                                                                                          
        )                                                                                                             
        WITH TOOL_EQPG AS (
              SELECT DISTINCT EQP_ID,SCHE_EQP_G
              FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE T
              WHERE 1=1
              AND HOST_EQP_FLAG = 'Y'
        ),
        FORMBATCH_TIME_DATA AS (
              SELECT  T.EQP_G,T.FORMBATCH_TIME,
                      UNNEST(STR_SPLIT(REPLACE(T.EQP_ID,'*',''), ',')) AS EQP_ID 
              FROM APS_SYNC_DIFF_FORMBATCH_TIME.APS_SYNC_DIFF_FORMBATCH_TIME T
        )
        SELECT E.EQP_ID,MAX(T.FORMBATCH_TIME) AS FORMBATCH_TIME
        FROM TOOL_EQPG E
        INNER JOIN FORMBATCH_TIME_DATA T
        ON T.EQP_G = E.SCHE_EQP_G
        AND INSTR(E.EQP_ID, T.EQP_ID)=1  
        GROUP BY E.EQP_ID                                                     
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_FORMBATCH_TIME",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_FORMBATCH_TIME")


def getPSTunningData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
        INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_PS_TUNNING(                                                       
                EQP_ID                                                                                         
        )                                                                                                             
        WITH TOOL_EQPG AS (
              SELECT DISTINCT EQP_ID,SCHE_EQP_G
              FROM {tempdb}APS_TMP_ETL_TOOL_SOURCE T
              WHERE 1=1
              AND HOST_EQP_FLAG = 'Y'
        ),
        FORMBATCH_TIME_DATA AS (
              SELECT  T.EQP_G,
                      REPLACE(T.EQP_ID,'*','%') AS EQP_ID 
              FROM APS_SYNC_UMT_PS_TUNNING_EQP_SETTING.APS_SYNC_UMT_PS_TUNNING_EQP_SETTING T
        )
        SELECT DISTINCT T.EQP_ID
        FROM FORMBATCH_TIME_DATA E
        INNER JOIN TOOL_EQPG T
        ON E.EQP_G = T.SCHE_EQP_G
        AND T.EQP_ID LIKE E.EQP_ID                                                       
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_PS_TUNNING",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_PS_TUNNING")


def getToolV2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # 先创建 chamber 侧别映射表
    createChamberSideMapping(duck_db_memory)

    sql = """
         INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_V2(                                                       
                PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY                                                                                         
        )                                                                                                             
        SELECT  PARENTID, TOOLG_ID, TOOL_ID, CH_ID, 
                -- 修改 TOOL_STATUS 的 NLP 逻辑
                CASE 
                    WHEN N.PORT_NUM = 0 AND TOOL_STATUS IN ('RUN','IDLE') THEN 'NLP' 
                    ELSE TOOL_STATUS 
                END AS TOOL_STATUS, 
                TOOL_STATUS_TIME, 
                -- 修改 CH_STATUS 的 NLP 逻辑，增加 chamber 侧别判断
                CASE 
                    WHEN N.PORT_NUM = 0 AND CH_STATUS IN ('RUN','IDLE') THEN 'NLP'
                    WHEN COALESCE(C.SHOULD_BE_NLP, 0) = 1 AND CH_STATUS IN ('RUN','IDLE') THEN 'NLP'
                    ELSE CH_STATUS 
                END AS CH_STATUS, 
                CH_STATUS_TIME,COAT,
                LOT_ID, 
                CASE WHEN P.EQP_ID IS NOT NULL AND CAST(P.DOWN_TIME AS DECIMAL) > COALESCE(CAST(V.DOWN_BACK_TIME AS DECIMAL),0)
                    THEN CAST(P.DOWN_TIME AS DECIMAL) ELSE CAST(V.DOWN_BACK_TIME AS DECIMAL) END AS DOWN_BACK_TIME, 
                COALESCE(N.PORT_NUM, V.PORT_NUM) AS PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,
                COALESCE(T.FORMBATCH_TIME,0) AS REMAIN_PROCESS_TIME,
                CASE WHEN T2.EQP_ID IS NOT NULL THEN 1 ELSE 0 END AS ignore_ps_flag,
                S1.SCHE_EQP_G AS SCHE_TOOLG_ID,
                S2.SCHE_EQP_G AS SCHE_ENTITY
        FROM {tempdb}APS_TMP_ETL_TOOL_V1 V
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_DOWN_PM P
        ON P.EQP_ID = V.TOOL_ID AND V.TOOL_STATUS NOT IN ('RUN','IDLE')   
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_LP_NUM N
        ON N.EQP_ID = V.TOOL_ID 
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_FORMBATCH_TIME T
        ON T.EQP_ID = V.TOOL_ID    
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_PS_TUNNING T2
        ON T2.EQP_ID = V.TOOL_ID 
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE S1
        ON S1.EQP_ID = V.TOOL_ID 
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_SOURCE S2
        ON S2.EQP_ID = V.CH_ID
        -- 新增：chamber 侧别映射
        LEFT JOIN APS_TMP_CHAMBER_SIDE_MAPPING C
        ON C.EQP_ID = V.TOOL_ID AND C.CH_EQP_ID = V.CH_ID
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_V2",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_V2")


def getAllToolV3DataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
         INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_V3(                                                       
                PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY,BN_FLAG                                                                                         
        )                                                                                                             
        SELECT  PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY,
                CASE WHEN BETN.EQP_G IS NOT NULL AND BETN.EQP_G <> '' THEN 'Y' END AS BN_FLAG
        FROM {tempdb}APS_TMP_ETL_TOOL_V2 V
        LEFT JOIN {tempdb}APS_TMP_ETL_TOOL_BOTTLENECK_EQPG BETN ON  BETN.EQP_G = V.SCHE_ENTITY                                             
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_V3",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_V3")

    # 把主键冲突报出来
    sql = """
         INSERT  /*+ append */  INTO {tempdb}APS_TMP_ETL_TOOL_ERROR(                                                       
                PARENTID, TOOL_ID, CH_ID, PARTCODE                                                                                       
        )
        with source_data as (                                                                                                             
            SELECT  PARENTID, TOOL_ID, CH_ID, PARTCODE,
                    row_number() over(partition by PARENTID, TOOL_ID, CH_ID, PARTCODE order by TOOL_STATUS_TIME desc) as rnt
            FROM {tempdb}APS_TMP_ETL_TOOL_V3 V
        )
        SELECT  PARENTID, TOOL_ID, CH_ID, PARTCODE    
        from source_data
        where rnt = 2                                       
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_TMP_ETL_TOOL_ERROR",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_TMP_ETL_TOOL_ERROR")


def getResultToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    # 防止主键冲突
    sql = """
         INSERT  /*+ append */  INTO APS_ETL_TOOL(                                                       
                PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY,BN_FLAG                                                                                         
        )
        with source_data as (                                                                                                             
            SELECT  PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                    LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                    CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                    QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY,
                    BN_FLAG,
                    row_number() over(partition by PARENTID, TOOL_ID, CH_ID, PARTCODE order by TOOL_STATUS_TIME desc) as rnt
            FROM {tempdb}APS_TMP_ETL_TOOL_V3 V
        )
        SELECT  PARENTID, TOOLG_ID, TOOL_ID, CH_ID, TOOL_STATUS, TOOL_STATUS_TIME, CH_STATUS,CH_STATUS_TIME,COAT,
                LOT_ID, DOWN_BACK_TIME, PORT_NUM, RETICLE_NUM, LOCATION, MODULE_ID, UPDATE_TIME, PARTCODE,
                CROSS_FAB_FLAG, STD_HR, STD_WIP, STD_MOVE, FOUP_NUM, PROCESS_MODE, MULTI_FLAG, ENTITY, PURGE_TIME, MINOR_CH_FLAG,
                QUEUE_BATCH_LIMIT,REVERSE_QUEUE_TIME,CAPACITY_COE,REMAIN_PROCESS_TIME,ignore_ps_flag,SCHE_TOOLG_ID,SCHE_ENTITY,
                BN_FLAG    
        from source_data
        where rnt = 1                                       
    """.format(uuid=uuid, current_time=current_time, tempdb=my_duck.get_temp_table_mark())

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_ETL_TOOL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_ETL_TOOL")


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
    ETL_Proc_Name = "APS_ETL_BR.APS_ETL_TOOL_2M"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_ETL_TOOL"
    used_table_list = ['APS_SYNC_EQP_STATUS_CONVERT_AVAILABLE',
                       'APS_SYNC_SCHE_UMT_MODULE_EQPG',
                       'APS_SYNC_SETTING_MCS_LOCATION_MANUAL',
                       'APS_SYNC_ETL_TOOL',
                       'APS_SYNC_WIP',
                       'APS_SYNC_FVPORT',
                       'APS_SYNC_UMT_EQP_BATCH_SIZE_SETTING',
                       'APS_SYNC_FVEQP',
                       'APS_SYNC_MCS_SCHED_LOCATION_INFO',
                       'APS_SYNC_UMT_EQP_CAP_STD',
                       'APS_SYNC_UNIT_EQP_CHAMBER_SETTING',
                       'APS_SYNC_UMT_WET_RECIPE_TANK_RULE',
                       'APS_SYNC_MLIF_EQP_SETTING',
                       'APS_SYNC_UMT_BOTTLENECK_EQPG_SETTING',
                       'APS_MID_PURGE11',
                       'APS_MID_PURGE12',
                       'APS_SYNC_EQP_MODE_SETTING',
                       'APS_SYNC_OUT_DF_PURGE_COMBINE',
                       'APS_SYNC_STRIP_CHAMBER',
                       'APS_SYNC_UMT_EQP_QUEUE_SETTING',
                       'APS_SYNC_RTD_CAPACITY_SETTING',
                       'APS_SYNC_NPW_PROCESSACTION',
                       'APS_SYNC_EQP_STATUS_BOOKING_SETTING',
                       'APS_SYNC_EQP_PORT_NOCONCERN',
                       'APS_SYNC_DIFF_FORMBATCH_TIME',
                       'APS_SYNC_UMT_PS_TUNNING_EQP_SETTING'
                       ]
    target_table_sql = """
        create  table {}APS_ETL_TOOL
        (
            parentid         VARCHAR(60) not null,
            toolg_id         VARCHAR(60) not null,
            tool_id          VARCHAR(60) not null,
            ch_id            VARCHAR(60) not null,
            tool_status      VARCHAR(60) not null,
            tool_status_time VARCHAR(60) not null,
            ch_status        VARCHAR(60),
            ch_status_time   VARCHAR(60),
            coat             DECIMAL,
            lot_id           VARCHAR(60),
            down_back_time   DECIMAL,
            port_num         INTEGER,
            reticle_num      INTEGER,
            location         VARCHAR(20),
            update_time      VARCHAR(60) not null,
            module_id           VARCHAR(60),
            partcode         VARCHAR(60) not null,
            cross_fab_flag   VARCHAR(1),
            std_hr           VARCHAR(60),
            std_wip          VARCHAR(60),
            foup_num         VARCHAR(60),
            process_mode     VARCHAR(64),
            multi_flag       VARCHAR(1),
            entity           VARCHAR(64),
            bn_flag          VARCHAR(1),
            purge_time       VARCHAR(60),
            MINOR_CH_FLAG    VARCHAR(1),
            QUEUE_BATCH_LIMIT VARCHAR(60),
            REVERSE_QUEUE_TIME DECIMAL(18,3),
            CAPACITY_COE    VARCHAR(60),
            STD_MOVE        DECIMAL,
            ignore_ps_flag  INTEGER,
            REMAIN_PROCESS_TIME VARCHAR(60),
            sche_toolg_id  varchar(64),
            sche_entity    varchar(64),
            PRIMARY KEY (PARENTID, TOOL_ID, CH_ID, PARTCODE)
        )
    """.format("")  # 注意:这里一定要这么写 [create {table_type} table 表名] => [create {table_type} table {}表名]
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

        # 先把Tool单独拿出来，因为下面会有很多次会使用到
        getViewToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 先获取V_SETTING_MCS_LOCATION_MANUAL数据
        getToolLocationSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取主机台和子机台的关系表
        getMainToolPart1Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取主机台和子机台的关系表
        getMainToolPart2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取主机台和子机台的关系表(虚拟CH_ID逻辑)
        getMainToolPart4Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取主机台和子机台的关系表，需要过滤掉虚拟CH_ID对应的设备信息
        getMainToolPart3Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取主机台和子机台的关系表结合起来
        getMainToolDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取正在Runing的Lot的信息
        getRuningLotDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取正在PORT NUM的信息 (修改后的逻辑)
        getPortNumDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取正在FOUP NUM的信息
        getFoupNumDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取Loaction资讯
        getLoactionDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 串联获取可跨厂的标准水位信息的信息
        getCrossDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Chamber机群的run货模式
        getChamberRunDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Chamber机群的run货模式
        getMultiLotDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 获取瓶颈机群的数据
        getBottleneckEqpgDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 获取Purge时间
        getPurgeDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # MA_AL(Cu)预计dummy完成时间
        getCuPurgeDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        getStripChamberDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Queue管逻辑
        getQueueSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # capacity针对cycle_time的参数修改逻辑
        getCapacitySql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # Down_back_time 考量PM
        getDownBackTimePMSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        row_count = my_duck.get_row_count_in_duckdb(duck_db=duck_db_memory,
                                                    tableName="{tempdb}APS_TMP_ETL_TOOL_CROSS".format(
                                                        tempdb=my_duck.get_temp_table_mark()))
        if row_count == 0:
            my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time,
                                       "APS_TMP_ETL_TOOL_CROSS没有数据，请及时确认一下！！")
            return
        # 获取ToolSql信息
        getToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 新增：获取Empty Cassette端口组信息
        getEmptyPortGroupDataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # ChamberType机台需考量机台的可用LP数量 ----改成所有机台 (修改后的逻辑)
        getLPPortNum(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        getFromBatchTime(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 由于某些机台报process_start是假的，uo需要加以处理
        getPSTunningData(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 修改后的V2逻辑，包含chamber状态判断
        getToolV2Sql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        getAllToolV3DataSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        # 把主键冲突报出来
        row_count = my_duck.get_row_count_in_duckdb(duck_db=duck_db_memory,
                                                    tableName="{tempdb}APS_TMP_ETL_TOOL_ERROR".format(
                                                        tempdb=my_duck.get_temp_table_mark()))
        if row_count != 0:
            my_oracle.EndCleanUpAndLog(oracle_conn, ETL_Proc_Name, current_time,
                                       "APS_TMP_ETL_TOOL_ERROR存在主键冲突，请及时确认一下！！(暂不影响ETL_TOOL数据产出)")

        getResultToolSql(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

        ################################################################################################################
        ## 写入log
        ################################################################################################################
        # 写版本号
        my_oracle.Monitor_HandlingVerControl(oracle_conn, uuid, target_table, target_db_file, current_time_short)

        ################################################################################################################
        ## 以上为业务逻辑
        ################################################################################################################
        if config.g_copy_to_pg and my_runner.judge_main_server(oracle_conn):
            select_sql_in_duck = """select  CASE WHEN parentid='' THEN NULL ELSE parentid END AS parentid,
                                            CASE WHEN toolg_id='' THEN NULL ELSE toolg_id END AS toolg_id,
                                            CASE WHEN tool_id='' THEN NULL ELSE tool_id END AS tool_id,
                                            CASE WHEN ch_id='' THEN NULL ELSE ch_id END AS ch_id,
                                            CASE WHEN tool_status='' THEN NULL ELSE tool_status END AS tool_status,
                                            tool_status_time,
                                            CASE WHEN ch_status='' THEN NULL ELSE ch_status END AS ch_status,
                                            ch_status_time,
                                            coat,
                                            CASE WHEN lot_id='' THEN NULL ELSE lot_id END AS lot_id,
                                            down_back_time,
                                            port_num,
                                            reticle_num,
                                            CASE WHEN location='' THEN NULL ELSE location END AS location,
                                            update_time,
                                            cross_fab_flag,
                                            std_hr,
                                            std_wip,
                                            foup_num,
                                            CASE WHEN process_mode='' THEN NULL ELSE process_mode END AS process_mode,
                                            CASE WHEN multi_flag='' THEN NULL ELSE multi_flag END AS multi_flag,
                                            CASE WHEN entity='' THEN NULL ELSE entity END AS entity,
                                            CASE WHEN bn_flag='' THEN NULL ELSE bn_flag END AS bn_flag,
                                            purge_time,
                                            CASE WHEN minor_ch_flag='' THEN NULL ELSE minor_ch_flag END AS minor_ch_flag,
                                            CASE WHEN module_id='' THEN NULL ELSE module_id END AS module_id,
                                            queue_batch_limit,
                                            reverse_queue_time,
                                            CASE WHEN CAPACITY_COE='' THEN NULL ELSE CAPACITY_COE END AS CAPACITY_COE,
                                            STD_MOVE,
                                            REMAIN_PROCESS_TIME,
                                            ignore_ps_flag,
                                            sche_toolg_id,
                                            sche_entity
                                    from APS_ETL_TOOL
                                 """
            postgres_table_define = """etl_tool(parentid,toolg_id,tool_id,ch_id,tool_status,tool_status_time,ch_status,ch_status_time,coat,lot_id,down_back_time,
                                                port_num,reticle_num,location,update_time,cross_fab_flag,std_hr,std_wip,foup_num,process_mode,multi_flag,
                                                entity,bn_flag,purge_time, minor_ch_flag,module_id,queue_batch_limit,reverse_queue_time,capacity_coe,std_move,
                                                REMAIN_PROCESS_TIME,ignore_ps_flag,sche_toolg_id,sche_entity)
                                    """
            my_postgres.copy_duckdb_to_postgres(uuid=uuid,
                                                duckdb=duck_db_memory,
                                                table_name_in_duckdb=target_table,
                                                table_name_in_pg="etl_tool",  # 要小写
                                                select_sql_in_duck=select_sql_in_duck,
                                                postgres_table_define=postgres_table_define,
                                                ETL_Proc_Name=ETL_Proc_Name,
                                                oracle_conn=oracle_conn)
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
                                       cons_error_code.APS_ETL_TOOL_CODE_XX_ETL)
        except Exception as log_err:
            logging.warning("寫入錯誤日誌失敗: {log_err}".format(log_err=log_err))
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
