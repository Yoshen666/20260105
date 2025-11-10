import gc
import logging
import os

from xinxiang import config
from xinxiang.util import my_duck, my_oracle, my_date, cons_error_code, cons


def create_temp_table(duck_db):
    pass


def biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name):
    sql = """
    INSERT /*+ append */ INTO APS_MID_TYPE21_RUNTIME_PX_ALL (              
        PARENTID,MODULE_ID, TOOLG_ID, TOOL_ID, PPID, LESSTHAN20_FLAG,                                                                                                                                                                                                                                                            
        TRACKER_ZWS_FLAG,PHOTO_TYPE,ONE_WAFER_PT_TRACKER, TRACKER_SLOPE_NEW, 
        TRACKER_INTER_NEW,UPDATE_TIME, PARTCODE  
    )                                                                          
     WITH TYPE21_LR_FIRST1 AS (                                                                                                                                                         
             SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.QTY, O.TRACKER_PT                                                                                      
                   , ROUND((REGR_SLOPE(CAST(O.TRACKER_PT AS DOUBLE), CAST(O.QTY AS DOUBLE)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) * CAST(O.QTY AS DOUBLE) +
                            REGR_INTERCEPT(CAST(O.TRACKER_PT AS DOUBLE), CAST(O.QTY AS DOUBLE)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID)),6) AS PT_Y_TRACKER
                   , COUNT(1) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) AS CNT_OLD                                                                                  
                   , CASE WHEN COUNT(1) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) < 20                                                                              
                          THEN 'Y'                                                                                                                                                      
                          ELSE 'N'                                                                                                                                                      
                     END AS LESSTHAN20_FLAG                                                                                                                                             
                   , CASE WHEN MAX(O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) = MIN(O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID)      
                          THEN 'Y'                                                                                                                                                      
                          ELSE 'N'                                                                                                                                                      
                     END AS SAMEQTY_FLAG_ZWS                                                                                                                                            
                   ,ROUND(CAST(O.TRACKER_PT AS DOUBLE) / CAST(O.QTY AS DOUBLE), 5) AS ONE_WAFER_PT_TRACKER
             FROM (SELECT A.MODULE_ID, A.TOOLG_ID, A.TOOL_ID, A.PPID,  cast(A.QTY as double)/2 as qty,                                                                                                               
                           round((extract(epoch from(CAST(A.PROCESS_END AS TIMESTAMP) - CAST(A.PROCESS_START AS TIMESTAMP))))/3600.0, 6)                                                                                                                                                    
                           AS TRACKER_PT,                                                                                                                                                                                                                                                                                             
                     FROM APS_TR_TYPE21.APS_TR_TYPE21 A                                                                                                                                               
                    WHERE A.PROCESS_START >= STRFTIME(DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 day', '%Y-%m-%d %H:%M:%S')
                    and toolg_id='PX_BOND'  
                   ) O                                                                                                                                                                  
      ),                                                                                                                                                                                
      TYPE21_LR_FIRST_ZWS AS (                                                                                                                                                          
             SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.SAMEQTY_FLAG_ZWS, O.LESSTHAN20_FLAG                                                                                   
                    -- ,AVG(O.ONE_WAFER_PT_TRACKER) AS ONE_WAFER_PT_TRACKER
                    ,AVG(CAST(O.ONE_WAFER_PT_TRACKER AS DOUBLE)) AS ONE_WAFER_PT_TRACKER
             FROM (                                                                                                                                                                     
                       SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.ONE_WAFER_PT_TRACKER, O.SAMEQTY_FLAG_ZWS, O.LESSTHAN20_FLAG
                       FROM (                                                                                                                                                           
                               SELECT A.*                                                                                                                                               
                                    ,MEDIAN(RN) OVER(PARTITION BY A.MODULE_ID, A.TOOLG_ID, A.TOOL_ID, A.PPID) MED                                                                       
                               FROM (                                                                                                                                                   
                                       SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.SAMEQTY_FLAG_ZWS, O.LESSTHAN20_FLAG
                                             ,O.ONE_WAFER_PT_TRACKER                                                                                                                    
                                             ,ROW_NUMBER() OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID ORDER BY O.ONE_WAFER_PT_TRACKER) RN
                                       FROM TYPE21_LR_FIRST1 O                                                                                                                          
                      ) A                                                                                                                                                               
                            ) O                                                                                                                                                         
                       WHERE ABS(RN - MED) <= 0.5                                                                                                                                       
                   ) O                                                                                                                                                                  
            GROUP BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.SAMEQTY_FLAG_ZWS, O.LESSTHAN20_FLAG                                                                                  
       ),                                                                                                                                                                               
       TYPE21_LR_FIRST_LESSTHAN20 AS (                                                                                                                                                  
             SELECT Z.MODULE_ID, Z.TOOLG_ID, Z.TOOL_ID, Z.PPID, Z.LESSTHAN20_FLAG, Z.SAMEQTY_FLAG_ZWS, 'N' AS PA_PB_0_FLAG_ZWS,
                    Z.ONE_WAFER_PT_TRACKER, 0 AS TRACKER_SLOPE_NEW, 0 AS TRACKER_INTER_NEW                                                                                                                                                                                                                                 
             FROM TYPE21_LR_FIRST_ZWS Z                                                                                                                                                 
             WHERE Z.LESSTHAN20_FLAG = 'Y'                                                                                                                                              
       ),                                                                                                                                                                               
       TYPE21_LR_FIRST_SAMEQTY AS (                                                                                                                                                     
             SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.LESSTHAN20_FLAG, O.SAMEQTY_FLAG_ZWS , 'N' AS PA_PB_0_FLAG_ZWS,
                    O.ONE_WAFER_PT_TRACKER,  0 AS TRACKER_SLOPE_NEW, 0 AS TRACKER_INTER_NEW                                                                                             
             FROM TYPE21_LR_FIRST_ZWS O                                                                                                                                                 
             WHERE O.SAMEQTY_FLAG_ZWS = 'Y'                                                                                                                                             
             AND NOT EXISTS (SELECT 1                                                                                                                                                   
                   FROM TYPE21_LR_FIRST_LESSTHAN20 L20                                                                                                                                  
                   WHERE O.MODULE_ID = L20.MODULE_ID                                                                                                                                    
                   AND O.TOOLG_ID = L20.TOOLG_ID                                                                                                                                        
                   AND O.TOOL_ID = L20.TOOL_ID                                                                                                                                          
                   AND O.PPID = L20.PPID)                                                                                                                                               
       ),                                                                                                                                                                               
       TYPE21_LR_FIRST2 AS (                                                                                                                                                            
             SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.QTY, O.TRACKER_PT, O.PT_Y_TRACKER, O.CNT_OLD, O.LESSTHAN20_FLAG,
                    O.SAMEQTY_FLAG_ZWS, Z.ONE_WAFER_PT_TRACKER,                                                                                                                         
                    -- ROUND(ABS((O.PT_Y_TRACKER - O.TRACKER_PT) /                                                                                                                         
                    ROUND(ABS((CAST(O.PT_Y_TRACKER AS DOUBLE) - CAST(O.TRACKER_PT AS DOUBLE)) /
                    -- SQRT((SUM(POWER(O.PT_Y_TRACKER - O.TRACKER_PT,2)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID)) / O.CNT_OLD)), 2) AS SE
                    SQRT((SUM(POWER(CAST(O.PT_Y_TRACKER AS DOUBLE) - CAST(O.TRACKER_PT AS DOUBLE),2)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID)) / CAST(O.CNT_OLD AS DOUBLE))), 2) AS SE                                                                                                                                                               
             FROM TYPE21_LR_FIRST1 O                                                                                                                                                    
             INNER JOIN TYPE21_LR_FIRST_ZWS Z ON O.MODULE_ID = Z.MODULE_ID AND O.TOOLG_ID = Z.TOOLG_ID AND O.TOOL_ID = Z.TOOL_ID AND O.PPID = Z.PPID
             WHERE NOT EXISTS (                                                                                                                                                         
                SELECT 1 FROM TYPE21_LR_FIRST_LESSTHAN20 L20 WHERE O.MODULE_ID = L20.MODULE_ID AND O.TOOLG_ID = L20.TOOLG_ID AND O.TOOL_ID = L20.TOOL_ID AND O.PPID = L20.PPID
             )                                                                                                                                                                          
             AND NOT EXISTS (                                                                                                                                                           
                SELECT 1 FROM TYPE21_LR_FIRST_SAMEQTY SQ WHERE O.MODULE_ID = SQ.MODULE_ID AND O.TOOLG_ID = SQ.TOOLG_ID AND O.TOOL_ID = SQ.TOOL_ID AND O.PPID = SQ.PPID
             )                                                                                                                                                                          
       ),                                                                                                                                                                               
       TYPE21_LR_FIRST3 AS(                                                                                                                                                             
               SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.QTY, O.TRACKER_PT, O.PT_Y_TRACKER,                                                                                  
                      O.CNT_OLD, O.SE, O.ONE_WAFER_PT_TRACKER,                                                                                                                          
                      O.LESSTHAN20_FLAG, O.SAMEQTY_FLAG_ZWS                                                                                                                             
                FROM TYPE21_LR_FIRST2 O                                                                                                                                                 
                WHERE O.SE <= 1.96                                                                                                                                                      
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND1 AS (                                                                                                                                                           
               SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.QTY, O.TRACKER_PT, O.ONE_WAFER_PT_TRACKER                                                                           
               , CASE WHEN COUNT(1) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) < 20                                                                                  
                      THEN 'Y'                                                                                                                                                          
                      ELSE 'N'                                                                                                                                                          
                 END AS LESSTHAN20_FLAG                                                                                                                                                 
               , CASE WHEN MAX(O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) = MIN(O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID)
                      THEN 'Y'                                                                                                                                                          
                      ELSE 'N'                                                                                                                                                          
                 END AS SAMEQTY_FLAG_ZWS                                                                                                                                                
               -- , REGR_SLOPE(O.TRACKER_PT, O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) AS TRACKER_SLOPE_NEW
               , REGR_SLOPE(CAST(O.TRACKER_PT AS DOUBLE), CAST(O.QTY AS DOUBLE)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) AS TRACKER_SLOPE_NEW
               -- , REGR_INTERCEPT(O.TRACKER_PT, O.QTY) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) AS TRACKER_INTER_NEW
               , REGR_INTERCEPT(CAST(O.TRACKER_PT AS DOUBLE), CAST(O.QTY AS DOUBLE)) OVER(PARTITION BY O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID) AS TRACKER_INTER_NEW
               FROM TYPE21_LR_FIRST3 O                                                                                                                                                  
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND_LESSTHAN20 AS (                                                                                                                                                 
             SELECT O.MODULE_ID, O.TOOLG_ID, O.TOOL_ID, O.PPID, O.LESSTHAN20_FLAG, O.SAMEQTY_FLAG_ZWS, 'N' AS PA_PB_0_FLAG_ZWS,
                    O.ONE_WAFER_PT_TRACKER, 0 AS TRACKER_SLOPE_NEW, 0 AS TRACKER_INTER_NEW                                                                                              
             FROM TYPE21_LR_SECOND1 O                                                                                                                                                   
             WHERE O.LESSTHAN20_FLAG = 'Y'                                                                                                                                              
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND_SAMEQTY AS (                                                                                                                                                    
             SELECT T.MODULE_ID, T.TOOLG_ID, T.TOOL_ID, T.PPID, T.LESSTHAN20_FLAG, T.SAMEQTY_FLAG_ZWS , 'N' AS PA_PB_0_FLAG_ZWS,
                    T.ONE_WAFER_PT_TRACKER,  0 AS TRACKER_SLOPE_NEW, 0 AS TRACKER_INTER_NEW                                                                                             
             FROM TYPE21_LR_SECOND1 T                                                                                                                                                   
             WHERE NOT EXISTS (SELECT 1                                                                                                                                                 
                   FROM TYPE21_LR_SECOND_LESSTHAN20 L20                                                                                                                                 
                   WHERE T.MODULE_ID = L20.MODULE_ID                                                                                                                                    
                   AND T.TOOLG_ID = L20.TOOLG_ID                                                                                                                                        
                   AND T.TOOL_ID = L20.TOOL_ID                                                                                                                                          
                   AND T.PPID = L20.PPID)                                                                                                                                               
             AND T.SAMEQTY_FLAG_ZWS = 'Y'                                                                                                                                               
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND2 AS (                                                                                                                                                           
              SELECT  N.MODULE_ID, N.TOOLG_ID, N.TOOL_ID, N.PPID, N.QTY, N.TRACKER_PT, N.ONE_WAFER_PT_TRACKER                                                                           
                     ,N.LESSTHAN20_FLAG, N.SAMEQTY_FLAG_ZWS                                                                                                                             
                     ,N.TRACKER_SLOPE_NEW, N.TRACKER_INTER_NEW                                                                                                                          
                     ,CASE WHEN N.TRACKER_SLOPE_NEW < 0 OR N.TRACKER_SLOPE_NEW + N.TRACKER_INTER_NEW < 0                                                                                
                           THEN 'Y'                                                                                                                                                     
                           ELSE 'N'                                                                                                                                                     
                      END AS PA_PB_0_FLAG_ZWS                                                                                                                                           
               FROM TYPE21_LR_SECOND1 N                                                                                                                                                 
               WHERE NOT EXISTS (                                                                                                                                                       
                 SELECT 1 FROM TYPE21_LR_SECOND_LESSTHAN20 L20 WHERE N.MODULE_ID = L20.MODULE_ID AND N.TOOLG_ID = L20.TOOLG_ID AND N.TOOL_ID = L20.TOOL_ID AND N.PPID = L20.PPID
               )                                                                                                                                                                        
               AND NOT EXISTS (                                                                                                                                                         
                 SELECT 1 FROM TYPE21_LR_SECOND_SAMEQTY SQ WHERE N.MODULE_ID = SQ.MODULE_ID AND N.TOOLG_ID = SQ.TOOLG_ID AND N.TOOL_ID = SQ.TOOL_ID AND N.PPID = SQ.PPID
               )                                                                                                                                                                        
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND_PA_PB_0 AS (                                                                                                                                                    
               SELECT N.MODULE_ID, N.TOOLG_ID, N.TOOL_ID, N.PPID, N.LESSTHAN20_FLAG, N.SAMEQTY_FLAG_ZWS, N.PA_PB_0_FLAG_ZWS,
                      N.ONE_WAFER_PT_TRACKER, 0 AS TRACKER_SLOPE_NEW, 0 AS TRACKER_INTER_NEW                                                                                            
               FROM TYPE21_LR_SECOND2 N                                                                                                                                                 
               WHERE N.PA_PB_0_FLAG_ZWS = 'Y'                                                                                                                                           
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND3 AS (                                                                                                                                                           
            SELECT N.MODULE_ID, N.TOOLG_ID, N.TOOL_ID, N.PPID, N.LESSTHAN20_FLAG,N.PA_PB_0_FLAG_ZWS, N.SAMEQTY_FLAG_ZWS,                                                                
                   N.ONE_WAFER_PT_TRACKER, N.TRACKER_SLOPE_NEW, N.TRACKER_INTER_NEW                                                                                                     
            FROM  TYPE21_LR_SECOND2 N                                                                                                                                                   
            WHERE NOT EXISTS (SELECT 1                                                                                                                                                  
                   FROM TYPE21_LR_SECOND_PA_PB_0 PAB                                                                                                                                    
                   WHERE N.MODULE_ID = PAB.MODULE_ID                                                                                                                                    
                   AND N.TOOLG_ID = PAB.TOOLG_ID                                                                                                                                        
                   AND N.TOOL_ID = PAB.TOOL_ID                                                                                                                                          
                   AND N.PPID = PAB.PPID )                                                                                                                                              
       ),                                                                                                                                                                               
       TYPE21_LR_SECOND_RESULT AS (                                                                                                                                                     
             SELECT N3.MODULE_ID, N3.TOOLG_ID, N3.TOOL_ID, N3.PPID, N3.LESSTHAN20_FLAG,                                                                                                 
                   N3.SAMEQTY_FLAG_ZWS , N3.PA_PB_0_FLAG_ZWS, N3.ONE_WAFER_PT_TRACKER, N3.TRACKER_SLOPE_NEW, N3.TRACKER_INTER_NEW
             FROM TYPE21_LR_SECOND3 N3                                                                                                                                                  
             UNION                                                                                                                                                                      
             SELECT PAB.MODULE_ID, PAB.TOOLG_ID, PAB.TOOL_ID, PAB.PPID, PAB.LESSTHAN20_FLAG,                                                                                            
                    PAB.SAMEQTY_FLAG_ZWS , PAB.PA_PB_0_FLAG_ZWS, PAB.ONE_WAFER_PT_TRACKER, PAB.TRACKER_SLOPE_NEW, PAB.TRACKER_INTER_NEW
             FROM TYPE21_LR_SECOND_PA_PB_0 PAB                                                                                                                                          
             UNION                                                                                                                                                                      
             SELECT OSQ.MODULE_ID, OSQ.TOOLG_ID, OSQ.TOOL_ID, OSQ.PPID, OSQ.LESSTHAN20_FLAG,                                                                                            
                    OSQ.SAMEQTY_FLAG_ZWS , OSQ.PA_PB_0_FLAG_ZWS, OSQ.ONE_WAFER_PT_TRACKER, OSQ.TRACKER_SLOPE_NEW, OSQ.TRACKER_INTER_NEW
             FROM TYPE21_LR_FIRST_SAMEQTY OSQ                                                                                                                                           
             UNION                                                                                                                                                                      
             SELECT OL20.MODULE_ID, OL20.TOOLG_ID, OL20.TOOL_ID, OL20.PPID, OL20.LESSTHAN20_FLAG,                                                                                       
                    OL20.SAMEQTY_FLAG_ZWS , OL20.PA_PB_0_FLAG_ZWS, OL20.ONE_WAFER_PT_TRACKER, OL20.TRACKER_SLOPE_NEW, OL20.TRACKER_INTER_NEW
             FROM TYPE21_LR_FIRST_LESSTHAN20 OL20                                                                                                                                       
             UNION                                                                                                                                                                      
             SELECT NSQ.MODULE_ID, NSQ.TOOLG_ID, NSQ.TOOL_ID, NSQ.PPID, NSQ.LESSTHAN20_FLAG,                                                                                            
                    NSQ.SAMEQTY_FLAG_ZWS , NSQ.PA_PB_0_FLAG_ZWS, NSQ.ONE_WAFER_PT_TRACKER, NSQ.TRACKER_SLOPE_NEW, NSQ.TRACKER_INTER_NEW
             FROM TYPE21_LR_SECOND_SAMEQTY NSQ                                                                                                                                          
             UNION                                                                                                                                                                      
             SELECT NL20.MODULE_ID, NL20.TOOLG_ID, NL20.TOOL_ID, NL20.PPID, NL20.LESSTHAN20_FLAG,                                                                                       
                    NL20.SAMEQTY_FLAG_ZWS , NL20.PA_PB_0_FLAG_ZWS, NL20.ONE_WAFER_PT_TRACKER, NL20.TRACKER_SLOPE_NEW, NL20.TRACKER_INTER_NEW
             FROM TYPE21_LR_SECOND_LESSTHAN20 NL20                                                                                                                                      
                                                                                                                                                                                        
             GROUP BY MODULE_ID, TOOLG_ID, TOOL_ID, PPID, LESSTHAN20_FLAG, SAMEQTY_FLAG_ZWS,                                                                                            
                      PA_PB_0_FLAG_ZWS, ONE_WAFER_PT_TRACKER, TRACKER_SLOPE_NEW, TRACKER_INTER_NEW                                                                                      
       )                                                                                                                                                                                                                                                                                                                                                              
       SELECT '{uuid}' AS PARENTID, 
              t.MODULE_ID, t.TOOLG_ID, t.TOOL_ID, t.PPID, t.LESSTHAN20_FLAG,                                                                                                                                                                                                                                                            
              CASE WHEN T.SAMEQTY_FLAG_ZWS = 'Y' OR T.PA_PB_0_FLAG_ZWS = 'Y' THEN 'Y'                                                                                                   
                   ELSE 'N'                                                                                                                                                             
              END AS TRACKER_ZWS_FLAG,                                                                                                                                                  
              'TS' AS PHOTO_TYPE,                                                                                                                                                                                                                                                               
              T.ONE_WAFER_PT_TRACKER, T.TRACKER_SLOPE_NEW, T.TRACKER_INTER_NEW,
              '{current_time}' AS UPDATE_TIME,                                                                                                                                                                         
              '' AS PARTCODE                                                                                                          
       FROM TYPE21_LR_SECOND_RESULT t  
       where tracker_slope_new <> '0.0' or tracker_slope_new <>'0'                                                                                                                                                                                                                 
    """.format(uuid=uuid, current_time=current_time)

    my_duck.exec_sql(oracle_conn=oracle_conn,
                     duck_db_memory=duck_db_memory,
                     ETL_Proc_Name=ETL_Proc_Name,
                     methodName="Insert Into APS_MID_TYPE21_RUNTIME_PX_ALL",
                     sql=sql,
                     current_time=current_time,
                     update_table="APS_MID_TYPE21_RUNTIME_PX_ALL")


def execute():
    # 计算Global type21的线性回归

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
    ETL_Proc_Name = "APS_ETL_BR.APS_MID_TYPE_RUNTIME_1D_APS_MID_TYPE21_RUNTIME_PX_ALL"
    current_time = my_date.date_time_second_str()
    current_time_short = my_date.date_time_second_short_str()
    uuid = my_oracle.UUID()

    target_table = "APS_MID_TYPE21_RUNTIME_PX_ALL"
    used_table_list = ['APS_TR_TYPE21']
    target_table_sql = """
    create table {}APS_MID_TYPE21_RUNTIME_PX_ALL
    (
        parentid varchar(64),
        module_id varchar(64), 
        toolg_id varchar(64), 
        tool_id varchar(64), 
        ppid varchar(64), 
        lessthan20_flag varchar(64),                                                                                                                                                                                                                                                            
        tracker_zws_flag varchar(64),
        photo_type varchar(64),
        one_wafer_pt_tracker varchar(64), 
        tracker_slope_new varchar(64), 
        tracker_inter_new varchar(64),
        update_time varchar(64), 
        partcode  varchar(64) 
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

        biz_method_01(duck_db_memory, uuid, current_time, oracle_conn, ETL_Proc_Name)

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
        my_oracle.SaveAlarmLogData(oracle_conn, ETL_Proc_Name, e, target_db_file,
                                   cons_error_code.APS_MID_TYPE_RUNTIME_1D_CODE_XX_ETL)
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