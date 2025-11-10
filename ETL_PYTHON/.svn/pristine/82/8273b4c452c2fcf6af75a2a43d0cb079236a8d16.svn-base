# Python版运行环境说明
    # Sync服务
        以 windows定时任务的方式执行
    
    # ETL服务
        小的ETL（不太占用系统资源的）,以Windows服务方式执行，服务名称为[PythonService_ETL],执行进程为 [pythonservice.exe]
        大的ETL,以Windows定时任务方式执行
            - Wip
            - Flow
            - 回写 WIP 到 Oracle
            - 回写 FLOW 到 Oracle
            
    # 特殊情况说明
        Sync的 sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db定时任务
        - 存在的原因: ETL执行后，会将duckdb结果回写到Postgres（生产环境）中,但是宇清要求也写到Postgres（测试环境）
                    但是如果 Postgres（生产环境）和 Postgres（测试环境）同时写，会导致执行时间很久
                    故，Python版本的ETL做法是，正常回写到Postgres（生产环境）中，并在Oracle的APS_ETL_SYNC_CONTROL_DUCK表中记录一笔数据
                    代表这笔产出没有同步到 Postgres（测试环境） 中去
                    而【sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db定时任务】，会定时刷【Oracle的APS_ETL_SYNC_CONTROL_DUCK】，
                    将未同步到Postgres（测试环境）记录同步过去，并删除这条记录
        - 目前状态：2023/10/25
            未开启
        - 如果今后开启，需要将 
            D:\XinXiang\SYNC_PYTHON\runner\00_install_task_in_windows.bat 的
            REM schtasks /create /tn sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db /tr D:\XinXiang\SYNC_PYTHON\runner\sync_to_pg_jobs_sync_duckdb_to_pg_test_db.vbs /sc minute /mo 5
            修改为
            schtasks /create /tn sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db /tr D:\XinXiang\SYNC_PYTHON\runner\sync_to_pg_jobs_sync_duckdb_to_pg_test_db.vbs /sc minute /mo 5
            
            D:\XinXiang\SYNC_PYTHON\runner\00_delete_task_in_windows.bat 的
            REM schtasks /delete /tn "sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db" /f
            修改为
            schtasks /delete /tn "sync_xinxiang.jobs_manager_sync_duckdb_to_pg_test_db" /f
            
            而且到Oracle中执行
            truncate table APS_ETL_SYNC_CONTROL_DUCK
            然后再执行下述操作
    
    # 特殊情况说明
        # Sync服务中的init_duckdb.py用来初始化所有Sync数据
        # Sync服务中的init_ETL.py 用来执行一些ETL的前置数据准备，譬如Sync，手动执行个别前置ETL，以及从Oracle中拉取上个版本的产出
        
# Step_0 SVN取最新版本
- 一定要取一下最新版本            
    
# Step_1 配置文件替换
    # Sync服务
    将config/__init__.py命名为 __init__local.py
    将config/__init__server.py命名为 __init__.py
    
    # ETL服务
    将config/__init__.py命名为 __init__local.py
    将config/__init__server.py命名为 __init__.py
    
# Step_2 关闭运行中ETL和Sync服务
    在服务器环境中
    # Sync服务
    执行 D:\XinXiang\SYNC_PYTHON\runner\00_delete_task_in_windows.bat 删除所有Sync的定时任务
    
    # ETL服务
    执行 D:\XinXiang\ETL_PYTHON\runner\99_temp_uninstall_windows_task.bat 删除所有ETL的定时任务
    
    在任务管理器中找到 pythonservice.exe，强行关闭
    检查 Windows服务中PythonService_ETL服务为关闭状态
    
    
# Step_3 联络
    联系高盛，关闭Java版本, 关闭Java版本的被替代服务
    联系宇清，关闭Kettle的etl_???[被替代服务]
    ※※※※具體對象要參考Phase對象※※※
    
# Step_4 开启服务
    在服务器环境中
    # Sync服务
    执行 D:\XinXiang\SYNC_PYTHON\runner\00_install_task_in_windows.bat 启动所有Sync的定时任务
    
    # ETL服务
     将Windows服务中PythonService_ETL服务启动
    
     执行 D:\XinXiang\ETL_PYTHON\runner\99_temp_install_windows_task.bat 启动ETL的定时任务
     
     请高盛手动修改(廢除)  
        Flow的执行时间点为整点(廢除)
        回写Flow数据到Oracle的执行时间点为整点+10分(廢除)

# Step_5 确认查出是否正常
     
    