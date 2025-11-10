# 阅读前
    请先阅读 [install_sop.md]，了解并掌握全部的安装流程
    
    
# 严重错误产生原因
- Python版本服务器宕机
- 硬盘中的duckdb文件丢失
- Oracle中[aps_etl_ver_control_duck]表中记录的版本文件，在对应的硬盘中不存在
- 数据严重不匹配 


# 不需要Rollback的情况
Python版本服务器宕机

    发生上述情况，可以通过重启服务来恢复

硬盘中的duckdb文件丢失

Oracle中[aps_etl_ver_control_duck]表中记录的版本文件，在对应的硬盘中不存在
    
    发生上述情况，都可以通过下述方法恢复数据后再执行重启python服务
    Sync服务中的init_duckdb.py用来初始化所有Sync数据
    Sync服务中的init_ETL.py 用来执行一些ETL的前置数据准备，譬如Sync，手动执行个别前置ETL，以及从Oracle中拉取上个版本的产出

# 发生了必须要Rollback的情况
    譬如数据严重不匹配 

## 假设只有Wip有问题
- 停掉新架构的Wip和wip回写Oracle功能
- 开启旧架构Wip功能
- 通知宇清开启etl_wip的Kettle Job

## 假设Flow有问题
- 停掉新架构的 Wip和wip回写Oracle功能 Flow和FLow回写Oracle功能
- 检查DuckDB的产出，找到最后一版正常的数据产出
    
    - 将改产出文件在[aps_etl_ver_control_duck]中的Update时间手动设置为当前时间
    - 手动执行 D:\XinXiang\ETL_PYTHON\xinxiang\jobs_temp\write_back_to_oracle.py 将DuckDB结果回写到Oracle
    
- 开启旧架构Wip功能 FLow功能
- 通知宇清开启 etl_wip 以及 etl_flow 的Kettle Job