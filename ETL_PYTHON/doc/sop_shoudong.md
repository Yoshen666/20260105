## 1.SVN下載最新代碼
——---OK
## 2.更新 [ETL/config] &  [Sync/config] 文件,在文件尾部追加
——---OK
    # 代表在95上，生產代碼+測試代碼混用模式: !!!重要 ，最終生產環境為 False
    g_test_and_prod_mixed = True
    # 已經上綫的ETL的PG產出表
    g_etl_prod_tables = ['etl_flow',
                         'etl_wip'
                         ]
    
    # PG的測試的測試環境
    g_postgres_test_test_host = '10.52.192.99'
    g_postgres_test_test_database = 'uo'
    g_postgres_test_test_port = '5433'
    g_postgres_test_test_user = 'xxetl'
    g_postgres_test_test_password = 'xxetl1234'
    g_pgserver_test_test_path = '\\\\10.52.192.99\\ETLOutput'

## 手動更新 【ETL】 my_postgres.py
    將本地代碼完全覆蓋 /xinxiang/util/my_postgres.py
——---OK

## 手動更新 【ETL】 init_ETL.py
——---OK
    執行 python init_ETL.py
    
## 手動停止 【ETL】 服務
——---OK

## 更新全部的 【ETL】 /xinxiang/jobs_etl 代碼
- 完成FLOW更新
——---OK

## 手動開啓 【ETL】 服務
——---OK

## 手動執行 runner/99_shoudong_install.bat
——---OK

# ----------完成 【ETL】上版
——---OK

## 手動更新 【Sync】 my_postgres.py
    將本地代碼完全覆蓋 /xinxiang/util/my_postgres.py
——---OK

# ----------完成 【Sync】上版
——---OK