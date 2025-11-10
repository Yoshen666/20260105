import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler

from xinxiang import config


def init_log(file_name, when='D'):
    error_file_name = os.path.join(config.g_error_log_path, "APS_ETL_ERROR_.log")
    # 确保日志目录存在
    if not os.path.isdir(config.g_log_path):
        os.makedirs(config.g_log_path)
    if not os.path.isdir(config.g_error_log_path):
        os.makedirs(config.g_error_log_path)

        # 创建logger
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)  # 设置logger级别为DEBUG，以便捕获所有级别的日志

    fmt = '[%(asctime)s] - [%(levelname)s] - %(pathname)s #Line:%(lineno)d - %(message)s'

    # 生成带日期的文件名
    current_day = datetime.datetime.now().strftime('%Y%m%d')
    target_file_name = os.path.join(config.g_log_path, file_name.replace('.log', '') + current_day + '.log')
    target_error_file_name = os.path.join(config.g_error_log_path,
                                          error_file_name.replace('.log', '') + current_day + '.log')

    # 创建并配置handlers
    info_log_handler = TimedRotatingFileHandler(target_file_name, encoding="utf-8", when=when, interval=1,
                                                backupCount=7)
    info_log_handler.setLevel(config.g_log_level)
    info_log_handler.setFormatter(logging.Formatter(fmt))

    error_log_handler = TimedRotatingFileHandler(target_error_file_name, encoding="utf-8", when=when, interval=1,
                                                 backupCount=7)
    error_log_handler.setLevel(config.g_error_log_level)
    error_log_handler.setFormatter(logging.Formatter(fmt))

    # 将handlers添加到logger
    log.addHandler(info_log_handler)
    log.addHandler(error_log_handler)
