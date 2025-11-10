import logging
import traceback

from apscheduler.triggers.cron import CronTrigger

# def handle_job_exception(job_function):
#     def wrapper(*args, **kwargs):
#         try:
#             return job_function(*args, **kwargs)
#         except Exception as e:
#             logging.info("处理错误：{job_function.__name__}:e")
#             raise e
#     return wrapper
#
#
class CronTriger6(CronTrigger):
    @classmethod
    def cron_triger(cls, expr, timezone=None):
        values = expr.split()
        if len(values) != 6:
            raise ValueError("Cron表达式不对")

        # return cls(second=values[0],
        #            minute=values[1],
        #            hour=values[2],
        #            day=values[3],
        #            month=values[4],
        #            day_of_week=values[5],
        #            year=values[6],
        #            timezone=timezone
        #            )
        return cls(second=values[0],
                   minute=values[1],
                   hour=values[2],
                   day=values[3],
                   month=values[4],
                   day_of_week="*",
                   year="*",
                   timezone=timezone
                   )
