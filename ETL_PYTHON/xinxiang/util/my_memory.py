import logging

from memory_profiler import memory_usage


def measure_memory(job):
    try:
        job_name = job.__name__
        mem_usage = memory_usage((job, ()), interval=10, timeout=1)
        logging.info(
            '|内存跟踪|------------------| {job_name} Use: {mem_usage} MB'.format(job_name=job_name, mem_usage=max(mem_usage)))
        print('{job_name} Use: {mem_usage}'.format(job_name=job_name, mem_usage=mem_usage))
    except Exception as e:
        logging.exception("处理出错: %s", e)