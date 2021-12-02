#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import platform

import rq

from base import RedisClient
from taskadmin import BatchStage, BatchWorkerEnvironmentVars, BatchWorkerQueues


def worker(processes: int):
    main_pid = os.getpid()
    while processes > 1:
        child = os.fork()
        if child:
            processes = processes - 1
        else:
            break
    name = f'{platform.uname().node}__{main_pid}__{processes}__{BatchStage.redis_friendly_timestamp()}'
    redis_connection = RedisClient(decode_responses=False).redis
    with rq.Connection(connection=redis_connection):
        w = rq.Worker(BatchWorkerQueues(redis_connection).stages_to_work_on(), name=name)
        try:
            w.work()
        finally:
            w.refresh()
            print(f'*************************************\n'
                  f'Worker going away {name}\n'
                  f'Jobs ok: {w.successful_job_count} - Jobs failed: {w.failed_job_count} - '
                  f'Working time: {w.total_working_time}\n'
                  f'*************************************')


if __name__ == "__main__":
    BatchWorkerEnvironmentVars().pull()
    worker(int(os.getenv('RQ_WORKER_PROCESSES', 4)))
