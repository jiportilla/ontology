#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import random
import time
import os

from redis import WatchError

from base import RedisClient
from .batch_stage import BatchStage


class BatchWorkerQueues(object):

    _PREFIX = 'workers_wip:'

    @classmethod
    def clear_max_wip(cls):
        redis = RedisClient(decode_responses=False).redis
        keys = redis.keys(pattern=f'{cls._PREFIX}*')
        if keys:
            redis.delete(*keys)

    def __init__(self, redis_connection):
        self._redis_connection = redis_connection

    def _max_wip_reached(self, name, max_wip):
        if not max_wip:
            return False
        # Adapted from https://github.com/andymccurdy/redis-py#pipelines
        key = f'{self._PREFIX}{name}:workers'
        with self._redis_connection.pipeline() as pipe:
            while True:
                try:
                    # put a WATCH on the key that holds our sequence value
                    pipe.watch(key)
                    # after WATCHing, the pipeline is put into immediate execution
                    # mode until we tell it to start buffering commands again.
                    # this allows us to get the current value of our sequence
                    current_value = pipe.get(key)
                    current_value = int(current_value) if current_value else 0
                    if current_value >= max_wip:
                        print(f'Max wip for {name} reached. This worker will ignore that queue.')
                        return True
                    next_value = current_value + 1
                    # now we can put the pipeline back into buffered mode with MULTI
                    pipe.multi()
                    pipe.set(key, next_value)
                    # and finally, execute the pipeline (the set command)
                    pipe.execute()
                    # if a WatchError wasn't raised during execution, everything
                    # we just did happened atomically.
                    return False
                except WatchError:
                    # another client must have changed our key between
                    # the time we started WATCHing it and the pipeline's execution.
                    # our best bet is to just retry.
                    time.sleep(random.randint(1, 5))
                    continue

    def stages_to_work_on(self):
        stages = []
        for spec in BatchStage.worker_selection_specs():
            name = spec['stage']
            skip = False
            if spec['env_vars']:
                for key, value in spec['env_vars'].items():
                    if key not in os.environ or os.environ[key] != value:
                        skip = True
            if not skip:
                skip = self._max_wip_reached(name, spec['max_wip'])
            if not skip:
                stages.append(name)
        return stages
