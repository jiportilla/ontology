#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

from base import BaseObject
from base import RedisClient


class BatchWorkerEnvironmentVars(BaseObject):

    _vars = [
        'RQ_CHUNK_SIZE_ASSEMBLE',
        'RQ_CHUNK_SIZE_BADGE',
        'RQ_CHUNK_SIZE_PARSE',
        'RQ_CHUNK_SIZE_XDM',
        'RQ_FOCUS_ONLY_ON',
        'RQ_MAX_WIP_ASSEMBLE',
        'RQ_MAX_WIP_INGEST',
        'RQ_STOP_AFTER',
        'RQ_TIMEOUT_MINUTES_JOB',
        'RQ_WORKER_PROCESSES',
        'ASSEMBLE_BADGES_BUILD',        # to-do: rename this
        'SUPPLY_SRC_BUILD',
        'SUPPLY_TAG_BUILD',
        'SUPPLY_XDM_BUILD',
        'DEMAND_SRC_BUILD',
        'DEMAND_TAG_BUILD',
        'DEMAND_XDM_BUILD',
        'LEARNING_SRC_BUILD',
        'LEARNING_TAG_BUILD',
        'LEARNING_XDM_BUILD',
        'FEEDBACK_SRC_BUILD',
        'FEEDBACK_TAG_BUILD',
        'FEEDBACK_XDM_BUILD',
        'PATENT_SRC_BUILD',
        'PATENT_TAG_BUILD',
    ]
    _PREFIX = 'workers_env:'

    def __init__(self):
        BaseObject.__init__(self, __name__)
        self.redis = RedisClient(decode_responses=True).redis

    def keys(self):
        return self.redis.keys(pattern=f'{self._PREFIX}*')

    def clean(self):
        keys = self.keys()
        if keys:
            self.logger.debug(f'Deleting keys {keys}...')
            self.redis.delete(*keys)

    def push(self):
        self.clean()
        env_dict = {f'{self._PREFIX}{env_var}': os.environ[env_var]
                    for env_var in self._vars if env_var in os.environ}
        if env_dict:
            self.logger.debug(f'Pushing environment:\n{env_dict}')
            self.redis.mset(env_dict)
        else:
            self.logger.warn('No environment to push to rqWorkers')

    def pull(self):
        keys = self.keys()
        if keys:
            values = self.redis.mget(*keys)
            for ii in range(len(keys)):
                key = keys[ii].replace(self._PREFIX, '')
                self.logger.debug(f'Setting {key} to "{values[ii]}"')
                os.environ[key] = values[ii]
        else:
            self.logger.warn('No environment pulled for rqWorkers')
