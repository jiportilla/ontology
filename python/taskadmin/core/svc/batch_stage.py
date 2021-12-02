#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from datetime import datetime
import glob
import os
import platform
import random
import time

import rq

from base import BaseObject
from base import FileIO
from base import RedisClient

from .batch_reporter import BatchReporter

from .batch_task_factories import IngestsTasks, \
                                  PreAssembleTasks, \
                                  AssembleTasks, \
                                  PostAssembleTasks, \
                                  PreBadgeAnalysisTasks, \
                                  BadgeTaggingTasks, \
                                  BadgeDistributionTasks, \
                                  PreParseTasks, \
                                  ParseTasks, \
                                  PostParseTasks, \
                                  PreXdmTasks, \
                                  XdmTasks, \
                                  ToDB2Tasks

MAX_ATTEMPTS = 5
# JOB_TIMEOUT_IN_MINUTES = int(os.environ['RQ_TIMEOUT_MINUTES_JOB'])


def requeue_failed_jobs(watchdog_job, reset_failure_count):
    result = 'nothing failed'
    for failed_id in watchdog_job.failed_job_registry.get_job_ids():
        failed = rq.job.Job.fetch(failed_id, connection=watchdog_job.connection)
        if failed_id.startswith('close_stage_'):
            watchdog_job.failed_job_registry.remove(failed)
            continue
        failed.meta.setdefault('failures', 0)
        if reset_failure_count:
            failed.meta['failures'] = 0
        else:
            failed.meta['failures'] += 1
        if failed.meta['failures'] >= MAX_ATTEMPTS:
            print(f'Giving up on job {failed.id}')
            BatchReporter().post(f'Batch process cancelled after {MAX_ATTEMPTS} failures in job {failed.id}')
            return 'give_up'
        else:
            print(f'Requeuing failed job {failed.id}. Has already run {failed.meta["failures"]} times')
            failed.requeue()
            result = 'requeued'
    return result


def enqueue_close_stage(queue_name, connection, depends_on, notification_msg, reset_failure_count=False):
    id = f'close_stage__{queue_name}__{platform.uname().node}__{os.getpid()}__{BatchStage.redis_friendly_timestamp()}'
    job = rq.job.Job.create(close_stage,
                            id=id,
                            depends_on=depends_on,
                            args=(notification_msg, reset_failure_count),
                            timeout='72h',
                            result_ttl=86400,       # result expires after 1 day
                            connection=connection)
    print(f'{job.id}')
    queue = rq.Queue(queue_name, connection=connection)
    queue.enqueue_job(job)


def close_stage(notification_msg, reset_failure_count=False):
    job = rq.get_current_job()
    queue_name = job.origin
    registry = rq.registry.StartedJobRegistry(name=queue_name)

    while True:
        failed_jobs_status = requeue_failed_jobs(job, reset_failure_count)
        if failed_jobs_status == 'give_up':
            break
        elif failed_jobs_status == 'requeued':
            print('requeueing close_stage so that the requeued failed jobs can access this node')
            enqueue_close_stage(queue_name, job.connection, job.dependency, notification_msg)
            break

        if registry.count == 1:
            if job.failed_job_registry.count == 0:
                print(f'Done with queue {queue_name}')
                if notification_msg:
                    BatchReporter().post(notification_msg)

                stop_after = os.environ.get('RQ_STOP_AFTER')
                if stop_after and stop_after.lower() == queue_name:
                    print(f'Stopping because RQ_STOP_AFTER == {queue_name}')
                    next_stage = None
                else:
                    next_stage = BatchStage.next(queue_name)

                if not next_stage:
                    print('No more stages!')
                    BatchReporter().post("No additional work scheduled")
                else:
                    print('Scheduling next stage')
                    next_stage.process()
                break

        sleep = 30
        print(f'Not done ({queue_name}). queue: {registry.count} failed: '
              f'{job.failed_job_registry.count}. Waiting for {sleep} seconds...')
        time.sleep(sleep)

    print(f'leaving close_stage with failed_jobs_status={failed_jobs_status} ')


def randomize_to_try_to_distribute_the_load(a_list):
    random.shuffle(a_list)
    return a_list


class BatchStage(BaseObject):
    _stage_specs = [
        # WARNING: do not use queue names that are substrings of other queue names
        # This (having 'assemble' instead of 'actual_assemble') drove me crazy
        # with a puzzling and apparently unrealted import error

        # It could be tempting to move this to a yaml file, but doing so
        # would hide some errors until runtime; no errors can be surfaced
        # at edit time by a Python linter.
        {
            'name': 'ingest',
            'exclude_manifests': ['ingest-manifest-budapest.yml'],
            'taskFactory': IngestsTasks,
            'randomize': True,
            'maxWip': '$RQ_MAX_WIP_INGEST',
            'timeout': 40,
            'workerSelectorVar': {
                'K8_NODE_ROLE': 'ua-batch-worker-permanent-pool'
            }
        },
        {
            'name': 'pre_assemble',
            'manifest_glob': 'assemble-manifest-*.yml',
            'taskFactory': PreAssembleTasks,
        },
        {
            'name': 'actual_assemble',
            'manifest_glob': 'assemble-manifest-*.yml',
            'taskFactory': AssembleTasks,
            'randomize': True,
            'maxWip': '$RQ_MAX_WIP_ASSEMBLE',
        },
        {
            'name': 'post_assemble',
            'manifest_glob': 'assemble-manifest-*.yml',
            'taskFactory': PostAssembleTasks,
        },
        {
            'name': 'pre_badge_analysis',
            'manifest_glob': 'badge-analysis-manifest.yml',
            'taskFactory': PreBadgeAnalysisTasks,
        },
        {
            'name': 'badge_tagging',
            'manifest_glob': 'badge-analysis-manifest.yml',
            'taskFactory': BadgeTaggingTasks,
        },
        {
            'name': 'badge_distribution',
            'manifest_glob': 'badge-analysis-manifest.yml',
            'taskFactory': BadgeDistributionTasks,
        },
        {
            'name': 'pre_parse',
            'manifest_glob': 'parse-manifest.yml',
            'taskFactory': PreParseTasks,
        },
        {
            'name': 'actual_parse',
            'manifest_glob': 'parse-manifest.yml',
            'taskFactory': ParseTasks,
            'randomize': True,
        },
        {
            'name': 'post_parse',
            'manifest_glob': 'parse-manifest.yml',
            'taskFactory': PostParseTasks,
        },
        {
            'name': 'pre_dimension',
            'manifest_glob': 'dimension-manifest.yml',
            'taskFactory': PreXdmTasks,
        },
        {
            'name': 'actual_dimension',
            'manifest_glob': 'dimension-manifest.yml',
            'taskFactory': XdmTasks,
            'randomize': True,
        },
        {
            'name': 'todb2',
            'manifest_glob': 'todb2-manifest-*.yml',
            'taskFactory': ToDB2Tasks,
            'timeout': 6 * 60,
            'workerSelectorVar': {
                # Note that we want to run now in the permanent pool not
                # because the temp pool is unable to reach the intranet (it will be able at this stage),
                # but because we want to be able to deallocate the expensive temp pool
                'K8_NODE_ROLE': 'ua-batch-worker-permanent-pool'
            }
        },
    ]

    @classmethod
    def stages(cls):
        return [stage['name'] for stage in cls._stage_specs]

    @classmethod
    def worker_selection_specs(cls):
        def stage_wip(stage):
            wip = 0
            if 'maxWip' in stage:
                wip = stage['maxWip']
                if not isinstance(wip, int):
                    if wip[0] == '$':
                        wip = os.environ[wip[1:]]
                    wip = int(wip)
            return wip
        specs = []
        for stage in cls._stage_specs:
            specs.append({
                'stage': stage['name'],
                'max_wip': stage_wip(stage),
                'env_vars': stage.get('workerSelectorVar', None)
            })
        return specs

    @classmethod
    def next(cls, stage_name=''):
        stages = cls.stages()
        if not stage_name:
            return BatchStage(cls._stage_specs[0])
        else:
            index = stages.index(stage_name)
            if index + 1 != len(stages):
                return BatchStage(cls._stage_specs[index + 1])
        return None

    @classmethod
    def clean_rq(cls):
        redis = RedisClient(decode_responses=False).redis
        rq_keys = redis.keys(pattern='rq:*')
        if rq_keys:
            redis.delete(*rq_keys)

    @classmethod
    def restart_failed_stage(cls):
        redis = RedisClient(decode_responses=False).redis
        for queue_name in cls.stages():
            queue = rq.Queue(queue_name, connection=redis)
            registry = queue.failed_job_registry
            if registry.count:
                enqueue_close_stage(queue_name,
                                    redis,
                                    None,
                                    f'{queue_name} stage completed after being re-started',
                                    True)
                BatchReporter().post(f'Re-starting failed tasks of stage {queue_name}')
                break
        else:
            print('Nothing to re-start')

    @staticmethod
    def redis_friendly_timestamp():
        return datetime.now().isoformat().replace(':', ';')

    def __init__(self, stageSpec):
        BaseObject.__init__(self, self.__class__.__name__)
        self.redis = RedisClient(decode_responses=False).redis
        self.spec = stageSpec
        self.queue_name = stageSpec['name']
        self.factory = stageSpec['taskFactory']()

    def _get_manifests(self):
        ignore = self.spec.get('exclude_manifests', [])
        manifest_glob = self.spec.get('manifest_glob', f'{self.queue_name}-manifest-*.yml')
        pattern = os.path.join(os.environ['CODE_BASE'], 'resources', 'manifest', manifest_glob)
        return [manifest for manifest in glob.glob(pattern) if os.path.basename(manifest) not in ignore]

    def _get_manifest_specs(self):
        specs = []
        for manifest_file in self._get_manifests():
            manifest = FileIO.file_to_yaml(manifest_file)
            manifest_name = os.path.splitext(os.path.basename(manifest_file))[0]
            for activity in manifest['activity']:
                specs.append((manifest_name, activity['name']))
        return specs

    def _report_scheduling(self):
        print(f'Scheduling stage *{self.queue_name}*')
        message = self.factory.opening_message()
        if message:
            BatchReporter().post(message)

    def process(self):
        self._report_scheduling()
        serial = self.spec.get('serial', False)
        timeout = self.spec.get('timeout', int(os.environ["RQ_TIMEOUT_MINUTES_JOB"]))
        queue = rq.Queue(self.queue_name, connection=self.redis)
        depends_on = None
        to_enqueue = []
        manifest_specs = self.spec.get('specs', self._get_manifest_specs())
        for manifest_name, activity_name in manifest_specs:
            tasks = self.factory.create_tasks(manifest_name, activity_name)
            for task_name, task_function, task_params in tasks:
                if task_name:
                    task_name = f'{task_name}__'
                id = f'{task_name}{manifest_name}__{activity_name}' \
                     f'__{platform.uname().node}__{os.getpid()}__{self.redis_friendly_timestamp()}'
                job = rq.job.Job.create(task_function,
                                        args=task_params,
                                        id=id,
                                        depends_on=depends_on,
                                        timeout=f'{timeout}m',
                                        result_ttl=24*60*60,        # result expires after 1 day
                                        connection=self.redis)
                if serial:
                    depends_on = job
                to_enqueue.append(job)
        randomize = self.spec.get('randomize', False)
        if randomize:
            to_enqueue = randomize_to_try_to_distribute_the_load(to_enqueue)
        for job in to_enqueue:
            print(f'{job.id}')
            queue.enqueue_job(job)
        enqueue_close_stage(self.queue_name, self.redis, depends_on, self.factory.closing_message())
