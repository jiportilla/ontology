#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import collections
import os
import re
import time
import typing

import kubernetes
import rq


from base import BaseObject
from base import RedisClient
from taskadmin.core.svc import BatchStage


class KubeStatus(BaseObject):

    @staticmethod
    def is_api_available():
        return 'KUBERNETES_SERVICE_HOST' in os.environ or \
               'KUBECONFIG' in os.environ

    def __init__(self):
        BaseObject.__init__(self, self.__class__.__name__)
        if 'KUBERNETES_SERVICE_HOST' in os.environ:
            kubernetes.config.load_incluster_config()
        else:
            kube_config_file = os.environ.get('KUBECONFIG')
            kubernetes.config.load_kube_config(config_file=kube_config_file)
        kubernetes.client.configuration.debug = False
        self.client = kubernetes.client.CoreV1Api()
        self.client.api_client.configuration.debug = False

    def get_pods(self):
        pods = self.client.list_namespaced_pod('cendant')
        counter = collections.Counter()
        for pod in pods.items:
            counter.update({pod.status.phase: 1})
        return {
            'success': True,
            'count': len(pods.items),
            'status': dict(counter)
        }

    def get_nodes(self):
        def is_ours(node):
            return 'role' in node.metadata.labels and 'ua-batch-worker' in node.metadata.labels['role']
        nodes = self.client.list_node()
        ours = [node for node in nodes.items if is_ours(node)]
        return {
            'success': True,
            'count': len(ours)
        }


class BatchStatus(BaseObject):

    def __init__(self, log_output=False):
        BaseObject.__init__(self, self.__class__.__name__)
        self.stages = BatchStage.stages()
        self.redis = RedisClient(decode_responses=False).redis
        self.kube_status = None
        self.log_output = log_output

    def _init_kube_status(self):
        if self.kube_status:
            return
        if KubeStatus.is_api_available():
            self.kube_status = KubeStatus()
        else:
            self.logger.error('Missing required kubernetes environment variables')

    def queues(self) -> typing.List[rq.Queue]:
        """
        Return the existing Queue objects sorted according to the stages ordering

        https://stackoverflow.com/a/48176435/239408
        """
        queues = rq.Queue.all(connection=self.redis)
        return sorted(queues, key=lambda q: self.stages.index(q.name))

    def _jobs_iterator(self, ids: list) -> typing.Iterator:
        for id in ids:
            yield rq.job.Job.fetch(id, connection=self.redis)

    def _redact_hex(self, text: str) -> str:
        return re.sub(r'0x\w*', '0x....', text)

    def get_failures(self, limit=-1) -> dict:
        failures = {
            'count': 0,
            'queue': '',
            'info': []
        }
        for queue in self.queues():
            registry = queue.failed_job_registry
            if registry.count:
                failures['queue'] = queue.name
                failures['count'] = registry.count
                failure_kinds: typing.Dict[str, list] = {}
                index = 0
                for job in self._jobs_iterator(registry.get_job_ids()):    # type: ignore
                    kind = self._redact_hex(job.exc_info)
                    if not len(kind):
                        kind = 'unknown'
                    if kind not in failure_kinds:
                        failure_kinds[kind] = []
                    failure_kinds[kind].append(job.id)
                    index += 1
                    if limit != -1 and index > limit:
                        break
                for kind, ids in failure_kinds.items():
                    by_lines = kind.split('\n')
                    failures['info'].append({                           # type: ignore
                        'error': kind,
                        'summary': f'{by_lines[1]}...{by_lines[-2]}' if len(by_lines) > 4 else kind,
                        'count': len(ids),
                        'ids': ids
                    })
                break
        if self.log_output:
            print(f'RQ-BATCH-STATUS failures: {failures}')
        return failures

    def get_wip(self, details_limit=4) -> dict:
        wip = {
            'count': 0,
            'queue': '',
            'workers': 0
        }
        for queue in self.queues():
            registry = rq.registry.StartedJobRegistry(queue=queue)
            if registry.count:
                wip['count'] = registry.count
                wip['queue'] = queue.name
                wip['workers'] = rq.Worker.count(queue=queue)
                if details_limit >= registry.count:
                    wip['job_ids'] = registry.get_job_ids()
                break
        if self.log_output:
            print(f'RQ-BATCH-STATUS wip: {wip}')
        return wip

    def get_waiting(self) -> dict:
        waiting = {
            'count': 0,
            'queue': '',
            'workers': 0
        }
        for queue in self.queues():
            if queue.count:
                waiting['queue'] = queue.name
                waiting['count'] = queue.count
                waiting['workers'] = rq.Worker.count(queue=queue)
                break
        if self.log_output:
            print(f'RQ-BATCH-STATUS waiting: {waiting}')
        return waiting

    def get_completed(self) -> dict:
        completed = {}
        for queue in self.queues():
            registry = rq.registry.FinishedJobRegistry(queue=queue)
            completed[queue.name] = registry.count
        if self.log_output:
            print(f'RQ-BATCH-STATUS completed: {completed}')
        return completed

    def get_pods(self) -> dict:
        result = {
            'success': False
        }
        self._init_kube_status()
        if self.kube_status:
            result = self.kube_status.get_pods()
        if self.log_output:
            print(f'RQ-BATCH-STATUS pods: {result}')
        return result

    def get_nodes(self) -> dict:
        result = {
            'success': False
        }
        self._init_kube_status()
        if self.kube_status:
            result = self.kube_status.get_nodes()
        if self.log_output:
            print(f'RQ-BATCH-STATUS nodes: {result}')
        return result

    @staticmethod
    def report():
        st = BatchStatus(log_output=True)
        st.get_wip()
        st.get_waiting()
        st.get_completed()
        st.get_failures(limit=10)
        st.get_pods()
        st.get_nodes()


def main():
    while True:
        try:
            BatchStatus.report()
        except Exception:   #: as err:
            pass
            # TO-DO Deal with the permission issue that is preventing us from getting pods/nodes
            #   User "system:serviceaccount:cendant:default"
            #   cannot list resource "pods" in API group "" in the namespace "cendant"

            # print(f'Error getting RQ-BATCH-STATUS: {err}')
        time.sleep(60)


if __name__ == "__main__":
    main()
