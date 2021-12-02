#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import abc
import os


# ASSEMBLE_CHUNK_SIZE = int(os.environ['RQ_CHUNK_SIZE_ASSEMBLE'])
# BADGE_CHUNK_SIZE = int(os.environ['RQ_CHUNK_SIZE_BADGE'])
# PARSE_CHUNK_SIZE = int(os.environ['RQ_CHUNK_SIZE_PARSE'])
# XDM_CHUNK_SIZE = int(os.environ['RQ_CHUNK_SIZE_XDM'])


class StageTasks(abc.ABC):

    @abc.abstractmethod
    def create_tasks(self, manifest_name, manifest_activity):
        def a_func(a, b):
            return a+b
        return [('string_for_the task_id', a_func, (1, 2))]

    def opening_message(self):
        return ''

    def closing_message(self):
        return ''

    @staticmethod
    def chunks(total, chunk_size):
        chunks = []
        first = 0
        while first < total:
            last = first + chunk_size - 1 if (first + chunk_size) < total else total - 1
            chunks.append((str(first), str(last)))
            first += chunk_size
        return chunks

    @staticmethod
    def is_activity_under_focus(manifest_activity):
        if 'RQ_FOCUS_ONLY_ON' in os.environ and \
           manifest_activity.lower() not in os.environ['RQ_FOCUS_ONLY_ON'].lower():
            focus_on = os.environ['RQ_FOCUS_ONLY_ON']
            print(f'Skipping "{manifest_activity}" because we are focusing on "{focus_on}"')
            return False
        else:
            return True


class IngestsTasks(StageTasks):
    def __init__(self):
        from dataingest import call_ingest_api
        self.function = call_ingest_api
        self.count = 0

    def create_tasks(self, manifest_name, manifest_activity):
        delay_range = 0
        self.count += 1
        return [
                ('', self.function, (manifest_name, manifest_activity, 'true', str(delay_range)))
               ]

    def opening_message(self):
        return 'Starting ingestion...'

    def closing_message(self):
        return 'Done with ingestion.'


class AssembleApiTasks(StageTasks):
    def __init__(self):
        from dataingest import call_assemble_api
        self.function = call_assemble_api
        self.activity_names = set()


class PreAssembleTasks(AssembleApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        return [
                ('flush_target',
                 self.function,
                 (manifest_name, manifest_activity, 'flush_target', '', '', ''))
               ]


class AssembleTasks(AssembleApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        tasks = []
        task_specs = self.function(manifest_name, manifest_activity, 'get_sources', '', '', '')
        for collection, count in task_specs:
            if not count:
                print(f'Skipping empty collection {collection}')
                continue
            for first, last in self.chunks(count, int(os.environ['RQ_CHUNK_SIZE_ASSEMBLE'])):
                tasks.append(
                             (f'assemble_{collection}_{first}-{last}',
                              self.function,
                              (manifest_name, manifest_activity, 'assemble', collection, first, last))
                            )
        return tasks


class PostAssembleTasks(AssembleApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        self.activity_names.add(manifest_activity)
        return [
                ('index_target',
                 self.function,
                 (manifest_name, manifest_activity, 'index_target', '', '', ''))
               ]

    def closing_message(self):
        return f'Done with collection assembly.\nCompleted activities {self.activity_names}.'


class BadgeAnalysisApiTasks(StageTasks):
    def __init__(self):
        from cendalytics.badges.bp import call_badge_analysis_api
        self.function = call_badge_analysis_api


class PreBadgeAnalysisTasks(BadgeAnalysisApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        return [
                ('flush_target',
                 self.function,
                 (manifest_name, manifest_activity, 'flush_target', '', ''))
               ]


class BadgeTaggingTasks(BadgeAnalysisApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        tasks = []
        task_specs = self.function(manifest_name, manifest_activity, 'get_sources', '', '')
        for collection, count in task_specs:
            if not count:
                print(f'Skipping empty collection {collection}')
                continue
            for first, last in self.chunks(count, int(os.environ['RQ_CHUNK_SIZE_BADGE'])):
                tasks.append(
                             (f'badge_tagging_{collection}_{first}-{last}',
                              self.function,
                              (manifest_name, manifest_activity, 'analyze_per_badge', first, last))
                            )
        return tasks


class BadgeDistributionTasks(BadgeAnalysisApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        return [
                ('analyze_distribution',
                 self.function,
                 (manifest_name, manifest_activity, 'analyze_distribution', '', ''))
               ]

    def closing_message(self):
        return f'Done with badges analysis'


class ParseApiTasks(StageTasks):
    def __init__(self):
        from dataingest import call_parse_api
        self.function = call_parse_api
        self.collection_set = set()


class PreParseTasks(ParseApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        return [
                ('flush_target',
                 self.function,
                 (manifest_name, manifest_activity, 'flush_target', '', ''))
               ]


class ParseTasks(ParseApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        to_fix = ['Patent', 'Feedback', 'JRS']
        if manifest_activity in to_fix:
            print("TO-DO. Fix patents/feedback/JRS parsing")
            return []
        tasks = []
        task_specs = self.function(manifest_name, manifest_activity, 'get_sources', '', '')
        for collection, count in task_specs:
            if not count:
                print(f'Skipping empty collection {collection}')
                continue
            self.collection_set.add(collection)
            for first, last in self.chunks(count, int(os.environ['RQ_CHUNK_SIZE_PARSE'])):
                tasks.append(
                             (f'parse_{collection}_{first}-{last}',
                              self.function,
                              (manifest_name, manifest_activity, 'parse', first, last))
                            )
        return tasks

    def closing_message(self):
        return f'Done with collection parsing.\nCompleted tagging of {self.collection_set}.'


class PostParseTasks(ParseApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        return [
                ('index_target',
                 self.function,
                 (manifest_name, manifest_activity, 'index_target', '', ''))
               ]

    def closing_message(self):
        return 'Done indexing the tagged collections'


class XdmApiTasks(StageTasks):
    def __init__(self):
        from cendantdim.batch.bp import call_dimensions_api
        self.function = call_dimensions_api
        self.collection_set = set()


class PreXdmTasks(XdmApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        return [
                ('flush_target',
                 self.function,
                 (manifest_name, manifest_activity, 'flush_target', '', ''))
               ]


class XdmTasks(XdmApiTasks):
    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        tasks = []
        task_specs = self.function(manifest_name, manifest_activity, 'get_sources', '', '')
        for collection, count in task_specs:
            if not count:
                print(f'Skipping empty collection {collection}')
                continue
            self.collection_set.add(collection)
            for first, last in self.chunks(count, int(os.environ['RQ_CHUNK_SIZE_XDM'])):
                tasks.append(
                             (f'parse_{collection}_{first}-{last}',
                              self.function,
                              (manifest_name, manifest_activity, 'parse', first, last))
                            )
        return tasks

    def closing_message(self):
        return f'Done with dimension analysis.\nCompleted XDM processing of {self.collection_set}.\nDEALLOCATE THE WORKERPOOL'


class ToDB2Tasks(StageTasks):
    def __init__(self):
        from dataingest import run_manifest_command
        self.function = run_manifest_command

    def create_tasks(self, manifest_name, manifest_activity):
        if not self.is_activity_under_focus(manifest_activity):
            return []
        params = (manifest_name, manifest_activity)
        return [
            ('', self.function, params)
        ]

    def closing_message(self):
        return f'Done transfering collections to DB2'
