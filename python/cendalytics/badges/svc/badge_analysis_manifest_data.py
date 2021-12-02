#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from collections import Counter
from statistics import mean
from statistics import stdev
import pprint
import typing

from pymongo.operations import UpdateOne
from pymongo.errors import BulkWriteError

from base import BaseObject
from base import MandatoryParamError
from datamongo.core.dmo import BaseMongoClient
from datamongo.core.bp import CendantCollection
from dataingest.core.dmo import ManifestConnectorForMongo


class BadgeAnalysisManifestData(BaseObject):
    """Tag and calc distribution values for badges

        input records:
            {
                "_id": "Containers, K8s and Istio on IBM Cloud",
                "key_field":"Containers, K8s and Istio on IBM Cloud"},
                "fields":[
                    {"agent":"system",
                     "type":"text",
                     "name":"strategy_tags",
                     "value":["cloud"],
                     "transformations":["tag_list"]
                    },
                    {"agent":"system",
                     "type":"text",
                     "name":"category_tags",
                     "value":["knowledge"],
                     "transformations":["tag_list"]
                     }
                ]
            }
        resulting records:
            {
                "_id": "Containers, K8s and Istio on IBM Cloud",
                "badge":"Containers, K8s and Istio on IBM Cloud"},
                "ingested_tags": ["knowledge", "cloud],
                "tags":[["cloud", 99], ["ibm cloud", 95.2], ["container", 91.7]],
                "owners":{
                    "count": 1133,
                    "zScore": -0.129
                }
            }
        Created:
            13-Jan-2020
            xavier.verges@es.ibm.com
        Updated:
            14-Jan-2020
            xavier.verges@es.ibm.com
            *   Using a query to count the number of owners of each badge could
                be asking too much from mongodb when running lots of processes
                in parallel. We now walk the supply collection one instead.
    """

    def __init__(self,
                 manifest: str,
                 activity: str,
                 first: int = -1,
                 last: int = -1,
                 is_debug: bool = False):

        BaseObject.__init__(self, __name__)
        from dataingest.core.dmo import ManifestActivityFinder

        if not manifest:
            raise MandatoryParamError("Manifest Name")
        if not activity:
            raise MandatoryParamError("Activity Name")

        self._is_debug = is_debug
        self._mongo_client = BaseMongoClient()
        self._manifest = ManifestActivityFinder(manifest,
                                                activity).process()
        self._first = first
        self._last = last

    def _source(self) -> CendantCollection:
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Source Manifest",
                pprint.pformat(self._manifest["source"])]))
        return ManifestConnectorForMongo(self._manifest["source"],
                                         some_base_client=self._mongo_client,
                                         is_debug=self._is_debug).process()

    def _target(self) -> CendantCollection:
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Target Manifest",
                pprint.pformat(self._manifest["target"])]))
        return ManifestConnectorForMongo(self._manifest["target"],
                                         some_base_client=self._mongo_client,
                                         is_debug=self._is_debug).process()

    def _owners(self) -> CendantCollection:
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Badge Owners Manifest",
                pprint.pformat(self._manifest["badge_owners"])]))
        return ManifestConnectorForMongo(self._manifest["badge_owners"],
                                         some_base_client=self._mongo_client,
                                         is_debug=self._is_debug).process()

    def _input_records(self) -> list:
        collection = self._source()
        if self._first < 0:
            records = collection.all()
        else:
            limit = self._last - self._first + 1
            records = collection.skip_and_limit(self._first, limit)
        return records

    def _badges_and_ingested_tags(self,
                                  input_records: list) -> list:
        records = []
        for input_record in input_records:
            raw_tags: typing.Set[str] = set()
            for element in input_record['fields']:
                if element['name'] in ['category_tags', 'skills_tags', 'strategy_tags']:  # just in case
                    raw_tags.update(element['value'])
            records.append({
                '_id': input_record['_id'],
                'badge': input_record['key_field'],
                'ingested_tags': list(raw_tags)
            })
        return records

    def _add_parsed_tags(self,
                         output_records: list) -> list:

        from cendalytics.badges.dmo import BadgeEntityAnalysis
        for record in output_records:
            cendant_tags = BadgeEntityAnalysis(record['badge'],
                                               record['ingested_tags']).process()
            record['tags'] = cendant_tags

        return output_records

    def _persist_target(self,
                        output_records: list) -> None:

        try:
            collection = self._target()
            self.logger.debug(f'Persiting {len(output_records)} in {collection.collection_name}...')
            actions = []
            for record in output_records:
                actions.append(UpdateOne({'_id': record['_id']},
                                         {'$set': record},
                                         upsert=True))
            results = collection.collection.bulk_write(actions, ordered=False)
            return self.logger.debug(f'Persisted to {collection.collection_name}'
                                     f'matched={results.matched_count}. '
                                     f'inserted={results.inserted_count}. '
                                     f'upserted={results.upserted_count}. '
                                     f'modified={results.modified_count}')

        except BulkWriteError as xcpt:
            self.logger.error(xcpt.details)
            raise

    def source_collections(self) -> list:
        collection = self._source()
        return [(collection.collection_name, collection.count())]

    def flush_target(self) -> None:
        collection = self._target()
        collection.delete(keep_indexes=False)

    def process(self) -> None:
        input_records = self._input_records()
        output_records = self._badges_and_ingested_tags(input_records)
        output_records = self._add_parsed_tags(output_records)
        self._persist_target(output_records)

    def _get_number_of_owners(self) -> Counter:
        counter: typing.Counter[str] = Counter()
        collection = self._owners()
        for chunk in collection.by_chunks(chunk_size=2000):
            # print('.', end='', flush=True)
            for record in chunk:
                for field in record['fields']:
                    if field['type'] == 'badge':
                        counter.update({field['value']: 1})
        return counter

    def _get_zScores(self, counter_of_owners: Counter) -> list:
        counts = [x[1] for x in counter_of_owners.items()]
        count_mean = mean(counts)
        count_stdev = stdev(counts)
        del counts

        def zScore(count):
            z = (count - count_mean) / count_stdev
            return round(z, 3)

        records = []
        for badge, count in counter_of_owners.items():
            records.append({
                '_id': badge,
                'owners': {
                    'count': count,
                    'zScore': zScore(count)
                }
            })
        return records

    def analyze_distribution(self) -> None:
        counter_of_owners = self._get_number_of_owners()
        output_records = self._get_zScores(counter_of_owners)
        self._persist_target(output_records)
