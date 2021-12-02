#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph
from pandas import DataFrame

from base import BaseObject
from base import RecordUnavailableRecord
from datamongo import BaseMongoClient
from datamongo import CendantTag
from nlusvc.core.svc import ExecuteSparqlQuery


class OntologyAPI(BaseObject):
    """ Ontology API """

    _vs_search = None

    __generate_dataframe_weights = None

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            3-May-2019
            craig.trim@ibm.com
            *   designed for the purpose of exposing Ontology functionality via iPython Notebooks
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   add collection-category parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243#issuecomment-11507326
        Updated:
            28-Jun-2019
            craig.trim@ibm.com
            *   updated documentation and examples
        Updated:
            26-Jul-2019
            craig.trim@ibm.com
            *   updated param list
                removed cendant-collection-category and mongo-client
            *   added 'sparql-executor' method to API
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    def record(self,
               mongo_client: BaseMongoClient,
               collection_name: str,
               key_field: str) -> dict:
        """
        Generates a JSON record record for a given Serial Number (or other Key Field)

        Given this input:
            key-field = 23837231

        Generate this output:
            {   'div_field': '',
                'fields': [
                    {   'agent': 'System',
                        'collection': 'ingest_badges',
                        'name': 'badge_name',
                        'normalized': [
                            'Interskill - Mainframe Operator - Network Operations' ],
                        'tags': {
                            'supervised': [
                                'Communications Server',
                                'Computer Operator',
                                ...
                                'Z/os'],
                            'unsupervised': [] },
                        'transformations': ['badge_name'],
                        'type': 'badge',
                        'value': [
                            'Interskill - Mainframe Operator - Network Operations' ]
                    },
                    ...
                ],
                'key_field': '23837231',
                'ts': '1559757298.260312' }}

            Note that the 'evidence' function is essentially identical, but returns the output
            as a pandas Dataframe rather than pure JSON and will map the JSON fields to a
            parameter-specified schema

        :param mongo_client:
        :param collection_name:
        :param key_field:
            a Serial Number or other Key Field
        :return:
            a dict
        """
        col = CendantTag(mongo_client=mongo_client,
                         collection_name=collection_name,
                         is_debug=self.is_debug)
        return col.collection.by_key_field(key_field)  # GIT-1415-16099357

    def sparql_executor(self,
                        sparql_query: str,
                        graph_name: str = "cendant") -> ExecuteSparqlQuery:
        """
        Purpose:
            Run a SPARQL query and return a Result Set
        :param sparql_query:
            any valid SPARQL query
        :param graph_name:
            any valid RDF Graph name (e.g., 'cendant')
        :return:
            a result set of type rdflib.query.ResultRow
        """
        return ExecuteSparqlQuery(ontology_name=graph_name,
                                  sparql_query=sparql_query,
                                  is_debug=self.is_debug)

    def evidence(self,
                 key_field: str,
                 xdm_schema: str,
                 mongo_client: BaseMongoClient,
                 collection_name: str) -> DataFrame:
        """
        Generates a Dataframe of 'Evidence' for a given Serial Number (or other Key Field)

            Given this input
                key-field =     1812546302
                schema-name =   dim

            Generate this Dataframe
                +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+
                |     | Collection      | FieldName      | NormalizedText     | OriginalText       | Schema      | Tag                   | TagType      |
                +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+
                |   0 | ingest_badges   | badge_name     | Mainframe Operator | Mainframe Operator | hard skill  | Communications Server | supervised   |
                +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+

            Note that actual Dataframe will may be large and span hundreds of rows,
            with column lengths for unstructed text fields (such as OriginalText and NormalizedText)
            being in excess of several hundred characters

            The 'record' function is essentially identical and returns the same data as
            a JSON record directly from MongoDB

        :param key_field:
            a serial number
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param mongo_client:
            an instantiated mongoDB connection
        :param collection_name:
            a collection (e.g., supply_tag_20190816)
        :return:
            a pandas DataFrame
        """
        from nlusvc.core.dmo import EvidenceExtractor

        record = self.record(key_field=key_field,
                             mongo_client=mongo_client,
                             collection_name=collection_name)
        if not record:
            raise RecordUnavailableRecord(f"Record Not Found: "
                                          f"(key-field={key_field}, "
                                          f"collection={collection_name})")

        return EvidenceExtractor(some_records=[record],
                                 xdm_schema=xdm_schema,
                                 is_debug=self.is_debug).process()

    @staticmethod
    def parse(some_text: str,
              is_debug: bool = False) -> DataFrame:
        """

        Given this input
            'i deployed the deep learning model on aws'

        Generate this output
            +----+------------------+
            |    | Tag              |
            |----+------------------|
            |  0 | AWS              |
            |  1 | Deep Learning    |
            |  2 | Deployment Skill |
            |  3 | Model            |
            +----+------------------+

        :param some_text:
            any input text (single sentence)
        :param is_debug:
        :return:
            a dataframe with tags
        """
        from nlusvc.core.svc import GeneratePhraseTags
        return GeneratePhraseTags(some_text=some_text,
                                  is_debug=is_debug).process()

    @staticmethod
    def inference(some_tags: list,
                  xdm_schema: str = 'supply',
                  ontology_name: str = 'base',
                  add_tag_syns: bool = True,
                  add_tag_rels: bool = True,
                  add_rel_syns: bool = False,
                  add_rel_owns: bool = True,
                  inference_level: int = 3,
                  add_wiki_references: bool = True,
                  filter_on_key_terms: bool = False,
                  is_debug: bool = False) -> DataFrame:
        """
        Purpose:

            Given this input
                ['aws']

            Generate this
                +----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------+
                |    | ExplicitSchema   | ExplicitTag   | ImplicitSchema   | ImplicitTag              | IsPrimary   | IsVariant   | Relationship   |
                |----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------|
                |  0 | cloud            | AWS           | cloud            | Public Cloud             | False       | False       | Implies        |
                |  1 | cloud            | AWS           | hard skill       | Technical Services       | False       | False       | Implies        |
                |  2 | cloud            | AWS           | hard skill       | Amazon Machine Image     | False       | False       | PartOf         |
                |  3 | cloud            | AWS           | cloud            | EC2                      | False       | False       | PartOf         |
                |  4 | cloud            | AWS           | other            | Amazon                   | False       | False       | OwnedBy        |
                |  5 | cloud            | AWS           | cloud            | Cloud Computing Platform | False       | False       | Parent         |
                |  6 | cloud            | AWS           | cloud            | Cloud Computing Platform | False       | False       | References     |
                |  7 | cloud            | AWS           | cloud            | EC2                      | False       | False       | References     |
                +----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------+

        :param some_tags:
            a list of 0..* Cendant Ontology entities (tags)
            e.g. ['aws']
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param add_tag_syns:
            if True     add synonyms (all language variability) for incoming tag set
        :param add_tag_rels:
            if True     add relationships (from Cendant Ontology) for all incoming tag set
        :param add_rel_syns:
            if True     add synonyms for related terms
        :param add_rel_owns:
            if True     add relationships from 'a owns b'
                        these tend to be noisier than the inverse
                            'b owned-by a'
                        in the inverse case, only one result is typically returned
                        use of the 'a owns b' relationship can return dozens (or even 100s) of results        :param inference_level:
        :param inference_level:
            number of transitive hops to perform
            Given:
                a is-rel b is-rel c is-rel d is-rel e is-rel f
            Given 'a' and inference_level = 0
                returns [a]
            Given 'a' and inference_level = 1
                returns [a, b]
            Given 'a' and inference_level = 2
                returns [a, b, c]
            ...
            Given 'a' and inference_level = 5
                returns [a, b, c, d, e, f]
            ...
            Given 'a' and inference_level = 100
                returns [a, b, c, d, e, f]

            default=0;          implies no inference is performed
            no upper limit;     if the param value is greater than the number of inferential routes
                                all inferential routes will be returned

        :param add_wiki_references:
            if True     add references from dbPedia (Wikipedia Lookups)
        :param filter_on_key_terms:
            if True     filter out terms that don't match any incoming tag
                        e.g.,   if the incoming tag set is ['redhat linux', 'windows']
                                the returned tags must contain either
                                    'redhat', 'linux' or 'windows'
        :param ontology_name:
            the name of the ontology
        :param is_debug:
            if True     additional console logging statements
        :return:
            a relationships dataframe
        """
        from nlusvc.core.svc import GenerateDataFrameRels
        generate_dataframe_rels = GenerateDataFrameRels(
            xdm_schema=xdm_schema,
            add_tag_syns=add_tag_syns,
            add_tag_rels=add_tag_rels,
            add_rel_syns=add_rel_syns,
            add_rel_owns=add_rel_owns,
            ontology_name=ontology_name,
            inference_level=inference_level,
            add_wiki_references=add_wiki_references,
            filter_on_key_terms=filter_on_key_terms,
            is_debug=is_debug)

        return generate_dataframe_rels.process(
            some_tags=some_tags)

    def weight(self,
               xdm_schema: str,
               df_evidence: DataFrame,
               df_inference: DataFrame,
               add_collection_weight: bool = False,
               add_field_name_weight: bool = False,
               add_implicit_weights: bool = False,
               add_badge_distribution_weight: bool = False) -> DataFrame:
        """
            +----+----------------------+----------+
            |    | Schema               |   Weight |
            |----+----------------------+----------|
            |  0 | cloud                |        2 |
            |  1 | system administrator |        3 |
            |  2 | database             |        0 |
            |  3 | data science         |        0 |
            |  4 | hard skill           |       23 |
            |  5 | other                |       10 |
            |  6 | soft skill           |       15 |
            |  7 | project management   |       13 |
            |  8 | service management   |        6 |
            +----+----------------------+----------+

        :rtype: object
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param df_evidence:
            an evidence DataFrame generated from the 'evidence' function
        :param df_inference:
            an inference DataFrame generated from the 'inference' function
        :param add_collection_weight:
            incorporate additional weighting based on collection provenance
            for example,
                some collections contain user specified text (subjective)
                other collections are system generated text (objective)
        :param add_field_name_weight:
            incorporate additional weighting based on field name provenance
            for example,
                a professions badge for 'Data Scientist Level 3' will add a great deal of weight
                to the data science dimension (should one should exist in the specified schema)
        :param add_implicit_weights:
            incorporate weighting rules based on weighting implicit (inferred) tags vs. explicit tags
        :param add_badge_distribution_weight:
            lower weights for popular and easily attainable badges
        :return:
        """
        from nlusvc.core.svc import GenerateDataframeWeights
        if not self.__generate_dataframe_weights:
            self.__generate_dataframe_weights = GenerateDataframeWeights(
                xdm_schema=xdm_schema,
                add_provenance_weight=add_collection_weight,
                add_field_text_weight=add_field_name_weight,
                add_implicit_weights=add_implicit_weights,
                add_badge_distribution_weight=add_badge_distribution_weight)

        return self.__generate_dataframe_weights.process(
            df_evidence=df_evidence,
            df_inference=df_inference)

    @staticmethod
    def graphviz(df: DataFrame,
                 graph_style: str = 'big',
                 engine: str = 'fdp') -> Digraph:
        """
        :param df:
            OntologyAPI.inference(...)
        :param graph_style:
            'nlp', 'big'
        :param engine:
            all these options are supported:
                'dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp', 'patchwork', 'osage'
            these options are recommended:
                'dot'   creates a hierarchy, suitable for smaller graphs
                        where parent/child detail is important
                'fdp'   creates a clustered graph
                        where 'whole graph' patterns are essential
        :return:
            a Graphviz digrrah
        """
        from datagraph import GenerateInferenceGraph
        return GenerateInferenceGraph(df=df,
                                      graph_style=graph_style).process(engine=engine)


if __name__ == "__main__":
    q = """
        SELECT
            ?x_label 
        WHERE {
            {
                ?x rdfs:subClassOf+ :Skill .
            } UNION 
            {
                ?z rdfs:subClassOf+ :Skill .
                ?x ?y ?z . 
            }
            ?x rdfs:label ?x_label .
        }
    """.strip()
    results = OntologyAPI(is_debug=True).sparql_executor(q)
    for row in results.results():
        print (row)
    #print(results.size())
