#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject


class AugmentationAPI(BaseObject):
    """ API for Operations to Augment the Cendant Knowledge Base
    """

    _records = None

    # Reference GIT-1721-17070627 for file access
    __redirects_file_raw = "/Users/craig.trimibm.com/Downloads/redirects_en.ttl"
    __redirects_file_csv = "/Users/craig.trimibm.com/Downloads/redirects.csv"

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            25-Jul-2019
            craig.trim@ibm.com
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   update param list for entities
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1703#issuecomment-17023405
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug

    def entities(self,
                 terms: list,
                 sub_class: str,
                 is_defined_by: Optional[str],
                 version_info: Optional[str],
                 see_also: Optional[list],
                 part_of: Optional[list],
                 implications: Optional[list],
                 comment: str) -> list:
        """
        Purpose:
            Given a list of input tokens (or text),
            generate a list of Cendant Ontology entries that can be added directly to the OWL file

        Sample Input:
            terms       ['Adobe Creative Cloud']
            sub_class   'Cloud_Computing_Platform'
            comment     'Wikipedia Analysis 22-Jul-2019
        Sample Output:
            ###  http://www.semanticweb.org/craigtrim/ontologies/cendant#Adobe_Creative_Cloud
            :Adobe_Creative_Cloud rdf:type owl:Class ;
                  rdfs:comment "Wikipedia Analysis 22-Jul-2019" ;
                  rdfs:label "Adobe Creative Cloud" .
                  rdfs:subClassOf :Cloud_Computing_Platform ;

        :param terms:
            any list of tokens or text
        :param sub_class:
            the rdfs:subClassOf target
            this entity must exist in the Cendant Ontology and must be cased/formatted correctly
        :param comment:
            a comment that identifies the activity used to generate these entities
            used for traceability purposes
        :param is_defined_by:
            typically the dbPedia link responsible for defining this entity
        :param version_info:
            info about the activity that generated this
        :param see_also:
            an optional list of synonyms
        :param part_of:
            an optional list of partonomgy
        :param implications:
            an optional list of implications
        :return:
            a list of language variations suitable for adding to synonyms_kb.csv
        """
        from taskmda.augment.svc import AddOntologyEntries

        svc = AddOntologyEntries(is_debug=self.is_debug)
        return svc.process(terms=terms,
                           comment=comment,
                           sub_class=sub_class,
                           see_also=see_also,
                           part_of=part_of,
                           implications=implications,
                           version_info=version_info,
                           is_defined_by=is_defined_by)

    def redirects(self,
                  terms: list,
                  ontology_name: str) -> dict:
        """
        Purpose:
            For each term, find the associated dbPedia redirects

        Sample Input:
            ['Cytotoxicity']
        Sample Output:
            ['Cytotoxia', 'Cytotoxic_agent', 'Cytotoxins', ... 'Cytotoxic_effects']

        :param terms:
            any list of terms
        :param ontology_name:
        :return:
            a dictionary keyed by each term
            where the value is a list of 0..* relevant dbPedia redirects
        """
        from taskmda.augment.svc import AddDBPediaRedirects

        svc = AddDBPediaRedirects(is_debug=self.is_debug,
                                  ontology_name=ontology_name,
                                  redirects_file=self.__redirects_file_csv)
        return svc.process(terms)

    def synonyms(self,
                 terms: list,
                 min_threshold: int = 2,
                 as_unified_list: bool = False) -> list:
        """
        Purpose:
            Given a list of input tokens (or text),
            generate a list of synonyms that can be added to the synonyms_kb.csv file

        Sample Input:
            [   'Accessory Cloud',
                'ActinoForm Cloud',
                'Adobe Creative Cloud' ]
        Sample Output:
            accessory_cloud~accessory cloud, accessory+cloud
            actinoform_cloud~actinoform cloud, actinoform+cloud
            adobe_creative_cloud~adobe creative cloud, adobe+creative+cloud

        :param terms:
            any list of tokens or text
        :param min_threshold:
        :param as_unified_list:
            if True         return a single synonyms entry
                            sample terms input:
                                [abab for sap, exam code, business one]
                            sample output:
                                [   abab for sap, abab_for_sap, abap+sap,
                                    exam code, exam_code, exam+code,
                                    business one, business_one, business+one ]
            if False        return a synonyms entry for each term input
                            sample terms input:
                                [abab for sap, exam code, business one]
                            sample output:
                                [   [ abap_for_sap~abab for sap, abap+sap ],
                                    [ exam_code~exam code, exam+code ],
                                    [ business_one~business one, business+one ]]
        :return:
            a list of language variations suitable for adding to synonyms_kb.csv
        """
        from taskmda.augment.svc import AddLanguageVariability

        svc = AddLanguageVariability(is_debug=self.is_debug)
        return svc.process(terms=terms,
                           min_threshold=min_threshold)


def main(ontology_name, some_term):
    api = AugmentationAPI(is_debug=True)
    api.redirects([some_term],
                  ontology_name=ontology_name)


if __name__ == "__main__":
    import plac

    plac.call(main)
