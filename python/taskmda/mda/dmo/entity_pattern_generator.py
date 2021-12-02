#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib import Graph

from base import BaseObject
from datagraph import OwlGraphConnector


class EntityPatternGenerator(BaseObject):
    """ Generate the NLU Variations for each token into a file
        that is used by the NLU Parser at runtime """

    # NOTE: no variables should be placed here (ref GIT-1601)

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            1-Apr-2019
            craig.trim@ibm.com
            *   added 'see-also'
        Updated:
            11-Apr-2019
            craig.trim@ibm.com
            *   change 'synonyms' from set to dictionary
                and add key/value for injection into synonyms generator downstream
        Updated:
            8-May-2019
            craig.trim@ibm.com
            *   added exception handling
                I had a funky label in the cendant OWL file and it took
                forever to trace down to this particular MDA function
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   removed 'go-words' (the input file was empty)
            *   read patterns from the synonyms.csv file
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/440
        Updated:
            21-Nov-2019
            craig.trim@ibm.com
            *   account for new expansion pattern developed here
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1424#issue-10910179
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   pass in ontology name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583#issuecomment-16612838
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   all variables initialized in __init__ statement
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1601
        """
        BaseObject.__init__(self, __name__)
        from taskmda.mda.dmo import OntologyLabelGenerator

        self._d_patterns = {}
        self._d_synonyms = {}

        self._is_debug = is_debug
        self._ontology_name = ontology_name

        self._label_generator = OntologyLabelGenerator(self._owlg())
        self._invalid_patterns = ["for", "and", "-", "with", "of", "&"]

    def _owlg(self) -> Graph:
        return OwlGraphConnector(is_debug=self._is_debug,
                                 ontology_name=self._ontology_name).process()

    def _patterns(self,
                  label: str) -> None:
        """
        Sample Input:
            'Redhat Data Virtualization Certification'

        Sample Output:
            [   'redhat+data+virtualization+certification',
                'redhat_data_virtualization_certification']

        Update Synonyms:
            { 'redhat_data_virtualization_certification': [ 'redhat data virtualization certification' ]}

        Update Patterns:
            { 'redhat_data_virtualization_certification': [ 'redhat+data+virtualization+certification' ]}

        :param label:
            any entity input
        """

        label = label.lower().strip()
        label = label.replace("/", "")

        tokens = [x.strip().lower() for x in label.split() if x]
        tokens = [x for x in tokens if x not in self._invalid_patterns]

        p1 = "+".join(tokens)
        p2 = "_".join(tokens)

        if p2 not in self._d_patterns:
            self._d_patterns[p2] = []
        if p1 not in self._d_patterns[p2]:
            self._d_patterns[p2].append(p1)

        if p2 not in self._d_synonyms:
            self._d_synonyms[p2] = []
        if label not in self._d_synonyms[p2]:
            self._d_synonyms[p2].append(label)

    def _patterns_from_synonyms_file(self) -> None:
        """
        Purpose:
        Extract explicit patterns from the Synonyms file

        Sample Input:
            redhat_data_virtualization_certification ~
                ex450,
                redhat data virtual certification,
                redhat data virtualization certification,
                redhat+data+certification,
                redhat+virtualization+certification
        Sample Output:
            {   'redhat_data_virtualization_certification': [
                    'redhat+data+certification',
                    'redhat+virtualization+certification' ]}
        """
        from taskmda.mda.dmo import SynonymsKbReader

        df_synonyms = SynonymsKbReader.by_name(self._ontology_name).read_csv()
        for _, row in df_synonyms.iterrows():
            canon = row['canon']

            variants = row['variants'].replace('!', '+')  # GIT-1424-10910179
            variants = variants.split(',')
            variants = [x.strip() for x in variants
                        if x and "+" in x]

            if not len(variants):
                continue

            if canon not in self._d_patterns:
                self._d_patterns[canon] = []
            [self._d_patterns[canon].append(x) for x in variants
             if x not in self._d_patterns[canon]]

    def _patterns_from_owl_file(self) -> None:
        """
        Purpose:
        Generate implicit patterns from the OWL file (e.g., the Cendant Ontology)

        Sample Entity:
            'Redhat Data Virtualization Certification'

        Sample Patterns:
            [   'redhat+data+virtualization+certification',
                'redhat_data_virtualization_certification' ]
        """
        owl_labels = self._label_generator.labels()

        for label in owl_labels:
            if "'" in label:  # GIT-1829-17642326
                label = label.replace("'", "")

            self._patterns(label)

            try:

                [self._patterns(x)
                 for x in self._label_generator.see_also_by_label(label)]

                [self._patterns(x)
                 for x in self._label_generator.infinitive_by_label(label)]

            except Exception as e:
                self.logger.error('\n'.join([
                    "Failed to Generate Label",
                    f"\tLabel: {label}"]))
                self.logger.exception(e)
                raise ValueError("Entity Pattern Generation Failure")

    def process(self) -> dict:
        self._patterns_from_owl_file()
        self._patterns_from_synonyms_file()

        self.logger.debug(f"Generated Variations "
                          f"(total={len(self._d_patterns)})")

        return {
            "patterns": self._d_patterns,
            "synonyms": self._d_synonyms}
