# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class FindCertifications(BaseObject):
    """ One-Stop-Shop Service API for Certification queries """

    _rev_map = {}

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            5-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/606#issuecomment-13677541
        """
        BaseObject.__init__(self, __name__)
        from datadict import the_certifications_dict
        from datadict import the_certification_hierarchy_dict

        self.is_debug = is_debug
        self._certifications = the_certifications_dict
        self._certifications_lcase = dict((key.lower().strip(), self._certifications[key])
                                          for key in self._certifications)
        self._certs_hierarchy = the_certification_hierarchy_dict

    def _load_revmap(self):
        for cert in self._certifications:
            vendor = self._certifications[cert].lower()
            if vendor not in self._rev_map:
                self._rev_map[vendor] = []
            self._rev_map[vendor].append(cert)

    def exists(self,
               candidate_certification: str) -> bool:
        """
        :param candidate_certification:
            a candidate certification name
        :return:
            True        if the certification is valid (exists in the Cendant Ontology)
        """
        return candidate_certification.lower().strip() in self._certifications_lcase

    def vendor_by_certification(self,
                                certification: str,
                                normalize: bool = True) -> str or None:
        """
        :param certification:
            any certification name, such as
                'Cisco Collaboration Certification' or
                'Microsoft Certified Database Fundamentals'
        :param normalize:
            force lower case (and any other normalization options)
            for easier search and comparison
        :return:
            a vendor name, such as
                'Cisco' or
                'Microsoft'
        """

        certification = certification.lower().strip()

        def vendor():
            if certification in self._certifications_lcase:
                return self._certifications_lcase[certification]

        result = vendor()
        if result and normalize:
            return result.lower().strip()

        return result

    def certification_label(self,
                            certification: str) -> str:
        """
        :param certification:
            any certification name
                e.g., 'mcse certification'
        :return:
            the label form of the certification
                e.g., 'MCSE Certification'
        """

        _certification = certification.lower().strip()
        for k in self._certifications:
            if k.lower() == _certification:
                return k

        return certification

    def certifications_by_vendor(self,
                                 vendor: str) -> list or None:
        """
        :param vendor:
            any vendor name, such as IBM, Amazon or Microsoft
        :return:
            a sorted and unique list of Certifications by Vendor
        """
        if not self._rev_map:
            self._load_revmap()

        vendor = vendor.lower()
        if vendor in self._rev_map:
            return self._rev_map[vendor]

    def certifications(self) -> list:
        """
        :return:
            a sorted and unique list of all Certifications
        """
        return sorted(self._certifications.keys())

    def vendors(self) -> list:
        """
        :return:
            a sorted and unique list of all Vendors
        """
        return sorted(set(self._certifications.values()))

    def is_ancestor(self,
                    candidate_ancestor: str,
                    candidate_child: str) -> bool:
        """
        Purpose:
            Determine if an ancestor-child relationship exists between two certifications
        Example:
            input   is_ancestor(candidate_ancestor='Oracle Certification'
                                candidate_child='Oracle Mobile Development 2015 Essentials')
            output  True
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/629#issuecomment-13695464
        :param candidate_ancestor:
            any candidate ancestor
            (could be immediate parent or at any level in the hierarchy)
        :param candidate_child:
            any candidate child
        :return:
            True    if the candidate-ancestor is a true ancestor of the candidate-child
        """
        candidate_ancestor = candidate_ancestor.lower().strip()
        candidate_child = candidate_child.lower().strip()

        if candidate_ancestor not in self._certs_hierarchy:
            return False

        children = self._certs_hierarchy[candidate_ancestor]
        return candidate_child in children
