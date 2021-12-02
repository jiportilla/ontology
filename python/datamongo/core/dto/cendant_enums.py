#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from enum import Enum


class CendantCollectionCategory(Enum):
    DEMAND = 1
    SUPPLY = 2
    LEARNING = 3

    @staticmethod
    def find(some_name: str) -> __name__:
        some_name = some_name.lower()

        # high precision match
        if some_name == "demand":
            return CendantCollectionCategory.DEMAND
        if some_name == "supply":
            return CendantCollectionCategory.SUPPLY
        if some_name == "learning":
            return CendantCollectionCategory.LEARNING

        # partial precision match
        if "demand" in some_name:
            return CendantCollectionCategory.DEMAND
        if "supply" in some_name:
            return CendantCollectionCategory.SUPPLY
        if "learning" in some_name:
            return CendantCollectionCategory.LEARNING

        raise NotImplementedError(f"Unrecognized Category: {some_name}")


class CendantCollectionType(Enum):
    SRC = 1
    TAG = 2
    XDM = 3

    @staticmethod
    def find(some_name: str) -> __name__:
        some_name = some_name.lower()

        # high precision match
        if some_name == "src":
            return CendantCollectionType.SRC
        if some_name == "tag":
            return CendantCollectionType.TAG
        if some_name == "xdm":
            return CendantCollectionType.XDM

        # partial precision match
        if "src" in some_name:
            return CendantCollectionType.SRC
        if "tag" in some_name:
            return CendantCollectionType.TAG
        if "xdm" in some_name:
            return CendantCollectionType.XDM

        raise NotImplementedError(f"Unrecognized Type: {some_name}")


class CendantCollectionUsage(Enum):
    BUILD = 1
    USE = 2

    @staticmethod
    def find(some_name: str) -> __name__:
        some_name = some_name.lower()

        # high precision match
        if some_name == "build":
            return CendantCollectionUsage.BUILD
        if some_name == "use":
            return CendantCollectionUsage.USE

        # partial precision match
        if "build" in some_name:
            return CendantCollectionUsage.BUILD
        if "use" in some_name:
            return CendantCollectionUsage.USE

        raise NotImplementedError(f"Unrecognized Type: {some_name}")


class CollectionFinder(object):

    @staticmethod
    def find_xdm(collection_category: CendantCollectionCategory or str,
                 collection_usage: CendantCollectionUsage = CendantCollectionUsage.USE) -> str:

        if type(collection_category) == str:
            collection_category = CendantCollectionCategory.find(collection_category)

        return __class__.find(collection_type=CendantCollectionType.XDM,
                              collection_category=collection_category,
                              collection_usage=collection_usage)

    @staticmethod
    def find_tag(collection_category: CendantCollectionCategory or str,
                 collection_usage: CendantCollectionUsage = CendantCollectionUsage.USE) -> str:

        if type(collection_category) == str:
            collection_category = CendantCollectionCategory.find(collection_category)

        return __class__.find(collection_type=CendantCollectionType.TAG,
                              collection_category=collection_category,
                              collection_usage=collection_usage)

    @staticmethod
    def find_src(collection_category: CendantCollectionCategory or str,
                 collection_usage: CendantCollectionUsage = CendantCollectionUsage.USE) -> str:

        if type(collection_category) == str:
            collection_category = CendantCollectionCategory.find(collection_category)

        return __class__.find(collection_type=CendantCollectionType.SRC,
                              collection_category=collection_category,
                              collection_usage=collection_usage)

    @staticmethod
    def find(
            collection_type: CendantCollectionType,
            collection_category: CendantCollectionCategory,
            collection_usage: CendantCollectionUsage = CendantCollectionUsage.USE) -> str:

        def supply_key() -> str:
            if CendantCollectionType.SRC == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "SUPPLY_SRC_USE"
                return "SUPPLY_SRC_BUILD"
            elif CendantCollectionType.TAG == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "SUPPLY_TAG_USE"
                return "SUPPLY_TAG_BUILD"
            elif CendantCollectionType.XDM == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "SUPPLY_XDM_USE"
                return "SUPPLY_XDM_BUILD"

        def demand_key():
            if CendantCollectionType.SRC == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "DEMAND_SRC_USE"
                return "DEMAND_SRC_BUILD"
            elif CendantCollectionType.TAG == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "DEMAND_TAG_USE"
                return "DEMAND_TAG_BUILD"
            elif CendantCollectionType.XDM == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "DEMAND_XDM_USE"
                return "DEMAND_XDM_BUILD"

        def learning_key():
            if CendantCollectionType.SRC == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "LEARNING_SRC_USE"
                return "LEARNING_SRC_BUILD"
            elif CendantCollectionType.TAG == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "LEARNING_TAG_USE"
                return "LEARNING_TAG_BUILD"
            elif CendantCollectionType.XDM == collection_type:
                if CendantCollectionUsage.USE == collection_usage:
                    return "LEARNING_XDM_USE"
                return "LEARNING_XDM_BUILD"

        def key():
            if CendantCollectionCategory.SUPPLY == collection_category:
                return supply_key()
            elif CendantCollectionCategory.DEMAND == collection_category:
                return demand_key()
            elif CendantCollectionCategory.LEARNING == collection_category:
                return learning_key()

        key = key()
        value = os.environ[key]

        print(f"Located Cendant Collection: "
              f"(type={collection_type}, "
              f"category={collection_category}, "
              f"key={key}, "
              f"collection={value})")

        return value
