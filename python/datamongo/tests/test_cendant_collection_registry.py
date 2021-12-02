# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from datamongo import CendantCollectionRegistry

NAMES = [
    "demand_src_20191002",
    "demand_tag_20191003",
    "demand_xdm_20191005",
    "learning_src_20191031",
    "learning_tag_20191106",
    "learning_xdm_20191111",
    "supply_src_20190913",
    "supply_src_20191008",
    "supply_src_20191025",
    "supply_src_20191031",
    "supply_src_20191112",
    "supply_src_20191116",
    "supply_src_20191116",
    "supply_tag_20190913",
    "supply_tag_20191014",
    "supply_tag_20191025",
    "supply_tag_20191106",
    "supply_tag_20191112",
    "supply_tag_20191117",
    "supply_tag_20191119",
    "supply_xdm_20190917",
    "supply_xdm_20191015",
    "supply_xdm_20191029",
    "supply_xdm_20191111",
]


class CollectionNamesLoader(object):
    def names(self):
        return NAMES


@pytest.fixture
def registry():
    yield CendantCollectionRegistry(collection_names_loader=CollectionNamesLoader())


def test_latest(registry: CendantCollectionRegistry):
    assert registry.latest().supply().src() == "supply_src_20191116"
    assert registry.latest().supply().tag() == "supply_tag_20191119"
    assert registry.latest().supply().xdm() == "supply_xdm_20191111"

    assert registry.latest().demand().src() == "demand_src_20191002"
    assert registry.latest().demand().tag() == "demand_tag_20191003"
    assert registry.latest().demand().xdm() == "demand_xdm_20191005"

    assert registry.latest().learning().src() == "learning_src_20191031"
    assert registry.latest().learning().tag() == "learning_tag_20191106"
    assert registry.latest().learning().xdm() == "learning_xdm_20191111"


def test_list(registry: CendantCollectionRegistry):
    for name in ["supply", "learning", "demand"]:
        expected = set([item.split('_')[-1] for item in NAMES if name in item])
        assert sorted(getattr(registry.list(), name)()) == sorted(expected)
        assert sorted(registry.list().by_name(name)) == sorted(expected)


def test_by_date(registry: CendantCollectionRegistry):
    registry.by_date("20191112").supply().src() == "supply_src_20191112"
    registry.by_date("20191112").supply().tag() == "supply_tag_20191112"
    registry.by_date("20191112").supply().xdm() == ""
