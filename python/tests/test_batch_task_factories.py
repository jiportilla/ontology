#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from taskadmin.core.svc.batch_task_factories import StageTasks


@pytest.mark.parametrize("count, chunk_size, expected", [
    (1,  10, [('0', '0')]),
    (10, 10, [('0', '9')]),
    (22, 10, [('0', '9'), ('10', '19'), ('20', '21')]),
])
def test_chunks(count, chunk_size, expected):
    assert StageTasks.chunks(count, chunk_size) == expected
