#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time


class BaseTimer(object):
    def __init__(self,
                 some_logger,
                 some_threshold=6.0,
                 is_debug: bool = False):
        """
        Created:
            9-Nov-2018
            craig.trim@ibm.com
        Updated:
            26-Feb-2019
            craig.trim@ibm.com
            *   add threshold in __init__
        """
        self._start = time.time()
        self.time_func_to_prevent_global_gc = time.time
        self.logger = some_logger
        self.is_debug = is_debug
        self.threshold = some_threshold

    def start(self) -> float:
        return self._start

    def end(self) -> str:
        end = str(self.time_func_to_prevent_global_gc() - self._start)
        if "e-" in end:
            return "0.000"
        return end[:5]

    def has_run_time(self) -> bool:
        if (float(self.end())) >= self.threshold:
            return True

        return False

    def log(self) -> None:
        if self.has_run_time() and self.is_debug:
            self.logger.debug("\n".join([
                "Running Time for {}: {}s".format(self.logger.name,
                                                  self.end())]))
