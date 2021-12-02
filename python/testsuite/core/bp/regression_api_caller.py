# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame
from tabulate import tabulate

from base import DataTypeError
from base import MandatoryParamError


def mandatory_param_error(name: str,
                          position: int):
    raise MandatoryParamError('\n'.join([
        f"Expected Parameter "
        f"(name={name}, "
        f"position={position})"]))


def data_type_error(param,
                    position: int,
                    expected_data_type):
    raise DataTypeError('\n'.join([
        "Unexpected Data Type "
        f"Chunk Size (position={position}):",
        f"\tActual Data Type: {type(param)}",
        f"\tExpected Data Type: {expected_data_type}"]))


def main(param1, param2, param3):
    if not param1:
        mandatory_param_error("Regression Name", 1)

    if type(param1) != str:
        data_type_error(param1, 1, str)

    if not param2:
        mandatory_param_error("Regression Filter Name", 1)

    if type(param2) != str:
        data_type_error(param2, 2, str)

    def regression_name():
        return param1.lower()

    def regression_filter_name():
        if param2:
            return param2.lower()

    def is_debug():
        if not param3:
            return False
        return param3.lower() == "true"

    from testsuite.core.bp import RegressionAPI

    _regression_name = regression_name()
    _filter_name = regression_filter_name()
    _is_debug = is_debug()

    api = RegressionAPI(
        is_debug=_is_debug,
        log_results=_is_debug)

    print('\n'.join([
        "Received Input Parameters",
        f"\tParam1: (name=regression-name, value={_regression_name})",
        f"\tParam2: (name=filter-name, value={_filter_name})",
        f"\tParam3: (name=is-debug, value={_is_debug})"]))

    def _tabulate(some_df: DataFrame) -> None:
        if some_df.empty:
            print("No Service Result")
        print(tabulate(some_df,
                       headers='keys',
                       tablefmt='psql'))

    if "certification" in _regression_name:
        if _filter_name and _filter_name != 'all':
            svcresult = api.certifications(segment_by_vendor=False).by_vendor(_filter_name)
        else:
            svcresult = api.certifications().all()
        if svcresult:
            _tabulate(svcresult['summary'])
    elif "synonym" in _regression_name:
        if _filter_name and _filter_name != 'all':
            svcresult = api.synonyms(segment_by_synonym=False).by_vendor(_filter_name)
        else:
            svcresult = api.synonyms().all()
        if svcresult:
            _tabulate(svcresult['summary'])
    elif "dimension" in _regression_name:
        svcresult = api.dimensions().by_serial_number(_filter_name)
        if svcresult:
            _tabulate(svcresult['summary'])
    else:
        raise NotImplementedError(f"Regression Not Found: {_regression_name}")


if __name__ == "__main__":
    import plac

    plac.call(main)
