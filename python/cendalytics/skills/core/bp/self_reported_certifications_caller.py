# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import DataTypeError
from base import MandatoryParamError

IS_DEBUG = True


def main(param1, param2):
    from cendalytics.skills.core.bp import SkillsReportAPI

    def _collection_name() -> str:
        if not param1:
            raise MandatoryParamError("Collection Name")
        if type(param1) != str:
            raise DataTypeError("Collection Name, str")
        return param1

    def _output_path() -> str:
        if not param2:
            raise MandatoryParamError("Output Path")
        if type(param2) != str:
            raise DataTypeError("Output Path, str")
        return param2

    collection_name = _collection_name()
    output_path = _output_path()
    print('\n'.join([
        "Params Received",
        f"\tParam 1: (name=collection-name, value={collection_name})",
        f"\tParam 2: (name=output_path, value={output_path})"]))

    api = SkillsReportAPI(is_debug=IS_DEBUG)
    results = api.self_reported_certifications(collection_name=collection_name,
                                               exclude_vendors=None,
                                               aggregate_data=False)

    results.to_csv(output_path, sep=',', encoding='utf-8')


if __name__ == "__main__":
    import plac

    plac.call(main)
