# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import FileIO
from datagit.analyze.bp import GitHubAnalysisOrchestrator
from datagit.graph.svc import GraphSocialNetwork
from datagit.navigate.bp import GitHubNavigationAPI


def generate_output_path(issue_number_x, issue_number_y) -> str:
    filename = f"G_{issue_number_x}-{issue_number_y}_SOCIAL-EVA.giz"
    return os.path.join(os.environ['DESKTOP'], filename)


def main():
    issue_number_x = 0
    issue_number_y = 700

    IS_DEBUG = False
    COLLECTION_NAME = "github-eva_pilot_src_20200104"
    output_path = generate_output_path(issue_number_x, issue_number_y)

    orchestrator = GitHubAnalysisOrchestrator(is_debug=IS_DEBUG)
    social_analysis = orchestrator.distributions(collection_name=COLLECTION_NAME).social(write_to_file=True)

    lines = ["digraph GitHubSNA {"]

    api = GitHubNavigationAPI(is_debug=IS_DEBUG)
    for issue in range(int(issue_number_x), int(issue_number_y)):
        svcresult = api.navigate(COLLECTION_NAME).by_issue(issue_id=issue)
        if svcresult:
            lines += GraphSocialNetwork(is_debug=IS_DEBUG,
                                        pattern=svcresult['pattern'],
                                        d_index=svcresult['index'],
                                        df_social_entity_analysis=social_analysis['ent'],
                                        df_social_relationship_analysis=social_analysis['rel']).lines()

    lines.append("}")

    FileIO.lines_to_file(lines, output_path)
    print('\n'.join([
        "Wrote to File",
        f"\tOutput Path: {output_path}"]))


if __name__ == "__main__":
    import plac

    plac.call(main)
