# Internal Feedback
Internal Employee Feedback.  This is a one-off analysis and likely will never form part of the repeatable Cendant Process

The data is loaded into a `src` collection, and from there parsed into a `tag` collection ~~and from there dimensionalized into an `xdm` collection (e.g., `feedback_xdm_20191122`)~~.  Update: Feedback Data is not dimensionalized.  Simple tagging is sufficient for analysis.  

Sample Data Flow:<br />
`Input Spreadsheet` => `feedback_src_20191122` => `feedback_tag_20191122` => `Analysis Output`

## Process Notes:
1.  Create the tag collection by instantiating a grid script:<br />
    - Template: `source admin.sh grid <source> <target>`
    - Example: `source admin.sh grid feedback_src_20200116 feedback_tag_20200116`
    - Each line of the grid script can run on a remote server
2.  Create the analysis output by following the instructions in:
    - `cendalytics/feedback/README.md`

## Input Data:
The input spreadsheet is confidential and will not be contained in Box or GitHub.    


## Work History and Reference:
1.  Initial Task (part of [Epic 1441](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441)):<br />
    [GIT-1419: Demonstrate Analytics Output on Engagement Survey Feedback](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419)
    - Nov, 2019
2.  Request for Regional Modification<br />
    [GIT-1745: Add a Japan Breakdown to Sentiment Report](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1745)
    - Jan, 2020