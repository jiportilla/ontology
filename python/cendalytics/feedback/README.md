# Internal Feedback
Internal Employee Feedback.  

This is a one-off analysis and likely will never form part of the repeatable Cendant Process.  The source data is confidential and is not stored in GitHub, Box or any other repository.

The code in this directory performs analytics on the `feedback_tag_<DATE>` collection within Cendant.

## Capabilities
The logic in this package fits into the yellow process shown below:
![Feedback Data Flow](https://media.github.ibm.com/user/19195/files/a6f02400-0e36-11ea-9a2e-5a0b32a92dd3 "Feedback Data Flow")

## Execution
Please read the `dataingest/README.md` for feedback ingestion.  This service presupposes the existence of a feedback tag collection.  The ingest service is responsible for that.

CSV File Generation:<br />
`python $FEEDBACK_SENTIMENT_API report --collection=feedback_tag_20200116`

## Output
1.  `feedback_tag_20200116-polarity-1579204997.csv`
    ```text
    Country	Leadership	Negative    Neutral Positive    RecordID    Region  Tenure
    China	Non-Manager	0	    0	    0	        32451	    gcg	    5.0
    India	Non-Manager	0	    0	    0	        32448	    ap	    4.0
    India	Non-Manager	0	    0	    1	        32447	    ap	    5.0
    India	Non-Manager	0	    1	    1	        32445	    ap	    3.0
    India	Non-Manager	0	    0	    1	        32444	    ap	    4.0
    ```
2.  `feedback_tag_20200116-summary-1579204921.csv`
    ```text
    Category    Country     Leadership      RecordID    Region	Schema	        Tag	        Tenure
    Cons	    China	Non-Manager	32451	    gcg	        Pay Level	Pay Raise	5.0
    Cons	    China	Non-Manager	32451	    gcg         Pay	        Pay Raise	5.0
    Other	    China	Non-Manager	32451	    gcg	        Revenue	        Profit	        5.0
    Other	    China	Non-Manager	32451	    gcg	        Organization	Profit	        5.0
    Other	    China	Non-Manager	32451	    gcg	        Pay Level	Pay Level	5.0
    ```
3.  `feedback_tag_20200116-meta-1579204921.csv` 
    This service provides a summarization of the output from step 2 (the summary report).
    ```text
    Category    Country	Leadership	RecordID    Region  Schema	Tag                     Tenure
    Cons	    India	Non-Manager	1	    ap	    Culture	Positive Team Culture	4.0
    Cons	    India	Non-Manager	1	    ap	    Culture	Positive Team Culture	4.0
    Cons	    India	Non-Manager	1	    ap	    Culture	Positive Team Culture	4.0
    Cons	    India	Non-Manager	1	    ap	    Culture	Positive Team Culture	4.0
    Cons	    India	Non-Manager	1	    ap	    Culture	Positive Team Culture	4.0
    ``` 
    Was formerly executed within this notebook `GIT-1457-1639903 (Analysis 2.0).ipynb`:
    http://localhost:8888/notebooks/Craig/GIT-1457-16399093%20(Analysis%202.0).ipynb


## Work History and Reference:
1.  Epics:
    -   Nov-Dec, 2019: [Epic 1441](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441)<br />
        "Feedback Sentiment Analysis"
    -   Jan, 2020: [Epic 1744](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1744)<br />
        "Sentiment Feedback Extension"
2.  Reporting Tasks:<br />
    -   Nov, 2019: [GIT-1419](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419)<br />
        "Demonstrate Analytics Output on Engagement Survey Feedback"
    -   Jan, 2020: [GIT-1745](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1745)<br />
        "Add a Japan Breakdown to Sentiment Report"
3.  Refactoring Tasks:<br />
    -   Jan, 2020: [GIT-1746](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1746)<br />
        "Refactor Jupyter Notebook into Cendant Service"