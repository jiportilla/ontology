# Purpose
One-stop shop for Cendant GitHub Ingestion and Analysis.

### Sub Directories
1. `ingest` <br />
    Performs the repository ingestion into MongoDB.
2. `navigate`<br />
    A reusable library for navigating a GitHub issues after ingestion.  Given a issue number, the navigation API allows us to find associated Pull Requests, Commits, Files and People.
3. `analyze`
    A reusable library for statistical analysis of GitHub issues.
4. `graph`<br />
    Creates the Graphviz graphs.  Leverages both the `navigate` and the `analyze` library for graph creation.
5. `scripts` <br />
    Scripts consume services and processes.  Similar to how we might use Jupyter.  These are "fit-for-purpose" scripts that do specific tasks, with no attempt at reuse or extensibility beyond their immediate function.  
6. `tests` <br />
    Unit tests for Travis
    
    

## Architecture Stack:<br />
![Architecture Stack](https://media.github.ibm.com/user/19195/files/38e54c00-3911-11ea-8577-c659007fc0c8 "Architecture Stack")

## Reference:
1.  [GIT-1619](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1619)<br />
    "EPIC - Analyze GitHub Data"