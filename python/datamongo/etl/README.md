# Purpose
Perform ETL (Extract-Transform-Load) on Cendant Collections.

This typically means, for a given collection, offer functionality to transform the records into `pandas.DataFrame` or write to file.

Writing a collection to file (caching) offers runtime advantages for analysis.  A local connection either WFTAG or MongoDB Cloud can be time consuming.  A file-cached version of the collection is quicker to deal with. 

## Sample Usage:

1.  The service will take Records retrieved in a prior step
2.  The Collection Name is passed for reference only<br />
    (Used for building the file output name) 
3.  The `include_text` parameter will optionally include the `Original` and `Normalized` forms of each field.
```python
from datamongo.etl.svc import TransformCendantRecords

records = [] # retrieved in a prior step ...
collection_name = str("supply_tag_20201116")  # for reference only

TransformCendantRecords.to_file(
    records=records,
    include_text=True,
    collection_name=collection_name)  
```

Sample Output:



## Work History and Reference
1.  [GIT-1769: Refactor Offline Caching of Collections](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1769)<br />
    Consolidate functionality for writing Cendant Records to DataFrame and File into a single service.