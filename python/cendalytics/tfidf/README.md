# Vector Space API
Methods for dealing with a Corpus Vector Space

## Create
Create a Vector Space using TFIDF metrics.

### Sample Usage
#### Shell
```shell script
source admin.sh vectorspace create "${COLLECTION_NAME}" "${LIMIT}"
source admin.sh vectorspace create "supply_tag_20191025" 5000
```

#### Python
```python
from datamongo import BaseMongoClient
from cendalytics import VectorSpaceAPI

collection_name = 'supply_tag_20191025'

api = VectorSpaceAPI(is_debug=True)
gen = api.create(
    limit=5000,
    collection_name=collection_name,
    mongo_client=BaseMongoClient('wftag'))

gen.process()
```   

### Output
This function will generate a DataFrame (persisted as a CSV file) that looks like this:

#### Sample Output
```text
+-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
|       | Doc       |   DocsWithTerm |      IDF |      TF |     TFIDF | Term                |   TermFrequencyInCorpus |   TermFrequencyInDoc |   TermsInDoc |   TotalDocs |
|-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------|
|     0 | 0697A5744 |            159 |  2.39904 | 0.06667 |   0.02779 | cloud service       |                     186 |                    1 |           15 |        1751 |
|     1 | 0697A5744 |           1094 |  0.47035 | 0.06667 |   0.14174 | management          |                    2573 |                    1 |           15 |        1751 |
|     2 | 0697A5744 |           2006 | -0.13596 | 0.06667 |  -0.49036 | agile               |                    2194 |                    1 |           15 |        1751 |
|     3 | 0697A5744 |           2995 | -0.53676 | 0.06667 |  -0.1242  | ibm                 |                    5857 |                    1 |           15 |        1751 |
|     4 | 0697A5744 |            513 |  1.22767 | 0.06667 |   0.0543  | data science        |                     745 |                    1 |           15 |        1751 |
...
| 97480 | 04132K744 |            479 |  1.29624 | 0.01754 |   0.01353 | maintenance         |                     945 |                    1 |           57 |        1751 |
+-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
```
The CSV file is persisted to:
`$GTS_BASE/resources/output/SUPPLY_TAG_20191025_TFIDF.csv`

## Read
Read a Vector Space by Key Field.

### Usage:
#### Shell:
```shell script
source admin.sh vectorspace read "${LIBRARY}" "${KEY_FIELD}"
source admin.sh vectorspace read "SUPPLY_TAG_20191025_TFIDF.csv" "0593B3744"
```

#### Python:
```python
from cendalytics import VectorSpaceAPI

api = VectorSpaceAPI(is_debug=True)
reader = api.read(
    expand=False,
    library_name="SUPPLY_TAG_20191025_TFIDF.csv")

reader.process(key_field="0593B3744")
```

### Output
The function will return a DataFrame with the `top-N` most discriminating skills for this individual.

#### Sample Output
```text
+----+-------------+--------+-------------------------+
|    | Expansion   |   Rank | Tag                     |
|----+-------------+--------+-------------------------|
|  0 |             |      1 | bachelor of engineering |
|  1 |             |      2 | new employee            |
|  2 |             |      3 | model                   |
+----+-------------+--------+-------------------------+
```

## Invert
Invert a Vector Space.  This function will create a DataFrame keyed by terms (e.g., the skills).  Each term (skill) will be associated to a list of documents (e.g., serial numbers) for whom this term is the most discriminating.  So for example, after inversion is complete, it would be possible to list individuals for whom __Data Science__ would be the most discriminating skill.

### Usage
#### Shell
```shell script
source admin.sh vectorspace invert "${LIBRARY}" "${TOP_N}"
source admin.sh vectorspace invert "SUPPLY_TAG_20191025_TFIDF.csv" 3
```

#### Python
```python
from cendalytics import VectorSpaceAPI

api = VectorSpaceAPI(is_debug=True)
inversion = api.invert(
    library_name="SUPPLY_TAG_20191025_TFIDF.csv")

inversion.process(top_n=3)
```

### Output
This function will generate a DataFrame (persisted as a CSV file) that looks like this:

#### Sample Output
```text
+------+------------+-------------------------------------------+
|      | KeyField   | Tag                                       |
|------+------------+-------------------------------------------|
|    0 | 0697A5744  | cloud service                             |
|    1 | 0697A5744  | data science                              |
|    2 | 0697A5744  | solution design                           |
|    3 | 05817Q744  | kubernetes                                |
|    4 | 05817Q744  | bachelor of engineering                   |
|    5 | 05817Q744  | developer                                 |
...
| 1328 | 249045760  | electrical engineering                    |
+------+------------+-------------------------------------------+
```

## Read Inversion
Read the Inversion Library created in the prior step.  Given a term (e.g., a Skill) this function will return results (e.g., Serial Numbers) for which this term is highly discriminating

### Usage
#### Shell
```shell script
source admin.sh vectorspace read_inversion <library-name> <term>
source admin.sh vectorspace read_inversion <SUPPLY_TAG_20191025_TFIDF_INVERSION.csv> "Anthropology"
```

### Python
```python
from cendalytics import VectorSpaceAPI

api = VectorSpaceAPI(is_debug=True)
inversion = api.read_inversion(
    library_name="SUPPLY_TAG_20191025_TFIDF.csv")

inversion.process(term="Anthropology")
```

### Output
The output is a list of key fields (e.g., Serial Numbers) that correspond to the input term (e.g., skill)

#### Sample Output
```text
TBA
```


Reference:
1. Vector Space comment in Ontology Inversion Task: <br />https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15727069
