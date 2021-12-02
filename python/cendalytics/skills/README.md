## Skills Report API
This API exposes multiple methods for finding Cendant data.

#### 1. Self-Reported Certifications <br />
`self_reported_certifications(collection_name, exclude_vendors=[...]) -> DataFrame`<br />
Returns a pandas DataFrame containing a list of self-reported certifications.<br />
This functionality is described in the Github epic [Self Certification Analysis](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/622)
![DataFlow](https://media.github.ibm.com/user/19195/files/eb868e00-b84c-11e9-95d8-6a4e539b5cbb)<br /><br >
References:<br />
1.  [GIT-1832: Automate Process](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1832)<br />
The process needs to be updated.  The DB2 writer is a manual invocation / part of Jupyter.
2.  [GIT-1833: Ontology Defect](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1833)<br />
Describes a defect in reporting that occured when Certification ownership was not set.    

#### 2. Text Normalize<br />
`normalize(text) -> str`<br />
Normalizes any unstructured text.  Normalization refers to the process of replacing terms and phrases with known synonyms and performing other pattern-reducing pre-processing activities.<br />

#### 3. Language Variability Generator<br />
`variants(text) -> list`<br />
Generates a list of known variations (including synonyms) for a given phrase or string.

#### 4. Graphviz<br />
`graphviz(DataFrame, engine=<RENDER>)`<br />
Renders a DataFrame produced by `OntologyAPI.relationships(...)`.  The _engine_ attribute can be any known graphviz rendering algorithm but is typically **dot** (for hierarchal graphs) or **fdp** (organic layouts).

#### 5. Search<br />
`search(search_terms=[...], div_field=<DIVISION>) -> DataFrame`<br />
Performs a full-text search on MongoDB given a list of one-or-more search terms.  The search can be optionally restricted to a division (**GBS** or **GTS**).  

#### 6. To CSV<br />
`to_csv(DataFrame, hash_serial_number)`<br />
Writes a Search DataFrame to CSV.  The _hash_serial_number_ parameter is a boolean (default is **True**) and will hash the serial numbers prior to writing to file.

#### 7. Hash Serial Number<br />
`hash_serial_number(DataFrame, column_name=<NAME>)`<br />
Given a DataFrame, apply the hash functionality to a given column.  Return the same DataFrame.  Doesn't have to be just serial numbers, but that's the only hash requirement our team has had.

