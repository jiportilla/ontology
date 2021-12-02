## Manifest Assembly API

Data-Driven Manifests that specify how to "assemble" multiple "ingest_<name>" collections to combine into a single collection.

For example, these three individual collections
1. ingest_cv_profile
2. ingest_cv_skills
3. ingest_cv_history

can be assembled into a single collection called
1. cv_data

The manifest will specify 
1. The source collections
2. The columns from each source ecollection to use
3. Any transformations to perform on the data in those source columns
4. The name of the target collection
5. Any renaming of the source columns into target columns