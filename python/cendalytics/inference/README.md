# Inference
The Cendant Ontology is a semantic graph.  Nodes are connected by edges.  Edges have semantic value and can be used in inference. 

This API exposes multiple functions for creating sub-graphs from Cendant.  A sub-graph can be used to demonstrate the skills of an individual.  Or the concepts that are related to a term.  A sub-graph may be small and readily consumable within a Jupyter notebook, rendering within 1-2 seconds.  Or a sub-graph may be extremely large, taking several hours to render.  

This API will attempt to expose functions that are fit for purpose for:
1. Displaying a Skills Sub-Graph in Jupyter
2. Displaying a Query Contract used in Search
3. Finding all Relations for a given Term 

References:
1. [GitHub Issue Traceability #1230](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1230)
2. [Ontologies and the Semantic Web - SlideShare.net](https://www.slideshare.net/CraigTrim/ontologies-and-the-semantic-web)