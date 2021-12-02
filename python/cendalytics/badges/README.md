# Badge Entity Analysis
The Badge DataSet contains tags that we are interested in extracting.

These tags provide additional inference capability at runtime.

For example, the badge `Selling IoT - Level 1 Program 2017 Version 1` has the following tags: [`Data Analyst`, `Internet of Things`, `Position`, `Software as a Service`].  

Here's a view of the Badge Data with the associated tags
![Badge Data](https://media.github.ibm.com/user/19195/files/e2609400-bfa5-11e9-92ca-4ed0ac38ef6c)

Note that we take our data from DB2 now, but the Excel screenshot conveys the purpose. The green highlighted cells are the tags that we are after. Each tag in this dataset has an equivalent entry in Cendant, making it easy for us to perform a level of inference for each badge that would not be otherwise obvious.

This analytics service performs a tag extraction and badge association routine, and writes the results to the MongoDB cendant collection **analytics_badges_tags**.  This collection is used when the annotation model runs to retrieve skill-based tags for each badge.

Data Flow:
![Data Flow](https://media.github.ibm.com/user/19195/files/28ec8d80-f1a5-11e9-8055-7886601fa91b)

Referenced Issues:
1. [GitHub #767: Annotation Model Defect](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/767) _the investigation that triggered this process refactoring_
2. [GitHub #94: Original Design](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/97) _this issue has very little detail_
3. [GitHub #768: Badge Analytics Refactoring](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/768) _the refactoring that resulted in the code being placed here_
4. [GitHub #886: Badge Distributions and DataFlow](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/886#issuecomment-)
5. [GitHub #1549: Further Questions and Documentation](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1549) handles 