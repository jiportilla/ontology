# Purpose
Parse the Python Cendant codebase.

## Implementation 

It is, of course, possible to parse a formal language using ANTLR.  ANTLR will generate an entire, and formal, AST (Abstract Syntax Tree).  This will include everything.  ANTLR (and similar parser compilers) are highly complex and brittle.

This package is an attempt to by pass the need to work with formal ASTs.  The service in this package will create a dependency tree analysis of python code by examining imports, definitions and class structures.  This may be sufficient for our needs.

## Reference
1. [GIT-1534: Formal Language Parse Epic](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1534)
  