# Purpose
Extracting the precise (x,y) substring for an entity match in text.

This is more complex than just doing
```python
entity = "Natural Killer Cell"
input_string = "The natural killer cell is regenerated ..."

x = input_string.index(entity.lower())
y = x + len(entity)
```

because of the Language Variability preprocessing that occurs.

Consider a case where an input string has several tokens modified, merged, swapped and combined.  The normalized form of the input string will look nothing like the original form.  The normalized form is used to find entity matches.

Also consider the impact of long distance matches.

Consider this language variability entry for the entity `"Memory T Cell"`:
```text
memory_t_cell~memory+t_cell
t_cell~t+cell
cell~cells,celluar,cd88
```

The input string:
```text
With respect to the memory of cells (t variety) it is known ...
```

This will become normalized to:

```text
with respect to the memory_t_cell variety it is known
```

and it is trivial for the system to extract the `"Memory T Cell"` entity.

It is also trivial to find the (x,y) coordinates of an entity in the normalized form of the input string.

This service provides the less straightforward service of finding the (x,y) coordinates of the entity in the original input string.

This service will perform that service, and extract the (x,y) coordinates of an entity within the original input string.

For the given example, the solution becomes:
```json
{   "input": "With respect to the memory of celluar organisms (of the t variety) it is known ..."
    "entity": "Memory T Cell",
    "x": 20, "y": 38,
    "substring": "memory of celluar organisms (of the t" }
```

## Rationale
The (x,y) coordinates are used for Displacy Visualizations in Jupyter.

Displacy Example:
![Displacy Exapmle](https://media.github.ibm.com/user/19195/files/29f81d80-1ebd-11ea-9514-207ea5781641 "Displacy Entity Visualization")

The displacy toolkit requires precise (x,y) coordinate information.

## Further Reading
View the test case named `test_perform_coordinate_extraction`.  The association of input to expected output will likely help clarify the intent considerably. 

## References:
1. [GIT-1722: Extract Entity Match Coordinates to assist Displacy Visualizer](https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1722)