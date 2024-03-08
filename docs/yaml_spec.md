# Data Format YAML Specification

# Global
```yaml
county-name:
    String

county-path:
    Absolute path to folder containing county data

var-map-path:
    Absolute path to file containing variable mapping file

operations:
    set of mappings (operations)

output:
    set of mappings (output type and destination)
```

# Operations
```yaml
{operation-name}
    type: {append, merge}
    files:
        {file1-pattern}: {format-file1}
        ...
    key:
        - {merge-key-col}
    join-type: {left, right, inner, outer, cross}
...
```
Note: make sure key columns are the new column names, not the original ones

# Output
```yaml
output:
    path: {absolute-output-path}
    formats:
        - {csv, parquet}
        - ...
```
