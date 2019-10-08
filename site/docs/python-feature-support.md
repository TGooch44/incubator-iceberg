# Feature Support

The goal is that the python library will provide a functional, performant subset of the java library. The initial focus has been on reading table metadata as well as providing the capability to both plan and execute a scan.

## Feature Comparison

### Metadata

|Operation                | Java    | Python    |
|:------------------------|:--------|:----------|
| Get Schema              | &#9745; |  &#9745;  |
| Get Snapshots           | &#9745; |  &#9745;  |
| Update current snapshot | &#9745; |           |

### Read Support

Read support is in the process of being implemented. Pyarrow is used for reading parquet files, so read support is limited to what is currently supported in the pyarrow.parquet package.

There is a [gap](https://issues.apache.org/jira/browse/ARROW-1644) in the current implementation that nested fields are only supported if they are:
> all repeated all repeated (lists) or all groups (structs) vs. a mix (structs and lists/repeated fields) then we can read and write them(otherwise we cannot)

#### Primitive Types


| Data Type               | Java    | Python |
|:------------------------|:--------|:-------|
| BooleanType             | &#9745; | &#9745;|
| DateType                | &#9745; | &#9745;|
| DecimalType             | &#9745; | &#9745;|
| FloatType               | &#9745; | &#9745;|
| IntegerType             | &#9745; | &#9745;|
| LongType                | &#9745; | &#9745;|
| TimeType                | &#9745; | &#9745;|
| TimestampType           | &#9745; | &#9745;|

#### Nested Types

| Data Type               | Java | Python |
|:------------------------|:------|:------|
| ListType of primitives  |&#9745;|&#9745;|
| MapType of primitives   |&#9745;|&#9745;|
| StructType of primitives|&#9745;|&#9745;|
| ListType of Nested Types|&#9745;|&#9745;|
| MapType of Nested Types |&#9745;|&#9745;|