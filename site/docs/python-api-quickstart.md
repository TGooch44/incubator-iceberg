# Iceberg Python API

Much of the python api conforms to the java api. You can get more info about the java api [here](https://iceberg.apache.org/api/).


## Tables

The [Table interface](/pythondoc/iceberg.api.html#iceberg.api.table.Table) provides access to table metadata

+ schema returns the current table [schema](/pythondoc/iceberg.api.html#iceberg.api.Schema)
+ spec returns the current table [partition spec](/pythondoc/iceberg.api.html#iceberg.api.PartitionSpec)
+ properties returns a map of key-value properties
+ currentSnapshot returns the current table snapshot
+ snapshots returns all valid snapshots for the table
+ snapshot(id) returns a specific snapshot by ID
+ location returns the table’s base location

Tables also provide refresh to update the table to the latest version.

### Scanning
Iceberg table scans start by creating a TableScan object with newScan.

``` python
scan = table.new_scan();
```

To configure a scan, call filter and select on the TableScan to get a new TableScan with those changes.

``` python
filtered_scan = scan.filter(Expressions.equal("id", 5))
```

String expressions can also be passed to the filter method.

``` python
filtered_scan = scan.filter("id=5")
```

Schema projections can be applied against a TableScan by passing a list of column names.

``` python
filtered_scan = scan.select(["col_1", "col_2", "col_3"])
```

Because some data types cannot be read using the python library, a convenience method for excluding columns from projection is provided.

``` python
filtered_scan = scan.select_except(["unsupported_col_1", "unsupported_col_2"])
```


Calls to configuration methods create a new TableScan so that each TableScan is immutable.

When a scan is configured, planFiles, planTasks, and schema are used to return files, tasks, and the read projection.

``` python
scan = table.new_scan() \
    .filter("id=5") \
    .select(["id", "data"])

projection = scan.schema
for task in scan.plan_tasks():
    print(task)
```

## Types

Iceberg data types are located in [iceberg.api.types.types](/pythondoc/iceberg.api.types.html)

### Primitives

Primitive type instances are available from static methods in each type class. Types without parameters use get, and types like __decimal__ use factory methods:

```python
IntegerType.get()    # int
DoubleType.get()     # double
DecimalType.of(9, 2) # decimal(9, 2)
```

### Nested types
Structs, maps, and lists are created using factory methods in type classes.

Like struct fields, map keys or values and list elements are tracked as nested fields. Nested fields track [field IDs](https://iceberg.apache.org/evolution/#correctness) and nullability.

Struct fields are created using __NestedField.optional__ or __NestedField.required__. Map value and list element nullability is set in the map and list factory methods.

```python
# struct<1 id: int, 2 data: optional string>
struct = StructType.of([NestedField.required(1, "id", IntegerType.get()),
                        NestedField.optional(2, "data", StringType.get()])
  )
```
```python
# map<1 key: int, 2 value: optional string>
map_var = MapType.of_optional(1, IntegerType.get(),
                          2, StringType.get())
```
```python
# array<1 element: int>
list_var = ListType.of_required(1, IntegerType.get());
```

## Expressions
Iceberg’s expressions are used to configure table scans. To create expressions, use the factory methods in Expressions.

Supported predicate expressions are:

+ __is_null__
+ __not_ull__
+ __equal__
+ __not_equal__
+ __less_than__
+ __less_than_or_equal__
+ __greater_than__
+ __greater_than_or_equal__

Supported expression operations are:

+ __and__
+ __or__
+ __not__

Constant expressions are:

+ __always_true__
+ __always_false__
