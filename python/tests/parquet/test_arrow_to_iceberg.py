
from iceberg.api import Schema
from iceberg.api.types import (BinaryType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               ListType,
                               LongType,
                               MapType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType)
from iceberg.parquet.parquet_to_iceberg import arrow_to_iceberg
import pyarrow as pa


def schema_equals(base, other):
    for field in base.struct.fields:
        assert field == other.find_field(field.id)

    return True


def test_convert_primitives():
    expected_iceberg_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                                      NestedField.required(2, "long_col", LongType.get()),
                                      NestedField.required(3, "float_col", FloatType.get()),
                                      NestedField.required(4, "double_col", DoubleType.get()),
                                      NestedField.required(5, "decimal_col", DecimalType.of(38, 5)),
                                      NestedField.required(6, "date_col", DateType.get()),
                                      NestedField.required(7, "string_col", StringType.get()),
                                      NestedField.required(8, "ts_col", TimestampType.without_timezone()),
                                      NestedField.required(9, "ts_w_tz_col", TimestampType.with_timezone()),
                                      NestedField.required(10, "bin_col", BinaryType.get()),
                                      NestedField.required(11, "fixed_bin_col", FixedType.of_length(10))
                                      ])

    arrow_schema = pa.schema([pa.field("int_col", pa.int32(), False, {b"PARQUET:field_id": b"1"}),
                              pa.field("long_col", pa.int64(), False, {b"PARQUET:field_id": b"2"}),
                              pa.field("float_col", pa.float32(), False, {b"PARQUET:field_id": b"3"}),
                              pa.field("double_col", pa.float64(), False, {b"PARQUET:field_id": b"4"}),
                              pa.field("decimal_col", pa.decimal128(38, 5), False, {b"PARQUET:field_id": b"5"}),
                              pa.field("date_col", pa.date32(), False, {b"PARQUET:field_id": b"6"}),
                              pa.field("string_col", pa.string(), False, {b"PARQUET:field_id": b"7"}),
                              pa.field("ts_col", pa.timestamp("us"), False, {b"PARQUET:field_id": b"8"}),
                              pa.field("ts_w_tz_col", pa.timestamp("us", "America/New_York"), False,
                                       {b"PARQUET:field_id": b"9"}),
                              pa.field("bin_col", pa.binary(), False, {b"PARQUET:field_id": b"10"}),
                              pa.field("fixed_bin_col", pa.binary(10), False, {b"PARQUET:field_id": b"11"})
                              ])
    converted = arrow_to_iceberg(arrow_schema)
    assert schema_equals(expected_iceberg_schema, converted)


def test_convert_struct_type():
    struct_type = StructType.of([NestedField.required(2, "a", IntegerType.get()),
                                 NestedField.required(3, "b", IntegerType.get())])
    expected_iceberg_schema = Schema([NestedField.required(1, "struct_col", struct_type)])

    arrow_struct = pa.struct([pa.field("a", pa.int32(), False, {b"PARQUET:field_id": b"2"}),
                              pa.field("b", pa.int32(), False, {b"PARQUET:field_id": b"3"})])
    arrow_schema = pa.schema([pa.field("struct_col", arrow_struct, False, {b"PARQUET:field_id": b"1"})])

    converted = arrow_to_iceberg(arrow_schema)
    assert schema_equals(expected_iceberg_schema, converted)


def test_convert_inferred_map():
    # arrow reads parquet map type as:
    # struct<map: list<map: struct<key: $key_type not null, value: $value_type> not null> not null>
    map_type = MapType.of_required(2, 3, IntegerType.get(), StringType.get())
    expected_iceberg_schema = Schema([NestedField.required(1, "map_col", map_type)])

    key_field = pa.field("key", pa.int32(), False, {b"PARQUET:field_id": b"2"})
    value_field = pa.field("value", pa.string(), False, {b"PARQUET:field_id": b"3"})

    arrow_struct = pa.struct([pa.field("map", pa.list_(pa.struct([key_field, value_field])), False)])
    arrow_schema = pa.schema([pa.field("map_col", arrow_struct, False, {b"PARQUET:field_id": b"1"})])
    converted = arrow_to_iceberg(arrow_schema)

    assert schema_equals(expected_iceberg_schema, converted)


def test_convert_list():
    list_type = ListType.of_required("2", IntegerType.get())
    expected_iceberg_schema = Schema([NestedField.required(1, "list_col", list_type)])

    element_field = pa.field("element", pa.int32(), False, {b"PARQUET:field_id": b"2"})
    arrow_schema = pa.schema([pa.field("list_col", pa.list_(element_field), False, {b"PARQUET:field_id": b"1"})])
    converted = arrow_to_iceberg(arrow_schema)

    assert schema_equals(expected_iceberg_schema, converted)


def test_primitive_nullabilty():
    expected_iceberg_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                                      NestedField.optional(2, "long_col", LongType.get())])

    arrow_schema = pa.schema([pa.field("int_col", pa.int32(), False, {b"PARQUET:field_id": b"1"}),
                              pa.field("long_col", pa.int64(), True, {b"PARQUET:field_id": b"2"})])

    converted = arrow_to_iceberg(arrow_schema)
    assert schema_equals(expected_iceberg_schema, converted)


def test_nested_nullabilty():
    struct_type = StructType.of([NestedField.required(3, "a", IntegerType.get()),
                                 NestedField.optional(4, "b", IntegerType.get())])
    list_type = ListType.of_optional(5, IntegerType.get())
    expected_iceberg_schema = Schema([NestedField.optional(1, "struct_col", struct_type),
                                      NestedField.required(2, "list_col", list_type)])

    element_field = pa.field("element", pa.int32(), True, {b"PARQUET:field_id": b"5"})
    arrow_struct = pa.struct([pa.field("a", pa.int32(), False, {b"PARQUET:field_id": b"3"}),
                              pa.field("b", pa.int32(), True, {b"PARQUET:field_id": b"4"}), ])
    arrow_schema = pa.schema([pa.field("struct_col", arrow_struct, True, {b"PARQUET:field_id": b"1"}),
                              pa.field("list_col", pa.list_(element_field), False, {b"PARQUET:field_id": b"2"})])

    converted = arrow_to_iceberg(arrow_schema)
    assert schema_equals(expected_iceberg_schema, converted)
