# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from collections import namedtuple

from iceberg.api import Schema
from iceberg.api.types import FloatType, IntegerType, LongType, NestedField, StringType
import pyarrow as pa
import pytest


TestRowGroupColumnStatistics = namedtuple("TestRowGroupColumnStatistics", ["min", "max", "null_count"])
TestRowGroupColumn = namedtuple("TestRowGroupColumn", ["path_in_schema",
                                                       "file_offset",
                                                       "total_compressed_size",
                                                       "statistics"])


class TestArrowParquetMetadata:
    __test__ = False

    def __init__(self, col_metadata, num_rows=100):
        self._col_metadata = col_metadata
        self._num_rows = num_rows

    @property
    def num_rows(self):
        return self._num_rows

    @property
    def num_columns(self):
        return len(self._col_metadata)

    def column(self, i):
        return self._col_metadata[i]

    def __getitem__(self, index):
        return self._col_metadata[index]


@pytest.fixture(scope="session")
def pyarrow_array():
    return [pa.array([1, 2, 3, None, 5], type=pa.int32()),
            pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
            pa.array([[0], [1, 2], [1], [1, 2, 3], None], type=pa.list_(pa.int64())),
            pa.array([True, None, False, True, True], pa.bool_())]


@pytest.fixture(scope="session")
def pytable_colnames():
    return ['int_col', 'str_col', 'list_col', 'bool_col']


@pytest.fixture(scope="session")
def rg_expected_schema():
    return Schema([NestedField.required(1, "string_col", StringType.get()),
                   NestedField.required(2, "long_col", LongType.get()),
                   NestedField.required(3, "int_col", IntegerType.get()),
                   NestedField.optional(4, "float_col", FloatType.get()),
                   NestedField.optional(5, "null_col", StringType.get()),
                   NestedField.optional(6, "missing_col", StringType.get()),
                   NestedField.optional(7, "no_stats_col", StringType.get())])


@pytest.fixture(scope="session")
def rg_expected_schema_map():
    return {"string_col": "string_col",
            "long_col": "long_col",
            "int_col": "int_col",
            "float_col_renamed": "float_col",
            "null_col": "null_col",
            "no_stats_col": "no_stats_col"}


@pytest.fixture(scope="session")
def rg_col_metadata():
    return [TestRowGroupColumn("string_col", 4, 12345, TestRowGroupColumnStatistics("b", "e", 0)),
            TestRowGroupColumn("long_col", 12349, 12345, TestRowGroupColumnStatistics(0, 1234567890123, 0)),
            TestRowGroupColumn("int_col", 24698, 12345, TestRowGroupColumnStatistics(0, 12345, 0)),
            TestRowGroupColumn("float_col_renamed", 37043, 12345, TestRowGroupColumnStatistics(0.0, 123.45, 123)),
            TestRowGroupColumn("null_col", 49388, 4, TestRowGroupColumnStatistics(None, None, 100)),
            TestRowGroupColumn("no_stats_col", 61733, 4, None)]
