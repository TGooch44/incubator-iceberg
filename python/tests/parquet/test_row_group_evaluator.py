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

import pytest

from iceberg.api.expressions import Expressions
from iceberg.parquet import ParquetRowgroupEvaluator

from .conftest import TestArrowParquetMetadata


@pytest.mark.parametrize("predicate, expression, lit_val, eval",
                         [("string_col", Expressions.equal, "b", True),
                          ("string_col", Expressions.equal, "z", False),
                          ("string_col", Expressions.greater_than_or_equal, "a", True),
                          ("string_col", Expressions.greater_than_or_equal, "e", True),
                          ("string_col", Expressions.greater_than_or_equal, "f", False),
                          ("string_col", Expressions.greater_than, "a", True),
                          ("string_col", Expressions.greater_than, "e", False),
                          ("string_col", Expressions.greater_than, "f", False),
                          ("string_col", Expressions.less_than, "a", False),
                          ("string_col", Expressions.less_than, "b", False),
                          ("string_col", Expressions.less_than, "c", True),
                          ("string_col", Expressions.less_than_or_equal, "a", False),
                          ("string_col", Expressions.less_than_or_equal, "b", True),
                          ("string_col", Expressions.less_than_or_equal, "c", True),
                          ("string_col", Expressions.is_null, None, False),
                          ("string_col", Expressions.not_null, None, True)
                          ])
def test_string_stats(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, lit_val, eval):
    if lit_val is None:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate), 0, 123456)
    else:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate, lit_val), 0, 123456)

    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval


@pytest.mark.parametrize("predicate, expression, lit_val, eval",
                         [("int_col", Expressions.equal, -1, False),
                          ("int_col", Expressions.equal, 10, True),
                          ("int_col", Expressions.equal, 1234567, False),
                          ("int_col", Expressions.greater_than_or_equal, -1, True),
                          ("int_col", Expressions.greater_than_or_equal, 12345, True),
                          ("int_col", Expressions.greater_than_or_equal, 12346, False),
                          ("int_col", Expressions.greater_than, -1, True),
                          ("int_col", Expressions.greater_than, 12344, True),
                          ("int_col", Expressions.greater_than, 12345, False),
                          ("int_col", Expressions.less_than, -1, False),
                          ("int_col", Expressions.less_than, 0, False),
                          ("int_col", Expressions.less_than, 2, True),
                          ("int_col", Expressions.less_than_or_equal, -1, False),
                          ("int_col", Expressions.less_than_or_equal, 0, True),
                          ("int_col", Expressions.less_than_or_equal, 2, True),
                          ("int_col", Expressions.is_null, None, False),
                          ("int_col", Expressions.not_null, None, True)
                          ])
def test_int_stats(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, lit_val, eval):
    if lit_val is None:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate), 0, 123456)
    else:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate, lit_val), 0, 123456)

    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval


@pytest.mark.parametrize("predicate, expression, lit_val, eval",
                         [("float_col", Expressions.equal, -1.0, False),
                          ("float_col", Expressions.equal, 10.0, True),
                          ("float_col", Expressions.equal, 1234.567, False),
                          ("float_col", Expressions.greater_than_or_equal, -1.0, True),
                          ("float_col", Expressions.greater_than_or_equal, 123.45, True),
                          ("float_col", Expressions.greater_than_or_equal, 1234.567, False),
                          ("float_col", Expressions.greater_than, -1.0, True),
                          ("float_col", Expressions.greater_than, 123.44, True),
                          ("float_col", Expressions.greater_than, 124.00, False),
                          ("float_col", Expressions.less_than, -1.0, False),
                          ("float_col", Expressions.less_than, 0.0, False),
                          ("float_col", Expressions.less_than, 2.0, True),
                          ("float_col", Expressions.less_than_or_equal, -1.0, False),
                          ("float_col", Expressions.less_than_or_equal, 0.0, True),
                          ("float_col", Expressions.less_than_or_equal, 2.0, True),
                          ("float_col", Expressions.is_null, None, True),
                          ("float_col", Expressions.not_null, None, True)
                          ])
def test_float_stats(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, lit_val, eval):
    if lit_val is None:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate), 0, 123456)
    else:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate, lit_val), 0, 123456)

    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval


@pytest.mark.parametrize("predicate, expression, lit_val, eval",
                         [("missing_col", Expressions.equal, "a", False),
                          ("missing_col", Expressions.not_equal, "a", False),
                          ("missing_col", Expressions.greater_than, "a", False),
                          ("missing_col", Expressions.greater_than_or_equal, "a", False),
                          ("missing_col", Expressions.less_than_or_equal, "a", False),
                          ("missing_col", Expressions.less_than, "a", False),
                          ("missing_col", Expressions.not_null, None, False),
                          ("missing_col", Expressions.is_null, None, True),
                          ("float_col", Expressions.greater_than, 100.00, True),
                          ("float_col", Expressions.greater_than, 200.00, False),
                          ("float_col", Expressions.is_null, None, True),
                          ("float_col", Expressions.not_null, None, True)
                          ])
def test_schema_evolution(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, lit_val, eval):
    if lit_val is None:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate), 0, 123456)
    else:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate, lit_val), 0, 123456)

    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval


@pytest.mark.parametrize("predicate, expression, eval", [("null_col", Expressions.is_null, True),
                                                         ("null_col", Expressions.not_null, False),
                                                         ("missing_col", Expressions.not_null, False),
                                                         ("missing_col", Expressions.is_null, True)])
def test_nullable_behavior(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, eval):
    rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                       expression(predicate), 0, 123456)
    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval


@pytest.mark.parametrize("predicate, expression, lit_val, eval",
                         [("no_stats_col", Expressions.equal, "a", True),
                          ("no_stats_col", Expressions.not_equal, "a", True),
                          ("no_stats_col", Expressions.greater_than, "a", True),
                          ("no_stats_col", Expressions.greater_than_or_equal, "a", True),
                          ("no_stats_col", Expressions.less_than_or_equal, "a", True),
                          ("no_stats_col", Expressions.less_than, "a", True),
                          ("no_stats_col", Expressions.not_null, None, True),
                          ("no_stats_col", Expressions.is_null, None, True)])
def test_missing_stats(rg_expected_schema, rg_expected_schema_map, rg_col_metadata, predicate, expression, lit_val, eval):
    if lit_val is None:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate), 0, 123456)
    else:
        rg_eval = ParquetRowgroupEvaluator(rg_expected_schema, rg_expected_schema_map,
                                           expression(predicate, lit_val), 0, 123456)

    assert rg_eval.eval(TestArrowParquetMetadata(rg_col_metadata)) == eval

