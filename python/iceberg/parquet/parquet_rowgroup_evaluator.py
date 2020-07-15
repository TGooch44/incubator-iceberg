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

from iceberg.api.expressions import Binder, Expressions, ExpressionVisitors


class ParquetRowgroupEvaluator(object):
    """
    Handles Reading a parquet file for a given start to end range.  Also, enforces
    the projected iceberg schema on the read data.  This may involve selectively reading
    columns, remapping column names, re-arranging the projection ordering, and possibly
    adding null columns

    Parameters
    ----------
    schema : iceberg.api.Schema
        An iceberg schema to use for binding the predicate
    unbound : iceberg.api.expressions.UnboundPredicate
        The unbound predicate to evaluate
    start : int
        The start index of the assigned reader
    end : int
        The end-index of the assigned reader
    """
    def __init__(self, schema, field_name_map, unbound, start, end):
        self.schema = schema
        self.struct = schema.as_struct()
        self.field_name_map = field_name_map

        self.expr = None if unbound is None else Binder.bind(self.struct, Expressions.rewrite_not(unbound))
        self.start = start
        self.end = end
        self._visitors = None

    def _visitor(self):
        if self._visitors is None:
            self._visitors = ParquetRowgroupEvalVisitor(self.expr, self.schema, self.field_name_map,
                                                        self.struct, self.start, self.end)

        return self._visitors

    def eval(self, row_group):
        return self._visitor().eval(row_group)


class ParquetRowgroupEvalVisitor(ExpressionVisitors.BoundExpressionVisitor):
    ROWS_MIGHT_MATCH = True
    ROWS_CANNOT_MATCH = False

    def __init__(self, expr, schema, field_name_map, struct, start, end):
        self.expr = expr
        self.schema = schema
        self.field_name_map = field_name_map
        self.struct = struct
        self.start = start
        self.end = end
        self.lower_bounds = None
        self.upper_bounds = None
        self.nulls = None
        self.num_rows = None
        self.midpoint = None
        self.parquet_cols = None

    def eval(self, row_group):
        """
        Returns a boolean that determines if the given row-group may contain rows
        for the assigned predicate and read-range[start, end]

        Parameters
        ----------
        row_group : pyarrow._parquet.RowGroupMetaData
            The pyarrow parquet row-group metadata being evaluated
        Returns
        -------
        boolean
            True if rows for the current evaluator might exist in the row group, false otherwise
        """
        if row_group.num_rows <= 0:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH
        self.num_rows = row_group.num_rows
        self.get_stats(row_group)

        # if the mid-point of the row-group is not contained by the
        # start-end range we don't read it
        if self.start is not None and self.end is not None \
                and not (self.start <= self.midpoint <= self.end):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        if self.expr is None:
            return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

        return ExpressionVisitors.visit(self.expr, self)

    def always_true(self):
        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def always_false(self):
        return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

    def not_(self, result):
        return not result

    def and_(self, left_result, right_result):
        return left_result and right_result

    def or_(self, left_result, right_result):
        return left_result or right_result

    def is_null(self, ref):
        id = ref.field_id
        field = self.struct.field(id=id)
        if field is not None and self.nulls.get(id, 1) == 0:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def not_null(self, ref):
        id = ref.field_id
        field = self.struct.field(id=id)

        # if the field is missing from schema evolution or all rows in the row group are null
        if id not in self.parquet_cols or self.nulls.get(id, -1) == self.num_rows:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def lt(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(id))

        if self.lower_bounds is not None and id in self.lower_bounds:
            if self.lower_bounds.get(id) >= lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def lt_eq(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(id))

        if self.lower_bounds is not None and id in self.lower_bounds:
            if self.lower_bounds.get(id) > lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def gt(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(id))

        if self.upper_bounds is not None and id in self.upper_bounds:
            if self.upper_bounds.get(id) <= lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def gt_eq(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(id))

        if self.upper_bounds is not None and id in self.upper_bounds:
            if self.upper_bounds.get(id) < lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def eq(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(id))

        if self.lower_bounds is not None and id in self.lower_bounds:
            if self.lower_bounds.get(id) > lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        if self.upper_bounds is not None and id in self.upper_bounds:
            if self.upper_bounds.get(id) < lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def not_eq(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH
        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def in_(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH
        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def not_in(self, ref, lit):
        id = ref.field_id
        if id not in self.parquet_cols:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def get_stats(self, row_group):
        """
        Summarizes the row group statistics for upper and lower bounds.  Also calculates
        the mid-point of the row-group for determining start <= midpoint <= end

        Parameters
        ----------
        row_group : pyarrow._parquet.RowGroupMetaData
            The pyarrow parquet row-group metadata being evaluated
        """

        self.lower_bounds = {}
        self.upper_bounds = {}
        self.nulls = {}
        self.parquet_cols = []

        start = -1
        size = 0

        for i in range(row_group.num_columns):
            column = row_group.column(i)

            if start < 0:
                start = column.file_offset
            size += column.total_compressed_size
            id = self.schema.lazy_name_to_id().get(self.field_name_map.get(column.path_in_schema))

            if id is not None:
                self.parquet_cols.append(id)
                if column.statistics is not None:
                    if column.statistics.min is not None:
                        self.lower_bounds[id] = column.statistics.min
                    if column.statistics.max is not None:
                        self.upper_bounds[id] = column.statistics.max
                    if column.statistics.null_count is not None:
                        self.nulls[id] = column.statistics.null_count

        self.midpoint = int(size / 2 + start)
