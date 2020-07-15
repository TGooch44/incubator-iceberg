
from datetime import datetime
import decimal
import logging

from iceberg.api.expressions import Expressions
from iceberg.api.types import TypeID
from iceberg.core.util import SCAN_THREAD_POOL_ENABLED
from iceberg.core.util.profile import profile
import numpy as np
import pyarrow as pa
import pyarrow.lib as lib
import pyarrow.parquet as pq

try:
    from .arrow_table_utils import apply_iceberg_filter
except ImportError:
    from .gandiva_utils import apply_iceberg_filter

from .parquet_rowgroup_evaluator import ParquetRowgroupEvaluator
from .parquet_schema_utils import prune_columns
from .parquet_to_iceberg import convert_parquet_to_iceberg

_logger = logging.getLogger(__name__)

USE_RG_FILTERING_PROP = "use-row-group-filtering"

DTYPE_MAP = {TypeID.BINARY: lambda field: pa.binary(),
             TypeID.BOOLEAN: lambda field: (pa.bool_(), False),
             TypeID.DATE: lambda field: (pa.date32(), datetime.now()),
             TypeID.DECIMAL: lambda field: (pa.decimal128(field.type.precision, field.type.scale),
                                            decimal.Decimal()),
             TypeID.DOUBLE: lambda field: (pa.float64(), np.nan),
             TypeID.FIXED: lambda field: pa.binary(field.length()),
             TypeID.FLOAT: lambda field: (pa.float32(), np.nan),
             TypeID.INTEGER: lambda field: (pa.int32(), np.nan),
             TypeID.LIST: lambda field: (pa.list_(pa.field("element",
                                                           DTYPE_MAP.get(field.type.element_type.type_id)(field.type)[0])),
                                         None),
             TypeID.LONG: lambda field: (pa.int64(), np.nan),
             # pyarrow doesn't currently support reading parquet map types
             # TypeID.MAP: lambda field: (,),
             TypeID.STRING: lambda field: (pa.string(), ""),
             TypeID.STRUCT: lambda field: (pa.struct([(nested_field.name,
                                                       DTYPE_MAP.get(nested_field.type.type_id)(nested_field.type)[0])
                                                     for nested_field in field.type.fields]), {}),
             TypeID.TIMESTAMP: lambda field: (pa.timestamp("us"), datetime.now()),
             # TypeID.TIME: pa.time64(None)
             }


class ParquetReader(object):
    COMBINED_TASK_SIZE = 8 * 1048576

    def __init__(self, input, expected_schema, options, filter,
                 case_sensitive, start=None, end=None):
        self._input = input
        self._input_fo = input.new_fo()

        self._arrow_file = pq.ParquetFile(self._input_fo)
        self._file_schema = convert_parquet_to_iceberg(self._arrow_file)
        self._expected_schema = expected_schema
        self._file_to_expected_name_map = ParquetReader.get_field_map(self._file_schema,
                                                                      self._expected_schema)

        self._options = options
        self.use_threads = self._options.get(SCAN_THREAD_POOL_ENABLED, False)

        _logger.debug("Starting Parquet Reader with %s" %
                      "threaded scan enabled" if self.use_threads else "threaded scan disabled")
        _logger.debug("Reading %s" % self._input.path)

        self._filter = filter if filter != Expressions.always_true() else None
        self._case_sensitive = case_sensitive
        self.start = start
        self.end = end

        self.materialized_table = False
        self.curr_iterator = None
        self._table = None
        self._stats = dict()

        self._skip_schema_evol_processing = False
        if len(self.get_missing_fields()) == 0:
            rename_fields = False
            for file_field_name, expected_field_name in self._file_to_expected_name_map.items():
                if not file_field_name == expected_field_name:
                    rename_fields = True

            if not rename_fields:
                _logger.debug("Skipping processing for schema evolution")
                self._skip_schema_evol_processing = True

    def to_pandas(self):
        if not self.materialized_table:
            self._read_data()

        return self._table.to_pandas(use_threads=True)

    def to_arrow_table(self):
        if not self.materialized_table:
            self._read_data()

        return self._table

    def _read_data(self):
        _logger.debug("Starting data read")

        # only scan the columns projected and in our file
        cols_to_read = prune_columns(self._file_schema, self._expected_schema)

        if not self._options.get(USE_RG_FILTERING_PROP, True):
            _logger.debug("Reading Full file")

            arrow_tbls = [pq.ParquetFile(self._input.new_fo()).read(columns=cols_to_read,
                                                                    use_threads=self.use_threads)]

        else:
            _logger.debug("Using row group filtering")
            # filter out row groups that cannot contain our filter expression
            with profile("rg_filtering", self._stats):
                rg_evaluator = ParquetRowgroupEvaluator(self._expected_schema, self._filter,
                                                        self.start, self.end)
                read_row_groups = [i for i in range(self._arrow_file.num_row_groups)
                                   if rg_evaluator.eval(self._arrow_file.metadata.row_group(i))]

            if len(read_row_groups) == 0:
                self._table = None
                self.materialized_table = True
                return

            with profile("read_row_groups", self._stats):
                arrow_tbls = [self.read_rgs(i, cols_to_read) for i in read_row_groups]

        _logger.debug("data fetch done")

        # process columns apply any residual filters
        with profile("schema_evol_proc", self._stats):
            if not self._skip_schema_evol_processing:

                processed_tbl = ParquetReader.migrate_schema(self._file_to_expected_name_map,
                                                             lib.concat_tables(arrow_tbls))
                for i, field in self.get_missing_fields():
                    dtype_func = DTYPE_MAP.get(field.type.type_id)
                    if dtype_func is None:
                        raise RuntimeError("Unable to create null column for type %s" % field.type.type_id)

                    dtype = dtype_func(field)
                    processed_tbl = (processed_tbl.add_column(i,
                                                              pa.field(field.name, dtype[0], False, None),
                                                              ParquetReader.create_null_column(processed_tbl[0],
                                                                                               dtype)))

            else:
                processed_tbl = lib.concat_tables(arrow_tbls)

        self._table = processed_tbl
        self.materialized_table = True

    def read_rgs(self, i, cols_to_read):
        with self._input.new_fo() as thread_file:
            arrow_tbl = (pq.ParquetFile(thread_file)
                         .read_row_group(i, columns=cols_to_read))

        return apply_iceberg_filter(arrow_tbl,
                                    ParquetReader.get_reverse_field_map(self._file_schema,
                                                                        self._expected_schema),
                                    self._filter)

    def __iter__(self):
        return self

    def __next__(self):
        if self.curr_iterator is None:
            if not self.materialized_table:
                self.to_pandas()
            self.curr_iterator = self.df.itertuples()
        try:
            curr_row = next(self.curr_iterator)
        except StopIteration:
            self.curr_iterator = None
            raise

        return curr_row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _logger.debug(self._stats)
        self.close()

    def close(self):
        self._input_fo.close()

    def get_missing_fields(self):
        return [(i, field) for i, field in enumerate(self._expected_schema.as_struct().fields)
                if self._file_schema.find_field(field.id) is None]

    @staticmethod
    def get_field_map(file_schema, expected_schema):
        return {file_schema.find_field(field.id).name: field.name
                for field in expected_schema.as_struct().fields
                if file_schema.find_field(field.id) is not None}

    @staticmethod
    def get_reverse_field_map(file_schema, expected_schema):
        return {expected_schema.find_field(field.id).name: field.name
                for field in file_schema.as_struct().fields
                if expected_schema.find_field(field.id) is not None}

    @staticmethod
    def migrate_schema(mapping, table):
        for key, value in mapping.items():
            column_idx = table.schema.get_field_index(key)
            column_field = table.schema[column_idx]
            table = table.set_column(column_idx,
                                     pa.field(value, column_field.type, column_field.nullable, column_field.metadata),
                                     table[column_idx])

        return table

    @staticmethod
    def create_null_column(reference_column, dtype_tuple):
        dtype, init_val = dtype_tuple
        return pa.chunked_array([pa.array(np.full(len(c), init_val), type=dtype, mask=[True] * len(c))
                                 for c in reference_column.data.chunks], type=dtype)
