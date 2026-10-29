"""Minimal check that Python UDFs run inside our Flink image.

UDF_TEST=scalar -> scalar UDF median over six columns (pivoted-row approach)
UDF_TEST=udaf   -> aggregate UDF median over rows of one group

Both jobs use a tiny bounded VALUES source and a print sink, so they finish by themselves.
"""

import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import AggregateFunction, udaf, udf


@udf(result_type=DataTypes.DOUBLE())
def median_of(a, b, c, d, e, f):
    # Median of the non-null values among six inputs.
    vals = sorted(v for v in (a, b, c, d, e, f) if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


class MedianAgg(AggregateFunction):
    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, value):
        if value is not None:
            accumulator.append(value)

    def get_value(self, accumulator):
        if not accumulator:
            return None
        s = sorted(accumulator)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2

    def get_result_type(self):
        return DataTypes.DOUBLE()

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.DOUBLE())


def main():
    mode = os.environ.get("UDF_TEST", "scalar")
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    if mode == "scalar":
        t_env.create_temporary_function("median_of", median_of)
        t_env.execute_sql("""
        CREATE TABLE out_scalar (m DOUBLE)
        WITH ('connector' = 'print', 'print-identifier' = 'SCALAR_UDF')
        """)
        # Row 1: six values, expected median 1.25E-3. Row 2: one NULL, expected median 1.2E-3.
        t_env.execute_sql("""
        INSERT INTO out_scalar
        SELECT median_of(f1, f2, f3, f4, f5, f6)
        FROM (VALUES
            (1.0E-3, 1.1E-3, 1.2E-3, 1.3E-3, 1.4E-3, CAST(3.0E-3 AS DOUBLE)),
            (1.0E-3, 1.1E-3, 1.2E-3, 1.3E-3, 1.4E-3, CAST(NULL AS DOUBLE))
        ) AS t(f1, f2, f3, f4, f5, f6)
        """)
    else:
        t_env.create_temporary_function("median_agg", udaf(MedianAgg()))
        t_env.execute_sql("""
        CREATE TABLE out_udaf (k STRING, m DOUBLE)
        WITH ('connector' = 'print', 'print-identifier' = 'UDAF')
        """)
        # Group w1: expected median 1.25E-3. Group w2: expected median 2.0.
        t_env.execute_sql("""
        INSERT INTO out_udaf
        SELECT k, median_agg(v)
        FROM (VALUES
            ('w1', 1.0E-3), ('w1', 1.1E-3), ('w1', 1.2E-3),
            ('w1', 1.3E-3), ('w1', 1.4E-3), ('w1', 3.0E-3),
            ('w2', 1.0E0), ('w2', 2.0E0), ('w2', 3.0E0)
        ) AS t(k, v)
        GROUP BY k
        """)


if __name__ == "__main__":
    main()
