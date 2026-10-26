"""Minimal check that a Python table function (UDTF) taking an ARRAY runs inside our Flink image.

Same shape as unpivot_districts in job3b_district_alerts.py: ARRAY[...] input with a NULL
element, LATERAL TABLE join, several output columns. Bounded generated source and a print sink,
so the job finishes by itself and the rows appear in the TaskManager log.

Expected output: 4 rows, two per source row: D001 with a value between 2 and 4 and
D002 with a value between 6 and 8 (the NULL third element is skipped, so there is no D003).
"""

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udtf


@udtf(result_types=[DataTypes.STRING(), DataTypes.DOUBLE()])
def unpivot_test(values):
    for i in range(len(values)):
        if values[i] is None:
            continue
        yield ("D%03d" % (i + 1), values[i] * 2)


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.create_temporary_function("unpivot_test", unpivot_test)
    t_env.execute_sql("""
    CREATE TABLE out_udtf (k STRING, v DOUBLE)
    WITH ('connector' = 'print', 'print-identifier' = 'UDTF_TEST')
    """)
    # A bounded generated source is used instead of VALUES: the planner folds a constant
    # ARRAY[...] of literals and fails before the job starts, which is not what the real job does.
    t_env.execute_sql("""
    CREATE TABLE src (a DOUBLE, b DOUBLE) WITH (
        'connector' = 'datagen', 'number-of-rows' = '2', 'rows-per-second' = '10',
        'fields.a.min' = '1', 'fields.a.max' = '2',
        'fields.b.min' = '3', 'fields.b.max' = '4'
    )
    """)
    t_env.execute_sql("""
    INSERT INTO out_udtf
    SELECT t.k, t.v
    FROM (SELECT a, b, CASE WHEN a > 100 THEN a END AS c FROM src) AS s,
         LATERAL TABLE(unpivot_test(ARRAY[s.a, s.b, s.c])) AS t(k, v)
    """).wait()


if __name__ == "__main__":
    main()
