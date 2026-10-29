"""Checks that the relative topic dump is ordered by window_start.
Usage: python flink/analysis/check_relative_order.py /tmp/relative.ndjson
Expected after the job3b fix: steps backwards = 0."""
import json, sys
import pandas as pd

rows = [json.loads(l) for l in open(sys.argv[1]) if l.startswith('{')]
df = pd.DataFrame(rows)
df['ts'] = pd.to_datetime(df['window_start'])
back = int((df['ts'].diff() < pd.Timedelta(0)).sum())
print('rows:', len(df), '| steps backwards in time:', back)
print('RESULT:', 'PASS' if back == 0 else 'FAIL')
