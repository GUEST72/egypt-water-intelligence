# Offline evaluation and exploration

These scripts are retained analysis tools. Run them from the repository root and read their input constants before use. They do not define the Airflow pipeline or the production Spark transformations.

| Script | Purpose / expected inputs |
| --- | --- |
| `evaluate_alerts.py` | CLI comparison of exported alerts with a selected run's restricted scenario/fault truth |
| `check_district_pipeline.py` | Compare district deliveries with `/tmp/clean.ndjson` and `/tmp/features.ndjson` |
| `check_relative_order.py` | Check time ordering in a relative-topic export passed as a positional path |
| `explore_districts.py` | Explore/plot the historical run's district deliveries |
| `find_anomalies.py` | Explore relative flow/pressure in the historical district run |
| `threshold_study.py` | Study thresholds using `/tmp/features.ndjson` and an optional numeric threshold argument |
| `water_balance.py` | Explore aligned district/meter volumes and previously derived anomaly-window CSVs |

Several scripts still reference the existing historical dataset `run-bdc85c43085c4dc8`. Select the actual intended run before using them on another dataset. Pandas and Matplotlib are required by the scripts that import them; the generator development environment already brings those dependencies through WNTR. The standalone evaluator uses the Python standard library.

The alert case generators/checkers now live in [flink/tests/](../flink/README.md), alongside their stream-processing component. Use the [complete usage guide](../docs/USER_GUIDE.md#12-evaluate-alerts-and-use-analysis-tools) for alert export and evaluation commands. The [injected-pattern report](../docs/reports/injected-patterns.md) supplies scenario-analysis context.

Generated CSVs and plots in this folder are ignored. Keep restricted truth and generated datasets out of Git, and use a separate Compose project for manual streaming experiments.
