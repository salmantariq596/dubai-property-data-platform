"""Data quality between the layers.

* :mod:`dubai_property.quality.checks` defines every check as plain data (SQL
  plus WARN and FAIL thresholds). No Spark, so the rules can be reviewed and
  unit-tested anywhere.
* :mod:`dubai_property.quality.runner` runs a stage's checks, writes them to
  ``dq_results``, and raises ``DataQualityError`` on FAIL.

Row-level problems are handled earlier, in silver, by quarantining the row.
"""
