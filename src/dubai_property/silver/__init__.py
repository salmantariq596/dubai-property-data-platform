"""Silver layer: typed, validated, deduplicated records, loaded with MERGE.

* :mod:`dubai_property.silver.rules` holds every rule as plain data and needs
  no Spark, so it can be read and tested anywhere.
* :mod:`dubai_property.silver.transform` and :mod:`dubai_property.silver.areas`
  turn bronze rows into silver rows (DataFrames in, DataFrames out).
* :mod:`dubai_property.silver.pipeline` reads new bronze loads and MERGEs them
  into the silver tables.

Import the Spark modules directly, so the rest of the package works without
Spark installed.
"""
