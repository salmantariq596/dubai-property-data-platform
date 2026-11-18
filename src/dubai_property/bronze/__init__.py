"""Bronze layer: raw files to append-only tables, every column a string.

Spark-dependent code lives in :mod:`dubai_property.bronze.ingest`; import it
directly so that the rest of the package works without Spark installed.
"""
