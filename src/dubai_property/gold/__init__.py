"""Gold layer: a Kimball star schema and monthly aggregates for Power BI.

* :mod:`dubai_property.gold.rules` holds constants and small pure functions
  (date keys, the UAE weekend, property type mapping) with no Spark dependency.
* :mod:`dubai_property.gold.dimensions` builds the dimensions, including
  ``dim_area`` as SCD Type 2.
* :mod:`dubai_property.gold.facts` builds the facts and ``agg_area_monthly``.
* :mod:`dubai_property.gold.pipeline` reads silver and writes the gold tables.

Import the Spark modules directly, so the rest of the package works without
Spark installed.
"""
