# Examples

## Inspect Table Metadata
``` python
import bdpiceberg

from bdpiceberg.metacat import MetacatCatalog

# instantiate Metacat Catalog(defaults to prodhive)
conf = {"iceberg.scan.plan-in-worker-pool": True}
cat = MetacatCatalog(conf=conf)

# load table
tbl = cat.load(db="tgooch", table="test_iceberg_schema_evol")

# inspect metadata
print(tbl.schema())
print(tbl.spec())
print(tbl.location())

# get table level record count
from pprint import pprint
pprint(int(tbl.current_snapshot().summary.get("total-records")))
```

[Notebook Link](https://commuter.dynprod.netflix.net:7002/view/tgooch/notebooks/Iceberg%20Python%20Examples/Inspect%20Table%20Metadata.ipynb)

## Iceberg Table -> Pandas DF
``` python
tbl = cat.load(db="tgooch", table="iceberg_geo_country_d")
df = tbl.new_scan().select(["country_iso_code", "country_desc"]).to_pandas()
print(df)
```

[Notebook Link](https://commuter.dynprod.netflix.net:7002/view/tgooch/notebooks/Iceberg%20Python%20Examples/Scan%20Iceberg%20Table%20to%20Pandas.ipynb)

## Iceberg Table -> Arrow Table
``` python
tbl = cat.load(db="tgooch", table="iceberg_geo_country_d")
arrow_tbl = tbl.new_scan().select(["country_iso_code", "country_desc"]).to_arrow_table()
print(arrow_tbl)
```

[Notebook Link](https://commuter.dynprod.netflix.net:7002/view/tgooch/notebooks/Iceberg%20Python%20Examples/Scan%20Iceberg%20Table%20to%20Arrow%20Table.ipynb)