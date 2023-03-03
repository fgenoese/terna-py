# terna-py
Python client for the Transparency API of Terna, the Italian electricity transmission system operator

[![PyPI Latest Release](https://img.shields.io/pypi/v/terna-py.svg)](https://pypi.org/project/terna-py/)
[![Downloads](https://static.pepy.tech/badge/terna-py)](https://pypi.org/project/terna-py/)

Documentation of the API: https://developer.terna.it/docs/read/APIs_catalog#en

## Installation
`python3 -m pip install terna-py`

## Usage
```python
import terna as trn
import pandas as pd

# Please follow the API documentation to register an account and create credentials
key = '<YOUR API KEY>'
secret = '<YOUR API SECRET>'
client = trn.TernaPandasClient(api_key=key,api_secret=secret)

# Note: you specifically need to set a start= and end= parameter which should be a pandas timestamp with timezone
start = pd.Timestamp("20210101", tz='Europe/Rome')
end = pd.Timestamp("20210131", tz='Europe/Rome')
bzone = ['NORD', 'CNOR', 'SUD', 'CSUD', 'SARD', 'SICI', 'CALA', 'Italy']
gen_type = ['Thermal', 'Wind', 'Geothermal', 'Photovoltaic', 'Self-consumption', 'Hydro']
year = 2022

# Note: all methods return Pandas DataFrames
df_tload = client.get_total_load(start=start, end=end, bzone=bzone)
df_mload = client.get_market_load(start=start, end=end, bzone=bzone)

df_gen = client.get_actual_generation(start=start, end=end, gen_type=gen_type)
df_cap = client.get_installed_capacity(year=year, gen_type=gen_type)

df_xborderschedule = client.get_scheduled_foreign_exchange(start=start, end=end)
df_xborderflow = client.get_physical_foreign_flow(start=start, end=end)

df_internalschedule = client.get_scheduled_internal_exchange(start=start, end=end)
df_internalflow = client.get_physical_internal_flow(start=start, end=end)
```

