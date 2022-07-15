# background

Come from a part of my project `mlc.sources`

# usage

## read_dir

Work with `.parquet` files by default.

```python
from data_reader import get_reader

r = get_reader()
df = r.read_dir(r"E:\Workspace\parquet_data")

df = r.read_dir("/e/Workspace/parquet_data")

```

for `csv` .

```python
r = get_reader(suffix='.csv')
df = r.read_dir("/tmp/Workspace/parquet_data")

```

## read with date partition


```python

end_date='2019-07-25 00:00:00'
start_date = pd.to_datetime(end_date) - timedelta(days=90)

reader = get_reader(categories=['store_id', 'product_id'])
columns = ["store_id", "product_id", "count",  "price", "price_guide", "price_change_category", "time_create"]
df1 = reader.read_dir_daterange(ORDERITEMS_PATH, columns=columns, start_date=start_date, end_date=end_date)

```

## read multi paths

```python

SOURCE_DATA_PATH = '/tmp'
today = datetime(2019, 4, 13)
now = datetime(2019, 4, 13, 9)

reader = get_reader()
dates = pd.date_range(today-timedelta(days=92), today )
df = reader.read_paths([join(SOURCE_DATA_PATH, 'arch', 'M-Day/orders/%s/*.parquet' % i.strftime('%Y/%m/%d') ) for i in dates], columns=['store_id', 'time_create', 'total_price'])

```

## read path by wildcard

```python

r = get_reader()
columns = ["store_product_id", "store_id", "product_id", "count", "time_create"]
df2019 = r.read_wildcard_path(r'E:\M-Day\orderitems\2019\*\*\*.parquet', columns=columns)

```