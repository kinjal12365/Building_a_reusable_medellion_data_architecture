import sys
sys.path.insert(0, 'src')
from bronze.discovery.format_detector import detect_format

# Test extension detection
print(detect_format('abfss://landing@store.dfs.core.windows.net/customers/file.csv'))
print(detect_format('abfss://landing@store.dfs.core.windows.net/orders/file.json'))
print(detect_format('abfss://landing@store.dfs.core.windows.net/sales/file.parquet'))

# Test config override
print(detect_format('somefile.txt', config_override='csv'))

# Test magic bytes
print(detect_format('somefile', read_bytes_fn=lambda p, n: b'PAR1'))
print(detect_format('somefile', read_bytes_fn=lambda p, n: b'{"k":'))

print('Format detector works!')