import pandas as pd
import numpy as np
from ta.utils.hdf5_handler import get_single_date

def custom_ohlc(data, unit_size):
    # Get the starting timestamp, normalize to unit_size
    #data['time_unit'] = data['timestamp'] // unit_size
    data['time_unit'] = data.index // unit_size
    grouped = data.groupby('time_unit')

    # Aggregate open, high, low, close
    ohlc = grouped['price'].agg(
        open='first', high='max', low='min', close='last'
    ).reset_index(drop=True)
    return ohlc

# Example trade data
data = pd.DataFrame({
    'timestamp': np.random.randint(1690000000, 1690003600, 1000), 
    'price': np.random.rand(1000) * 100
})

trades = get_single_date('AAPL', '20241007')

# Calculate custom OHLC with 1-minute intervals (unit size in seconds)
ohlc_custom = custom_ohlc(trades, unit_size=7200)
print(ohlc_custom)
