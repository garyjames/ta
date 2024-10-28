import pandas as pd
import numpy as np
from ta.utils.hdf5_handler import get_single_date

# Generate sample trade data with timestamps
data = pd.DataFrame({
    'timestamp': pd.to_datetime(
        np.random.randint(1690000000, 1690003600, 1000), unit='s'
    ),
    'price': np.random.rand(1000) * 100
})
data.set_index('timestamp', inplace=True)

trades = get_single_date('AAPL', '20241007')
trades.index = pd.to_datetime(trades.index, unit='ns')

# Resample with OHLC for 1-minute intervals
ohlc_pandas = trades['price'].resample('1h').ohlc()
print(ohlc_pandas)
