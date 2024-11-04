import h5py
import numpy as np
import pandas as pd
from scipy.stats import linregress
from typing import Tuple, List, Optional
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from os.path import isfile

# Define default MACD parameters
DEFAULT_SHORT_PERIOD = 12
DEFAULT_LONG_PERIOD = 26
DEFAULT_SIGNAL_PERIOD = 9
#DEFAULT_SHORT_PERIOD = 3
#DEFAULT_LONG_PERIOD = 10
#DEFAULT_SIGNAL_PERIOD = 16

# Helper function to generate a list of dates between two dates
def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate a list of date strings between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    g = [(start+timedelta(days=i)).strftime("%Y%m%d") for i in range((end-start).days+1)]
    dates = list(g)
    return dates

# Function to load tick data from multiple HDF5 files in date range
def load_tick_data(symbol: str,
                   start_date: str,
                   end_date: str,
                   file_path_template: str) -> pd.DataFrame:
    """
    Load tick data for a symbol across multiple HDF5 files within a specific date range.
    
    Args:
    - symbol: Stock symbol to load data for.
    - start_date: Start date in "YYYY-MM-DD" format.
    - end_date: End date in "YYYY-MM-DD" format.
    - file_path_template: "/path/to/data/{}.h5" with {} replaced with date.

    Returns:
    - A concatenated DataFrame of tick data within the specified date range.
    """
    date_range = generate_date_range(start_date, end_date)
    all_data = []
    
    for date in date_range:
        file_path = file_path_template.format(date)
        if isfile(file_path): # ignore missing files
            try:
                with h5py.File(file_path, 'r') as f:
                    group_path = f"trades/{symbol}"
                    if group_path in f:
                        symbol_data = f[group_path]
                        df = pd.DataFrame({
                            #'timestamp': symbol_data['timestamp'][:],
                            'ts': symbol_data['ts'][:],
                            'price': symbol_data['price'][:],
                            'size': symbol_data['size'][:],
                            'trade_id': symbol_data['trade_id'][:]
                        })
                        df['ts'] = pd.to_datetime(df['ts'], unit='ns')
                        all_data.append(df)
            except (OSError, KeyError):
                print(f"File or group not found for {file_path} and symbol {symbol}")
        else:
            continue
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# EWMA Calculation with Custom Decay
def ewma(data: np.ndarray, period: int, decay_factor: Optional[float] = None) -> np.ndarray:
    """Calculate Exponentially Weighted Moving Average."""
    if decay_factor is None:
        decay_factor = 2 / (period + 1)
    ewma_data = np.zeros_like(data)
    ewma_data[0] = data[0]
    for i in range(1, len(data)):
        ewma_data[i] = decay_factor * data[i] + (1 - decay_factor) * ewma_data[i - 1]
    return ewma_data

# MACD Calculation
def calculate_macd(data: np.ndarray,
                   short_period: int = DEFAULT_SHORT_PERIOD,
                   long_period: int = DEFAULT_LONG_PERIOD,
                   signal_period: int = DEFAULT_SIGNAL_PERIOD,
                   decay_factor: Optional[float] = None) -> Tuple[np.ndarray,
                                                                  np.ndarray,
                                                                  np.ndarray]:
    """Calculate MACD line, Signal line, and MACD histogram."""
    short_ema = ewma(data, short_period, decay_factor)
    long_ema = ewma(data, long_period, decay_factor)
    macd_line = short_ema - long_ema
    signal_line = ewma(macd_line, signal_period, decay_factor)
    macd_histogram = macd_line - signal_line
    return macd_line, signal_line, macd_histogram

# Mark Condition Function
def find_macd_conditions(macd_line: np.ndarray, signal_line: np.ndarray) -> List[int]:
    """Identify points where MACD crosses above/below the signal line."""
    conditions = []
    for i in range(1, len(macd_line)):
        if macd_line[i] > signal_line[i] and macd_line[i - 1] <= signal_line[i - 1]:
            conditions.append(i)  # Cross above
        elif macd_line[i] < signal_line[i] and macd_line[i - 1] >= signal_line[i - 1]:
            conditions.append(i)  # Cross below
    return conditions

# Visualization
# Modified plot_macd function to use aggregated timestamps
def plot_macd(aggregated_data: np.ndarray,
              macd_line: np.ndarray,
              signal_line: np.ndarray,
              conditions: List[int]):
    """
    Plot aggregated close prices and MACD with markers for specified conditions,
    ensuring proper datetime labeling on the x-axis.
    """
    plt.figure(figsize=(14, 8))

    # Convert nanosecond timestamps to datetime objects for x-axis
    timestamps = pd.to_datetime(aggregated_data['ts'])

    # Plotting the aggregated close prices
    plt.subplot(2, 1, 1)
    plt.plot(timestamps, aggregated_data['close'], label='Close Price')
    plt.title('Aggregated Price and MACD with Conditions')
    plt.legend()

    # Set date formatting for the x-axis to improve readability
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    # Plotting the MACD and Signal line
    plt.subplot(2, 1, 2)
    plt.plot(timestamps, macd_line, label='MACD Line')
    plt.plot(timestamps, signal_line, label='Signal Line')
    plt.scatter(timestamps[conditions],
                macd_line[conditions],
                color='red',
                label='Conditions',
                marker='x')

    plt.legend()

    # Set date formatting for the x-axis in the second plot
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.show()

# Define a function to convert timestamp to the nearest interval
def convert_to_interval(ts: np.ndarray, interval: str = '1min') -> np.ndarray:
    """Convert nanosecond timestamps to the nearest interval."""

    # Convert datetime64[ns] to integer nanoseconds
    ts_ns = ts.astype('int64')

    if interval == '1min':
        interval_ns = int(60 * 1e9) # 1 minute in nanoseconds
    elif interval == '5min':
        interval_ns = int(5 * 60 * 1e9) # 5 minutes in nanoseconds
    elif interval == '10min':
        interval_ns = int(10 * 60 * 1e9)
    elif interval == '30min':
        interval_ns = int(30 * 60 * 1e9)
    elif interval == '1h':
        interval_ns = int(60 * 60 * 1e9)
    elif interval == '2h':
        interval_ns = int(120 * 60 * 1e9)
    elif interval == '4h':
        interval_ns = int(240 * 60 * 1e9)
    elif interval == '1d':
        interval_ns = int(390 * 60 * 1e9)
    else:
        raise ValueError(f'Unsupported interval {interval}')

    # Calculate interval index (bucket) for each timestamp
    return (ts_ns // interval_ns).astype(np.int64)

# Aggregate data at specified intervals
def aggregate_trades(trades: np.ndarray, interval: str = '1min') -> np.ndarray:
    """Aggregate trade data to specified intervals.

    Parameters:
    - trades: NumPy structured array with 'ts' and 'price' fields.
    - interval: Time interval for aggregation ('1min', '5min', etc.)

    Returns:
    - Aggregated close prices per interval."""

    intervals = convert_to_interval(trades['ts'], interval)
    unique_intervals, last_indices = np.unique(intervals, return_index=True)

    # Extract the last trade per interval as the "close" price
    close_prices = trades['price'][last_indices]
    timestamps = trades['ts'][last_indices]

    # Return structured array with interval timestamps and close prices
    return np.array(list(zip(timestamps, close_prices)),
                    dtype=[('ts', 'i8'), ('close', 'f4')])

def filter_symbols_for_macd(symbols: List[str],
                            date_range: Tuple[str, str],
                            file_path_template: str,
                            interval: str = '5min') -> List[Tuple[str, float]]:
    """ Filter symbols based on MACD criteria: both MACD and Signal Line are negative,
    and MACD is trending toward a crossover with the highest positive slope.

    Returns:
    - A list of tuples with (symbol, slope), sorted by the greatest positive slope."""

    qualified_symbols = []

    for symbol in symbols:
        # Load and aggregate data
        tick_data = load_tick_data(symbol, date_range[0], date_range[1], file_path_template)
        if tick_data.empty:
            continue  # Skip symbols with no data

        aggregated_data = aggregate_trades(tick_data.to_records(), interval=interval)

        # Calculate MACD and Signal Line
        macd_line, signal_line, _ = calculate_macd(aggregated_data['close'])

        # Check if both MACD and Signal Line are negative
        if macd_line[-1] < 0 and signal_line[-1] < 0:
            # Calculate slope over the last few points to assess trend
            # TODO
            #recent_macd = macd_line[-3:]  # Last 5 points for slope calculation
            recent_macd = macd_line[-5:]  # Last 5 points for slope calculation
            slope, _, _, _, _ = linregress(range(len(recent_macd)), recent_macd)

            # Check if MACD is trending positively (towards crossing signal line)
            # TODO
            if slope > 0 and macd_line[-1] < signal_line[-1]:
            #if slope > 0 and macd_line[-1] == signal_line[-1]:
                qualified_symbols.append((symbol, slope))

    # Sort symbols by the greatest positive slope
    qualified_symbols.sort(key=lambda x: x[1], reverse=True)

    return qualified_symbols


# Example usage
symbols = []
with h5py.File('/srv/b/h5/20241028.h5', 'r') as hf:
    for symbol in hf['trades'].keys():
        symbols.append(symbol)

date_range = ('2024-10-28', '2024-11-01')
file_path_template = '/srv/b/h5/{}.h5'

# Get symbols prioritized by MACD trend
filtered_symbols = filter_symbols_for_macd(symbols,
                                           date_range,
                                           file_path_template,
                                           interval='2h')

# Display the top results
for symbol, slope in filtered_symbols[:25]:  # Limit to top 25
    print(f'Symbol: {symbol}  Slope: {slope:.4f}')


if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--start-date', type=str, default='2024-10-07')
    argparser.add_argument('--end-date', type=str, default='2024-10-18')
    argparser.add_argument('--symbol', type=str, default='CRMD')
    argparser.add_argument('--file-path-template', type=str, default='/srv/b/h5/{}.h5')
    argparser.add_argument('--aggregate', type=str)
    args = argparser.parse_args()

    # Load data and calculate MACD
    tick_data = load_tick_data(args.symbol,
                               args.start_date,
                               args.end_date,
                               args.file_path_template)
    if tick_data.empty:
        print(f"No data available for symbol {symbol} in the given date range.")
    elif args.aggregate:
        aggregated_data = aggregate_trades(tick_data.to_records(), interval=args.aggregate)
        macd_line, signal_line, _ = calculate_macd(aggregated_data['close'])
    
        # Plot the MACD and conditions
        conditions = find_macd_conditions(macd_line, signal_line)
        plot_macd(aggregated_data, macd_line, signal_line, conditions)
    else:
        print(f"No data available for symbol {symbol} in the given date range.")

