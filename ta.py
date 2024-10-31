import h5py
import numpy as np
import pandas as pd
from typing import Tuple, List, Optional
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Define default MACD parameters
DEFAULT_LONG_PERIOD = 26
DEFAULT_SHORT_PERIOD = 12
DEFAULT_SIGNAL_PERIOD = 9

# Helper function to generate a list of dates between two dates
def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate a list of date strings between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    g = [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]
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
    - file_path_template: Template for file path, e.g., "/path/to/data/{}.h5" where {} will be replaced with the date.

    Returns:
    - A concatenated DataFrame of tick data within the specified date range.
    """
    date_range = generate_date_range(start_date, end_date)
    all_data = []
    
    for date in date_range:
        file_path = file_path_template.format(date)
        try:
            with h5py.File(file_path, 'r') as f:
                group_path = f"trades/{symbol}"
                if group_path in f:
                    symbol_data = f[group_path]
                    df = pd.DataFrame({
                        #'timestamp': symbol_data['timestamp'][:],
                        'timestamp': symbol_data['ts'][:],
                        'price': symbol_data['price'][:],
                        'size': symbol_data['size'][:],
                        'trade_id': symbol_data['trade_id'][:]
                    })
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ns')
                    all_data.append(df)
        except (OSError, KeyError):
            print(f"File or group not found for {file_path} and symbol {symbol}")
    
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
                   decay_factor: Optional[float] = None) -> Tuple[np.ndarray,np.ndarray,np.ndarray]:
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
def plot_macd(data: pd.DataFrame,
              macd_line: np.ndarray,
              signal_line: np.ndarray,
              conditions: List[int]):
    """Plot price and MACD with markers for specified conditions."""
    plt.figure(figsize=(14, 8))
    
    # Plotting the stock price
    plt.subplot(2, 1, 1)
    plt.plot(data['timestamp'], data['price'], label='Price')
    plt.title('Price and MACD with Conditions')
    plt.legend()
    
    # Plotting the MACD and Signal line
    plt.subplot(2, 1, 2)
    plt.plot(data['timestamp'], macd_line, label='MACD Line')
    plt.plot(data['timestamp'], signal_line, label='Signal Line')
    plt.scatter(data['timestamp'].iloc[conditions],
                macd_line[conditions],
                color='red',
                label='Conditions',
                marker='x')
    plt.legend()
    
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--start-date', type=str, default='2024-10-07', help='YYYY-MM-DD')
    argparser.add_argument('--end-date', type=str, default='2024-10-18', help='YYYY-MM-DD')
    argparser.add_argument('--symbol', type=str, default='CRMD')
    argparser.add_argument('--file-path-template', type=str, default='/srv/b/h5/{}.h5')
    args = argparser.parse_args()

    # Load data and calculate MACD
    tick_data = load_tick_data(args.symbol, args.start_date, args.end_date, args.file_path_template)
    if tick_data.empty:
        print(f"No data available for symbol {symbol} in the given date range.")
    else:
        macd_line, signal_line, _ = calculate_macd(tick_data['price'].values)
        conditions = find_macd_conditions(macd_line, signal_line)
    
        # Plot the MACD and conditions
        plot_macd(tick_data, macd_line, signal_line, conditions)
        print(f"No data available for symbol {symbol} in the given date range.")

