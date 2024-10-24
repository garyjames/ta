import argparse
import pandas as pd
from stockanalysis.data import parse_data

STOREDIR = '/home/ggalvez/data/historical/iex/h5'

def df_to_hdf5(df, date):
    with pd.HDFStore(f'{STOREDIR}/{date}.h5',
                     mode='w',
                     complevel=9,
                     complib='blosc') as store:
        for symbol in df['symbol'].unique():
            store.put(f'/trades/{symbol}', df[df['symbol'] == symbol])

def save_daily_trades_v1(df, date):
    with pd.HDFStore(f'{STOREDIR}/{date}.h5',
                     mode='w',
                     complevel=9,
                     complib='blosc') as store:
        for symbol in df['symbol'].unique():
            store.put(f'/trades/{symbol}', df[df['symbol'] == symbol])


def terminal_interface():
    parser = argparse.ArgumentParser(description="Stock MACD Analysis Tool")
    
    # Command for analyzing a symbol with period and precision options
    parser.add_argument('--symbol', type=str, help="Stock symbol to analyze")
    parser.add_argument('--period', type=str, help="Time period to analyze, e.g., 3m for 3 months")
    parser.add_argument('--precision', type=str, help="Time precision, e.g., 'day', 'hour'")
    # Command for saving a pcap file
    parser.add_argument('--save', type=str, help="Save the parsed trade data from a pcap file")
    
    # Command for retrieving data by date
    parser.add_argument('--retrieve', action='store_true', help="Retrieve data for a specific date")
    parser.add_argument('--date', type=str, help="Date to retrieve data, formatted as YYYYMMDD")
    
    args = parser.parse_args()
    return args

args = terminal_interface()
df = parse_data.parse_pcap_to_dataframe(args.save)
save_daily_trades_v1(df, args.date)
