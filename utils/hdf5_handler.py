import pandas as pd
import concurrent.futures
from datetime import datetime

# Optimized HDF5 file handler with parallel writing
def write_to_hdf5_parallel(df, h5_filename, symbol):
    """ Write symbol data to HDF5 file with optimized settings """
    store = pd.HDFStore(h5_filename, mode='a', complevel=9, complib='blosc')
    store.put(f'/tradedata/{symbol}', df, format='table', data_columns=True)
    store.close()

def save_daily_trade_data(df_dict, date):
    """ Saves all symbols' trade data for a single day to an HDF5 file """
    h5_filename = f'DATADIR/h5/{date}.h5'
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(write_to_hdf5_parallel, df, h5_filename, symbol) 
                   for symbol, df in df_dict.items()]
        concurrent.futures.wait(futures)

def load_hdf5_for_period(start_date, end_date):
    """ Load multiple HDF5 files to satisfy period requirements """
    hdf5_files = [f'DATADIR/h5/{date}.h5' for date in pd.date_range(start_date, end_date)]
    combined_data = []
    for h5_file in hdf5_files:
        store = pd.HDFStore(h5_file, mode='r')
        for symbol in store.keys():
            combined_data.append(store[symbol])
        store.close()
    return pd.concat(combined_data)

def save_df_to_hdf5(df, date):
    with pd.HDFStore(f'{STOREDIR}/{date}.h5',
                     mode='w',
                     complevel=9,
                     complib='blosc') as store:
        for symbol in df['symbol'].unique():
            store.put(f'/trades/{symbol}', df[df['symbol'] == symbol])

import h5py
import numpy as np

# Optimized HDF5 writer using h5py
def trades_to_hdf5(tradeparser, h5filepath):
    print(f'Starting trades_to_hdf5: {datetime.now()}')
    with h5py.File(h5filepath, 'a') as h5f:
        for trade in tradeparser:
            ts, symbol, size, price, trade_id = trade
            symbol_group = f'/trades/{symbol}'
            if symbol_group in h5f:
                # Group exists, append to the dataset
                dataset = h5f[symbol_group]
                trade = np.array([(ts, symbol, size, price, trade_id)],
                                 dtype=[('ts', 'i8'),
                                        ('symbol', 'S10'),
                                        ('size', 'i4'),
                                        ('price', 'f4'),
                                        ('trade_id', 'i8')])
                current_size = dataset.shape[0]
                dataset.resize(current_size + 1, axis=0)
                dataset[current_size] = trade
            else:
                # Group does not exist, create it
                trade = np.array([(ts, symbol, size, price, trade_id)],
                                    dtype=[('ts', 'i8'),
                                           ('symbol', 'S10'),
                                           ('size', 'i4'),
                                           ('price', 'f4'),
                                           ('trade_id', 'i8')])
                h5f.create_dataset(symbol_group,
                                   data=trade,
                                   maxshape=(None,),
                                   dtype=trade.dtype)

    print(f'Finished trades_to_hdf5: {datetime.now()}')
