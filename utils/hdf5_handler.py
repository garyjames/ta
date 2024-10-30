from datetime import datetime
from collections import defaultdict
import pandas as pd
import numpy as np
import h5py

def get_single_date(symbol, date, datadir='/srv/b/h5'):
    # Open a single HDF5 file and access the "symbol" group
    with h5py.File('{}/{}.h5'.format(datadir, date), 'r') as hfile:
        # Access the dataset for 'AAPL' inside the 'trades' group
        aapl_data = hfile['trades'][f'{symbol}']

        # Convert the dataset to a list of tuples
        data = [tuple(row) for row in aapl_data]

        # Convert the list of tuples into a DataFrame

        # elements after the first one for columns
        df = pd.DataFrame([row[1:] for row in data],
                          # first element (timestamp) as the index
                          index=[row[0] for row in data],
                          # Specify column names
                          columns=['symbol', 'size', 'price', 'trade_id']
                         )

        df['symbol'] = df['symbol'].str.decode('utf-8')
        return df

def get_daterange(start,end):
    test_files = [
        '/srv/b/h5/20241007.h5',
        '/srv/b/h5/20241008.h5',
        '/srv/b/h5/20241009.h5',
        '/srv/b/h5/20241010.h5',
        '/srv/b/h5/20241011.h5']
    
    # Initialize an empty list to hold DataFrames
    dfs = []
    
    # Loop through each file and extract the data for 'AAPL'
    for file in files:
        with h5py.File(file, 'r') as hfile:
            symbol_data = hfile['trades'][f'{symbol}']
            data = [tuple(row) for row in symbol_data]
    
            # Create a DataFrame for the current file's data
            df = pd.DataFrame([row[1:] for row in data],
                              index=[row[0] for row in data],
                              columns=['symbol', 'size', 'price', 'trade_id']
                             )
            df['symbol'] = df['symbol'].str.decode('utf-8')
            dfs.append(df)
    
    # Concatenate all the DataFrames from different files
    final_df = pd.concat(dfs)
    
    # Sort the DataFrame by index (timestamp) if needed
    final_df = final_df.sort_index()
    final_df['ts'] = final_df.index.map(lambda x: datetime.fromtimestamp(x/1000000000))
    
    print(final_df)
    return final_df


# Optimized HDF5 writer with batch writes
def trades_to_hdf5(tradeparser, h5filepath, batch_size=1000):
    print(f'Starting trades_to_hdf5: {datetime.now()}')

    # Buffer to hold trades for each symbol before batch write
    trade_buffers = defaultdict(list)

    with h5py.File(h5filepath, 'a') as h5f:
        for trade in tradeparser:
            ts, symbol, size, price, trade_id = trade
            symbol_group = f'/trades/{symbol}'

            # Append trade to the buffer for the symbol
            trade_buffers[symbol_group].append((ts, symbol, size, price, trade_id))

            # If buffer reaches batch size, write to HDF5
            if len(trade_buffers[symbol_group]) >= batch_size:
                write_trades_to_dataset(h5f, symbol_group, trade_buffers[symbol_group])
                trade_buffers[symbol_group].clear()  # Clear buffer after writing

        # Write any remaining data in the buffer to HDF5
        for symbol_group, trades in trade_buffers.items():
            if trades:  # Only write if buffer is not empty
                write_trades_to_dataset(h5f, symbol_group, trades)

    print(f'Finished trades_to_hdf5: {datetime.now()}')

def write_trades_to_dataset(h5f, symbol_group, trades):
    """
    Helper function to write a batch of trades to HDF5 dataset.
    """
    # Convert trades to structured NumPy array
    trade_array = np.array(trades, dtype=[
        ('ts', 'i8'),
        ('symbol', 'S10'),
        ('size', 'i4'),
        ('price', 'f4'),
        ('trade_id', 'i8')
    ])

    if symbol_group in h5f:
        # Dataset exists, append data
        dataset = h5f[symbol_group]
        current_size = dataset.shape[0]
        dataset.resize(current_size + trade_array.shape[0], axis=0)
        dataset[current_size:] = trade_array
    else:
        # Create dataset if it does not exist
        h5f.create_dataset(
            symbol_group,
            data=trade_array,
            maxshape=(None,),
            dtype=trade_array.dtype
        )

def show_h5py_hdf5(filepath):
    with h5py.File(filepath, 'r') as hfile:
        hfile.visit(lambda x: print(x))

def show_pd_hdf5(filepath):
    with pd.HDFStore(filepath, 'r') as store:
        for group, _, _ in store.walk():
            print(group)
