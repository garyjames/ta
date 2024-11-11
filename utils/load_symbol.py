'''load symbol'''

from datetime import timedelta, date
from os.path import isfile

import numpy as np
import h5py

def load_symbol(symbol, n_days):
    '''load symbol from n_days ago'''

    group_path = 'trades/%s' % symbol
    today = date.today()

    dtype = np.dtype([
        ('ts', '<i8'),
        ('symbol', 'S10'),
        ('size', '<i4'),
        ('price', '<f4'),
        ('trade_id', '<i8')
        ])
    trades = np.empty(0, dtype=dtype)

    for dt in (today - timedelta(n) for n in reversed(range(n_days))):
        filepath = '/srv/b/h5/%s.h5' % dt.strftime('%Y%m%d')
        if isfile(filepath): # ignore missing files
            try:
                with h5py.File(filepath, 'r') as hf:
                    if group_path in hf:
                        daytrades = np.array(hf[group_path], dtype=dtype)
                        trades = np.concatenate((trades, daytrades))
            except (TypeError, OSError, KeyError) as err:
                print('something wrong, file or group not found:', err)
        else:
            continue # missing file

    return trades


if __name__ == '__main__':

    from terminal_interface import terminal_interface as termx
    args = termx()

    trades = load_symbol(args.symbol, args.n_days)
