import pandas as pd
from utils.hdf5_handler import save_daily_trade_data, load_hdf5_for_period

def test_hdf5_write():
    df = pd.DataFrame({'ts': [1, 2, 3], 'price': [100, 101, 102]})
    df_dict = {'AAPL': df}
    save_daily_trade_data(df_dict, '20231012')
    loaded_df = load_hdf5_for_period('20231012', '20231012')
    assert not loaded_df.empty

def test_hdf5_read_period():
    df = load_hdf5_for_period('20231010', '20231012')
    assert not df.empty

