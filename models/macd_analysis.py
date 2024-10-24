import pandas as pd

# MACD calculation: difference of the 12-day and 26-day EMAs, signal is 9-day EMA of MACD
def calculate_macd(df):
    df['ema_12'] = df['price'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['price'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    return df
