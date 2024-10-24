# Adjusts zoom and time-precision in the chart display
def adjust_zoom(df, zoom_level):
    zoom_mapping = {1: '1min', 2: '5min', 3: '15min', 4: '1h', 5: '1d'}
    if zoom_level in zoom_mapping:
        return df.resample(zoom_mapping[zoom_level]).mean()
    return df
