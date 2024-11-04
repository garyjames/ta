import numpy as np
import datetime

'''In this example:

1. The convert_timestamp function converts each timestamp to a formatted string.
2. np.vectorize is used to apply convert_timestamp to each ts in the array.
3. Finally, we construct a new array with the readable timestamps and the original close
   values.'''

# Sample data
data = np.array([(1726488053442816981, 117.51), (1726502400137467423, 116.23),
                 (1726516864882569486, 116.72), (1726574702160557380, 117.87),
                 (1730217600094502913, 141.685), (1730232164947803001, 141.08),
                 (1730292688450214140, 140.36), (1730304000108818533, 139.81),
                 (1730318544520916668, 139.54)],
                dtype=[('ts', '<i8'), ('close', '<f4')])

# Define a function to convert timestamp to readable format
def convert_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts / 1e9).strftime('%Y-%m-%d %H:%M:%S')

# Vectorize the function for element-wise operation on array
convert_vectorized = np.vectorize(convert_timestamp)

# Apply the function to the 'ts' column
readable_timestamps = convert_vectorized(data['ts'])

# Create a new structured array with the readable timestamps
result = np.array([(readable_timestamps[i], data['close'][i]) for i in range(len(data))],
                  dtype=[('ts_readable', 'U19'), ('close', '<f4')])

print(result)
