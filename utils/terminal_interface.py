import argparse

# Command-line interface for the app
def terminal_interface():
    parser = argparse.ArgumentParser(description="Stock Analysis Tool")
    
    parser.add_argument('--date', type=str, help="Date as YYYYMMDD")
    parser.add_argument('--filepath', type=str)
    parser.add_argument('--h5-filepath', type=str)
    parser.add_argument('--pcap-filepath', type=str)

    # symbol with period and precision options
    parser.add_argument('--symbol',
                        type=str,
                        help="Equity symbol")
    parser.add_argument('--period',
                        type=str,
                        help="Time period.  (e.g. 3m for 3 months)")
    parser.add_argument('--precision',
                        type=str,
                        help="Time precision, e.g., 'day', 'hour'")
    
    # pcap file and getting trades
    parser.add_argument('--get-trades', action='store_true')
    parser.add_argument('--parse-pcap', action='store_true')
    parser.add_argument('--save-to-hdf5', action='store_true')
    parser.add_argument('--save', action='store_true')

    # TODO needs to be enhanced using pytest
    parser.add_argument('--test-args', action='store_true')
    
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    print(terminal_interface())

