import pandas as pd
from ta.utils.ggparser import TradeParser
from datetime import datetime

#   Parses raw pcap trade data into DataFrame

#   The parser iterable has only trade-report events when 'messages.TradeReport'
# filter is used and indexed by epoch timestamp.'''

def get_parser(pcap_filepath):
    return TradeParser(pcap_filepath)

def iter_trades(parser):
    g = ( (i.timestamp, i.symbol, i.size, i.price, i.trade_id) for i in parser )
    return g

def get_df():
    # TODO This needs to be enhanced with logging
    print(f'Start: {datetime.now()}')
    if filepath.endswith('h5'):
        if args.use_h5py:
            pass
        elif args.use_pd:
            pass
        else:
            print(f'unknown option {args}')
            return None
    elif (filepath.endswith('gz') or filepath.endswith('pcap')): 
        parser = get_parser(filepath)
        df = pd.DataFrame(iter_trades(parser),
                          columns=['ts', 'symbol', 'size', 'price', 'trade_id'])
        df.set_index('ts', inplace=True)

    print(f'Done: {datetime.now()}')
    return df


if __name__ == '__main__':

    args = parser.parse_args()

    df = get_df(args.filepath)
