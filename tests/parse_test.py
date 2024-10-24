from stockanalysis.data.parse_data import get_parser
from datetime import datetime

if __name__ == '__main__':

    pcapfilepath = ('/home/ggalvez/data/historical/iex/pcap/'
                    'data_feeds_20240617_20240617_IEXTP1_TOPS1.6.pcap.gz')

    trades = get_parser(pcapfilepath)

    print(f'Starting {datetime.now()}')
    for trade in trades:
        trade
    print(f'Done {datetime.now()}')

