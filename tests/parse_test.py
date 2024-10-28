import pytest
from ta.data.parse_data import get_parser
from datetime import datetime

pcap_test_filepath = ('/home/ggalvez/projects/ta/tests/data_test.pcap')

def test_get_trades_from_pcap():
    trades = get_parser(pcap_test_filepath)
    for i, trade in enumerate(trades):
        assert trade

    assert i == 5024 # should be 5025 trades (0 through 5024) total in data_test.pcap

