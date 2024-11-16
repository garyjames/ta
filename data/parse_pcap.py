'''parse_pcap'''

import subprocess
import dpkt
from datetime import datetime
import struct

import gzip
import pandas as pd
import h5py

extract = struct.unpack_from

pcap_test_filepath = '/home/ggalvez/projects/ta/tests/data_test.pcap'
pcap_filepath = '/srv/a/pcap/data_feeds_20240617_20240617_IEXTP1_TOPS1.6.pcap.gz'
pcap_filepath2 = '/srv/a/pcap/data_feeds_20241111_20241111_IEXTP1_TOPS1.6.pcap.gz'

# Step 1: Extract data with tshark
def extract_pcap_data(pcap_file):
    tshark_command = [
        "tshark", "-c", "3200", "-r", pcap_file, "-T", "fields",
        "-e", "frame.time_epoch",        # Timestamp (Epoch format)
        "-e", "data.data"            # Extract hex payload
    ]
    
    # Run the command and capture the output
    result = subprocess.run(tshark_command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error running tshark: {result.stderr}")
    
    return result.stdout.splitlines()

# Step 2: Parse raw packet data based on byte offsets
def parse_trade_report(line):
    fields = line.split("\t")
    if len(fields) < 2:
        return None  # Incomplete line
    
    timestamp_epoch = float(fields[0])  # First column is the timestamp
    timestamp = datetime.fromtimestamp(timestamp_epoch)
    
    # Convert hex string to binary data
    raw_data = bytes.fromhex(fields[1])
    message_header = raw_data[:40]
    message_length = raw_data[40:42]
    payload = raw_data[42:]
    
    # Unpack fields based on known offsets and types
    message_type = payload[0:1].decode('utf-8', errors='ignore')  # Offset 0, Length 1 byte
    if message_type != 'T':  # Skip non-trade messages
        return None

    sale_condition_flags = struct.unpack_from('<c', payload, 0)[0]
    timestamp_trade = struct.unpack_from('<Q', payload, 2)[0]
    symbol = struct.unpack_from('<4s', payload, 10)[0]
    size = struct.unpack_from('<I', payload, 18)[0]
    price = struct.unpack_from('<Q', payload, 22)[0]
    trade_id = struct.unpack_from('<Q', payload, 30)[0]

    return {
        "timestamp": timestamp,
        "sale_condition_flags": sale_condition_flags,
        "timestamp_trade": timestamp_trade,
        "symbol": symbol,
        "size": size,
        "price": price / 10000,  # Adjust for implied decimal point
        "trade_id": trade_id
    }

# Step 3: Organize and save to HDF5
def save_to_hdf5(trade_reports, hdf5_file):
    with h5py.File(hdf5_file, "w") as h5f:
        for report in trade_reports:
            symbol_group = h5f.require_group(f"trades/{report['symbol']}")
            symbol_group.attrs.update({
                'timestamp': report['timestamp'],
                'size': report['size'],
                'price': report['price'],
                'trade_id': report['trade_id']
            })
        print(f"Data saved to {hdf5_file}")

def parse_iex_pcap(filepath):
    '''pasrse trades from iex pcap file'''

    with open(filepath, 'rb') as f:
        pcap = dpkt.pcap.Reader(f)

        for timestamp, buf in pcap:
            pcap_time = datetime.utcfromtimestamp(timestamp)

            eth = dpkt.ethernet.Ethernet(buf)
            if not isinstance(eth.data, dpkt.ip.IP):
                continue
            ip = eth.data

            if isinstance(ip.data, dpkt.udp.UDP):
                udp = ip.data
                payload = udp.data

            if len(payload) < 40:
                continue
            message_data = payload[40:]

            if len(message_data) >= 38:
                message_type = message_data[2:3].decode('utf-8', errors='ignore')
                if message_type == 'T':
                    sale_condition_flags = message_data[3:4]
                    timestamp_trade = struct.unpack_from('<Q', message_data, 4)[0]
                    symbol = message_data[12:20].decode('ascii', errors='replace').strip()
                    size = struct.unpack_from('<I', message_data, 20)[0]
                    price = struct.unpack_from('<Q', message_data, 24)[0]
                    trade_id = struct.unpack_from('<Q', message_data, 32)[0]

                    trade_report = {
                        "packet_time": pcap_time,
                        "sale_condition_flags": sale_condition_flags,
                        "timestamp_trade": timestamp_trade,
                        "symbol": symbol,
                        "size": size,
                        "price": price / 10000,  # Adjust for decimal placement
                        "trade_id": trade_id
                    }
                    yield trade_report

def parse_pcap_file_with_custom_header(file_path, header_offset=0):
    '''another function'''
    with open(file_path, 'rb') as f:
        # Skip custom headers if present
        if header_offset > 0:
            f.seek(header_offset)

        try:
            pcap = dpkt.pcap.Reader(f)
        except ValueError as e:
            raise Exception(f"Failed to parse pcap file."
                            f"Check custom header offset or file format. Error: {e}")

        for timestamp, buf in pcap:
            # Convert the timestamp
            packet_time = datetime.utcfromtimestamp(timestamp)

            # Parse Ethernet and IP layers
            eth = dpkt.ethernet.Ethernet(buf)
            if not isinstance(eth.data, dpkt.ip.IP):
                continue  # Skip non-IP packets
            ip = eth.data

            # Parse UDP packets only
            if isinstance(ip.data, dpkt.udp.UDP):
                udp = ip.data
                payload = udp.data  # Extract the UDP payload

                # Skip the 40-byte IEX Transport Protocol header
                if len(payload) < 40:
                    continue
                message_data = payload[40:]

                # Parse Trade Report message (0x54)
                if len(message_data) >= 38:
                    message_type = message_data[2:3].decode('utf-8', errors='ignore')
                    if message_type == 'T':
                        sale_condition_flags = message_data[3:4]
                        timestamp_trade = struct.unpack_from('<Q', message_data, 4)[0]
                        symbol = message_data[12:20].decode('ascii', errors='replace').strip()
                        size = struct.unpack_from('<I', message_data, 20)[0]
                        price = struct.unpack_from('<Q', message_data, 24)[0]
                        trade_id = struct.unpack_from('<Q', message_data, 32)[0]

                        trade_report = {
                            "packet_time": packet_time,
                            "sale_condition_flags": sale_condition_flags,
                            "timestamp_trade": timestamp_trade,
                            "symbol": symbol,
                            "size": size,
                            "price": price / 10000,  # Adjust for decimal placement
                            "trade_id": trade_id
                        }
                        yield trade_report

def please_parse(file_path):
    '''foo'''

    offset = {
        'version_offset': 0,
        'reserved_field': 1,
        'msg_protocol_id': 2,
        'channel_id': 4,
        'session_id': 8,
        'payload_len': 12,
        'msg_count': 14,
        'stream_offset': 16,
        'first_msg_seq_no': 24,
        'send_time': 32
    }

    open_type = gzip.open if file_path.endswith('gz') else open
    with open_type(file_path, 'rb') as f:
        pcap_line = f.readline()
        while True:
            if len(pcap_line) >= 80:
                header = (
                    extract('<B', pcap_line, offset['version_offset'])[0],
                    extract('<c', pcap_line, offset['reserved_field'])[0],
                    extract('<H', pcap_line, offset['msg_protocol_id'])[0],
                    extract('<I', pcap_line, offset['channel_id'])[0],
                    extract('<I', pcap_line, offset['session_id'])[0],
                    extract('<H', pcap_line, offset['payload_len'])[0],
                    extract('<H', pcap_line, offset['msg_count'])[0],
                    extract('<q', pcap_line, offset['stream_offset'])[0],
                    extract('<q', pcap_line, offset['first_msg_seq_no'])[0],
                    extract('<q', pcap_line, offset['send_time'])[0]
                )

                message = pcap_line[42:]

                message_type = extract('<B', message, 0)[0]
                sale_condition_flag = extract('<B', message, 1)[0]
                ts = extract('<q', message, 2)[0]
                symbol = extract('<8s', message, 10)[0]
                size = extract('<I', message, 18)[0]

                trade = message_type, sale_condition_flag, ts, symbol

                yield header, trade

            elif len(pcap_line) == 0:
                return

            pcap_line = f.readline()

def parse_pcap_file(filepath):
    '''bar'''
    open_type = gzip.open if filepath.endswith('gz') else open
    offset = {
        'version_offset': 0,
        'reserved_field': 1,
        'msg_protocol_id': 2,
        'channel_id': 4,
        'session_id': 8,
        'payload_len': 12,
        'msg_count': 14,
        'stream_offset': 16,
        'first_msg_seq_no': 24,
        'send_time': 32
    }
    with open_type(filepath, 'rb') as fh:
        iex_header = fh.read(8)
        tp_header = fh.read(40)
        #ts = extract( '<q', iex_header, 32)[0]
        yield iex_header, tp_header

def foo(filepath):
    '''foo!'''
    from os.path import getsize

    dt_from = datetime.fromisoformat("2024-06-01")
    dt_to = datetime.fromisoformat("2024-12-01")

    open_type = gzip.open if filepath.endswith('gz') else open
    with open_type(filepath, 'rb') as fp:
        for i in range(getsize(filepath) - 8):
            # foo
            fp.seek(i)
            ret = fp.read(8)
            ts_ = struct.unpack('<q', ret)[0]
            ts_ = datetime.fromtimestamp(ts_/1000000000)
            if dt_from < ts_ < dt_to:
                print(ts_, fp.tell())
                ret = get_header(fp)
                print(ret)
                payload_length = int.from_bytes(ret[5], "little")
                message_count = int.from_bytes(ret[6], "little")
                print(f"Payload Length: {payload_length}")
                print(f"Message Count: {message_count}")
                if payload_length > 0:
                    for msg_no in range(message_count):
                        pass
                input()

def get_header(fp):
    '''get header from fp'''

    fp.seek(fp.tell() - 40) # Beginning of the header

    version =          fp.read(1)
    reserved =         fp.read(1)
    msg_protocol_id =  fp.read(2)
    channel_id =       fp.read(4)
    session_id =       fp.read(4)
    payload_length =   fp.read(2)
    msg_count =        fp.read(2)
    stream_offset =    fp.read(8)
    first_msg_seq_no = fp.read(8)
    ts =               fp.read(8)

    header = (
        version,
        reserved,
        msg_protocol_id,
        channel_id,
        session_id,
        payload_length,
        msg_count,
        stream_offset,
        first_msg_seq_no,
        ts
    )
    return header


if __name__ == '__main__':
    data_lines = extract_pcap_data(pcap_test_filepath)
    for line in data_lines:
        result = parse_trade_report(line)
        if result:
            print(result)
    print()
    for i in please_parse(pcap_test_filepath):
        print(i)
