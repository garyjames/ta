from data.parse_data import get_parser, iter_trades, get_df
from models.macd_analysis import calculate_macd
from utils.chart_display import display_macd_chart
from utils.zoom_control import adjust_zoom
from utils.terminal_interface import terminal_interface
from utils.hdf5_handler import (get_single_date,
                                get_daterange,
                                save_df_to_hdf5,
                                trades_to_hdf5,
                                show_h5py_hdf5,
                                show_pd_hdf5)

def main():
    args = terminal_interface()

    # Handle save command
    if args.save_to_hdf5:
        trades = iter_trades(get_parser(args.pcap_filepath))
        trades_to_hdf5(trades, args.h5_filepath)

    elif args.show_h5py_hdf5:
        show_h5py_hdf5(args.filepath)

    elif args.show_pd_hdf5:
        show_pd_hdf5(args.filepath)

    elif args.get_df_from_pcap:
        df = get_df(args.get_df_from_pcap)

    elif args.get_df_from_h5:
        df = get_df(args.get_df_from_h5)

    # Handle MACD analysis for a symbol with period
    elif args.symbol:
        start_date, end_date = parse_period_to_dates(args.period)
        df = load_hdf5_for_period(start_date, end_date)
        df = df[df['symbol'] == args.symbol]
        df = calculate_macd(df)
        df = adjust_zoom(df, args.precision)
        display_macd_chart(df)

    elif args.test_args:
        print(f'\ttesting args\t{args.test_args}\n{args}')

    elif args.save:
        df = get_df(args.filepath)
        save_df_to_hdf5(df, args.date)
        print(f"Saved parsed data from {args.save}")


if __name__ == "__main__":
    main()
