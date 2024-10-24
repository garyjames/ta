import matplotlib.pyplot as plt

# Display a 5x5 grid of stock symbols' MACD results
def display_macd_chart(filtered_symbols):
    fig, axs = plt.subplots(5, 5)
    for i, symbol in enumerate(filtered_symbols):
        ax = axs[i // 5, i % 5]
        ax.plot(symbol['macd'], label='MACD')
        ax.plot(symbol['signal'], label='Signal')
        ax.set_title(symbol['symbol'])
    plt.show()
