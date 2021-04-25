from dark.finra.dbase import Dbase
import matplotlib.pyplot as plt
from dark.tdresearch.tdresearch import get_iown_single
import yfinance as yf
import pandas as pd
import requests
from yahooquery import Ticker

window_ = 10
symbol_ = 'MSFT'


def make_plot_for_ticker(symbol: str, window: int, length: int):
    # Crete a class for the database
    dconnection = Dbase()
    # get the data for the ticker
    df = dconnection.get_by_ticker(ticker=symbol)
    # Make a rolling mean for the short ratio
    df['rollingratio'] = df['shortratio'].rolling(window).mean()

    start_ = str(df['tradedate'].iloc[-1])
    end_ = str(df['tradedate'].iloc[0])

    # Get the stock data from yahoo query
    ans = Ticker(symbol, formatted=True, asynchronous=True).history(interval='1d', start=start_)
    # Reset the multilevel index to single level index
    ans.reset_index(level=0, inplace=True)
    # Make the length of the dataframe to be same
    ans = ans[0:len(df)]
    # Sort the index of the dataframe
    ans.sort_index(ascending=False, inplace=True)

    # Make the volume ratio column
    ans['volumeratio'] = df['totalvolume'].to_numpy() / ans['volume'].to_numpy()
    ans['rollingratio'] = ans['volumeratio'].rolling(window).mean()

    print('\n', 'Length of sql data = ', len(df), '\n', 'Length of yahoo data =', len(ans))
    # Raise assertion error
    assert list(ans.index) == list(df.tradedate)
    # Plot object
    fig, ax1 = plt.subplots()

    # Make a twin axis
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()

    # Plot the data on the ax1
    ax1.plot(df['tradedate'], df['rollingratio'], '--', color='tab:red', linewidth=3.0, label='ShortRatio')
    # Set formating for ax1
    # Set the x and y limits and rotate the ticks by 90 degree
    ax1.set_ylim(0.2, 0.8)
    ax1.xaxis.set_tick_params(rotation=90)
    ax1.set_xlabel('Trade Dates')
    ax1.set_ylabel('Short Ratio from RegSHO')
    # ax1.fill_between(df['tradedate'], df['rollingratio'] + 0.1, df['rollingratio'] - 0.1, alpha=0.5, color='yellow')

    # Plot the data on the ax2
    ax2.plot(ans.index, ans['close'], '-', linewidth=3.0, label='Stock Price')
    ax2.set_ylim()
    ax2.set_ylabel('Daily Close for the Stock')

    # Plot the data on the ax3
    ax3.spines['right'].set_position(('axes', 1.1))
    # ax3.scatter(ans.index, ans['volumeratio'], marker='^')
    ax3.plot(ans.index, ans['rollingratio'], '--', linewidth=3.0, color='tab:green', label='VolumeRatio')
    ax3.set_ylim(0.2, 0.8)
    ax3.set_ylabel('Volume Ratio Total vs Short')
    # ax3.fill_between(ans.index, ans['rollingratio'] + 0.1, ans['rollingratio'] - 0.1, alpha=0.5)
    iown = get_iown_single(symbol=symbol_)
    # Set title of the subplot and pass the tight layout argument
    fig.suptitle(f'Volume vs ShortRatio for {symbol} \n Ownership = {iown}')
    plt.tight_layout()
    fig.legend(loc='lower right')
    dconnection.close()
    return 1


make_plot_for_ticker(symbol=symbol_, window=window_, length=0)

# #
# ticker = 'AAPL'
# url = f'https://finance.yahoo.com/quote/{ticker}/history?period1=1585699200&period2=1617840000&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true'
# #
# # # x = pd.read_html(url)
# base = 'https://query1.finance.yahoo.com'
# #
# url = "{}/v8/finance/chart/{}".format(base, ticker)
# proxy = 2
# if proxy is not None:
#     if isinstance(proxy, dict) and "https" in proxy:
#         proxy = proxy["https"]
#     proxy = {"https": proxy}
#
# param = {'validrange': '1d'}
# data = requests.get(url=url, params=param)
#
# data = data.json()
#
#
# # x = pd.read_json(data, params=param )
#
# print(len(data['chart']['result'][0]['indicators']['quote'][0]['close']))
# # # contains data
# # data['chart']['result'][0]['indicators']['quote'][0].keys()
# #
# # # contains time stamps
# # data['chart']['result'][0]['timestamp']
