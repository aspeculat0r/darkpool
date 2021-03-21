import yfinance as yf
import pandas as pd
from dark.tdresearch import *
from dark.data import combine_data


def get_volume(symbols: list or pd.DataFrame, dfc_ret: pd.DataFrame, dfv_ret: pd.DataFrame, outpath: str):
    """Function to get the candles of symbols (open, high, low, close)
    the fucntion will return the dataframe of the colse and dataframe of the volume
    """

    print("Getting Volume from Yahoo Finance")

    for ticker in symbols:
        try:
            # Make a ticker object to get the data from yahoo finance
            ticker_object = yf.Ticker(ticker)

            # Dictionary with the info about the ticker
            info = ticker_object.info

            # Get the history for close price for last 6 months
            data = ticker_object.history(period='6mo')['Close']

            # Make a new dataframe and transpose it to make columns as dates'
            data = pd.DataFrame(data).T

            # Get the ownership for the stock from TDA
            try:
                val = get_iown_single(symbol=ticker)
                if val == 0:
                    val = info['heldPercentInstitutions']
                    print(f'Getting data for {ticker} from yfin')
            except Exception as _:
                val = 0
            # Add the Symbol, Iown and marketCap Column
            data['Symbol'] = ticker
            data['Iown'] = val
            data['MarketCap'] = info['marketCap']

            # Concat to the existing dataframe
            dfv_ret = pd.concat([dfv_ret, data])
        except Exception as _:
            print(_)
    dfv_ret.to_csv(outpath + r'\\' + 'dfv_.csv')
    dfc_ret.to_csv(outpath + r'\\' + 'dfc_.csv')

    return dfv_ret


if __name__ == '__main__':
    from time import time

    t1 = time()

    print('Running Finance')

    x = combine_data()['Symbol']

    get_volume(symbols=x, dfv_ret=pd.DataFrame(),
               dfc_ret=pd.DataFrame(), outpath=r'E:\Github\dpool\bin')

    print("Run Time = ", round(time() - t1))
