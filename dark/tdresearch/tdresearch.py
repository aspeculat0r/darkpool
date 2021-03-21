from bs4 import BeautifulSoup as bs
import requests
from dark.funcs import *
from math import nan


def get_iown(df: pd.DataFrame):
    """Function to get the institutional ownership from TDA research website"""

    # Make a new column  of Iown and assign a value of 1
    df['Iown'] = 0
    # Get a series of all the symbols in dataframe
    symbols = df.Symbol
    print("Getting Market Cap")
    # Iterate over the symbols
    for indx, ticker in enumerate(symbols):
        try:
            # URL for research page
            url = r'https://research.tdameritrade.com/grid/public/research/stocks/fundamentals?symbol={}'.format(ticker)
            # Store page in a get request
            page = requests.get(url)
            # Make a soup object
            soup = bs(page.content, 'lxml')
            # Find the heading where text = '% Helo by Institions', then find nextheading 'dd' and get the string under
            # the heading 'dd'
            ans = soup.find('h4', text='% Held by Institutions').findNext('dd').string
            # Convert the string to float object
            ans = float(ans.split('%')[0])
            # Assign the value to respective index
            df.loc[df.Symbol == ticker, 'Iown'] = ans

            print(ticker, '     ', ans)

        except Exception as _:
            # If exception assign value of nan
            print("Error in ", ticker, "   ", _)
            df.loc[df.Symbol == ticker, 'Iown'] = nan
            print(ticker, '     ', 'N/A')
    # Store the Dataframe at harcoded location
    df.to_csv(r'E:\Github\dpool\bin\dfc_own.csv', index=False)

    return df


def get_iown_single(symbol: str):
    """Function to get the institutional ownership from TDA research website for a single stock"""
    print(symbol)
    try:
        # URL for research page
        url = r'https://research.tdameritrade.com/grid/public/research/stocks/fundamentals?symbol={}'.format(symbol)
        # Store page in a get request
        page = requests.get(url)
        # Make a soup object
        soup = bs(page.content, 'lxml')
        # Find the heading where text = '% Helo by Institions', then find nextheading 'dd' and get the string under
        # the heading 'dd'
        ans = soup.find('h4', text='% Held by Institutions').findNext('dd').string
        # Convert the string to float object
        ans = float(ans.split('%')[0])

    except Exception as _:
        # If exception assign value of nan
        print("Error in ", f'{symbol}', _)
        ans = 0
    return ans


if __name__ == '__main__':

    get_iown(return_sorted_df(r'E:\Github\dpool\bin\dfc.csv'))
