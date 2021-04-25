from time import time
import aiohttp
import asyncio
import pandas as pd

baseurl = r'https://finance.yahoo.com/quote/{}'


async def fetch_volume(session, ticker: str, vol: list, cap: str):

    async with session.get(baseurl.format(ticker)) as response:
        if response.status == 200:
            ans = await response.text()

            df = pd.read_html(ans)
            vol.append(df[0][1][6])
            cap.append(df[1][1][0])
            print(ticker, 'Response=200', df[0][1][6], df[1][1][0])
        else:
            print(ticker, 'Response=404',  0, 0)
            vol.append(0)
            cap.append(0)

        # Vol
        # print(y[0][1][6])
        # # Cap
        # print(y[1][1][0])
        return vol, cap


async def main_volume(symbols: pd.Series):
    vol = []
    cap = []
    async with aiohttp.ClientSession() as session:
        y = await asyncio.gather(*[fetch_volume(session, ticker, vol=vol, cap=cap) for ticker in symbols])

    return vol, cap

if __name__ == '__main__':
    t1 = time()
    x = pd.read_csv(r'E:\Github\dpool\bin\filter_data.csv')
    loop = asyncio.get_event_loop()
    a, b = loop.run_until_complete(main_volume(symbols=x['Symbol'].tolist()[1:10]))
    print('time = ', time() - t1)
