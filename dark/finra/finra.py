import requests
from time import time
from math import nan
from urllib.request import Request, urlopen
from dark.finra.utils import months
from _datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os


class Finra(object):
    """Class to download the data from from the finra website for short sales on dark pools"""

    # File type for the short data
    filetype = 'CNMS'

    def __init__(self, outpath: str, runpath: str):
        """
        Initialize the Finra class
        :param outpath: path to dump the data from website
        :param runpath: path to save the new csv files from pandas
        """
        self.outpath = outpath
        self.last_runpath = runpath
        month = datetime.today().strftime('%Y%m%d')[4:6]
        self.finra_url = "http://regsho.finra.org/regsho-{}.html".format(months[month])

    @property
    def last_run(self):
        """Get the last run date from the text file name"""
        # Get the files in the directory in list
        files = os.listdir(self.outpath)
        # Sort the list by reverse
        files.sort(reverse=True)
        # Return the date from the file
        # print(files[0][-12:-4])
        return files[0][-12:-4]

    @staticmethod
    def get_files(url: str, dtype: str, outpath: str):
        """
        Method to get the files from the url for a particular file type
        :param url: url for the request
        :param dtype: starting name for the files -- Ex. 'CNMS' download all files starting with 'CNMS
        :param outpath: local path to store the files
        :return:
        """
        # Crate an empty list to append the file names on url
        ans = []
        print(url)
        # Send a request to the url
        req = Request(url, headers={'User-Agent': 'Mozilla'})
        # Open the url to read the file
        a = urlopen(req).read()
        # Make a soup to read the data
        soup = BeautifulSoup(a, 'html.parser')
        # Search for the file type in the soup and if true append to the ans list
        for a in soup.find_all(href=True):
            # Href in html is used for links to the files so search for href
            if dtype in a['href']:
                ans.append(a['href'])
        # Download all the files in the ans list
        for fname in ans:
            # Create a file name in the out path
            filename = outpath + r'\\' + fname.split(r'/')[-1]
            # Send a get request to get the data from the server
            r = requests.get(fname, allow_redirects=True)
            # IO operation to download the data
            print(filename)
            with open(filename, 'wb+') as fileout:
                fileout.write(r.content)

        return 1

    def get_data(self):
        """Method to download the files if the code is not run today"""
        date_today = datetime.today().strftime('%Y%m%d')
        time_now = datetime.now().strftime("%H:%M:%S")
        # Only download if the time now is > 5 P.M
        print(str(date_today) == self.last_run)
        print(str(date_today))
        if date_today != self.last_run and int(time_now[0:2]) >= 17:
            print('Downloading files from Finra')

            # self.get_files(url=self.finra_url, dtype=Finra.filetype, outpath=self.outpath)
            pass

        for i in months.values():
            url = "http://regsho.finra.org/regsho-{}.html".format(i)
            self.get_files(url=url, dtype=Finra.filetype, outpath=self.outpath)
        return 1

    def filter_by_vol(self, interval: int = 7, volume_filter: int = 1000000):
        """
        Method to perform the data operations on the downloaded data to get total volume over interval
        :param volume_filter: Integer to filter the volumnes
        :param interval: interger
        :return:
        """
        # List of file names in the outpath
        fnames = os.listdir(self.outpath)

        # Sort the filenames in reverse order
        fnames.sort(reverse=True)

        # Store the data frames in dictionary
        dic = dict()

        for i, file in enumerate(fnames[0:interval]):
            # Read the file by separator | and make a dataframe
            df = pd.read_csv(self.outpath + "\\" + file, sep="|")

            # Drop the na and make make the dataframe current
            df.dropna(inplace=True)

            # Change the Total Volume type to integer
            df['TotalVolume'] = df['TotalVolume'].astype(int)

            # Filter the stocks where total volumne> volume filter
            df = df[df['TotalVolume'] > volume_filter]

            # Append the dataframe to the dictionary
            dic['df' + str(i)] = df

        # Concat all the data frames in the dictionary and groupby Symbols
        ans = pd.concat(list(dic.values())).groupby('Symbol')[
            ['TotalVolume', 'ShortExemptVolume', 'ShortVolume']].sum().reset_index()

        return ans

    def data_organize(self, filter_volume: int = 10 ** 6):
        """Funtion to organize the data where the tickers are column and unique id"""
        # Read the files to a list
        files = os.listdir(self.outpath)
        # Sort the list of the files
        files.sort(reverse=True)
        # Column names for the dataframe
        # columns = [x[9:13] + '-' + x[13:15] + '-' + x[15:17] for x in files]
        # Make a empty dataframe for short volume
        dsv = pd.DataFrame()
        # Make an empty dataframe for the Total Volume
        dtv = pd.DataFrame()
        # Read the first file to get the tickers with volumen > filter volume into temp datframe
        temp = pd.read_csv(self.outpath + '\\' + files[0], sep='|')
        # Filter the stocks where volumne > filter val
        temp = temp[temp['TotalVolume'] > filter_volume]
        dsv['Symbol'] = temp['Symbol']
        dtv['Symbol'] = temp['Symbol']

        for indx, file in enumerate(files):
            # Column name for the file read in the database
            column_name = file[9:13] + '-' + file[13:15] + '-' + file[15:17]
            # Read the csv file into temp2 datframe
            temp2 = pd.read_csv(self.outpath + '\\' + file, sep='|')
            # Filter the stocks where volumne > filter val
            temp2 = temp2[temp2['Symbol'].isin(list(temp['Symbol']))]
            # Missing values check
            if len(temp['Symbol']) != len(temp2['Symbol']):
                missing = list(set(temp['Symbol']).difference(set(temp2['Symbol'])))
                for ticker in missing:
                    temp2 = temp2.append({'Symbol': ticker,
                                          'Date': file[9:17],
                                          'ShortVolume': nan,
                                          'ShortExemptVolume': nan,
                                          'TotalVolume': nan,
                                          'Market': nan}, ignore_index=True)
            # Sort the value by the Symbol
            temp2.sort_values(by='Symbol', inplace=True)
            # Reset the index (must be done)
            temp2.reset_index(inplace=True, drop=True)
            # Append the data to the dataframes
            dsv[column_name] = temp2['ShortVolume']
            dtv[column_name] = temp2['TotalVolume']

        # Save the data to the csvs after looping

        dsv.to_csv(r'E:\Github\dpool\bin\ShortVolume.csv', index=False)
        dsv.to_csv(r'E:\Github\dpool\bin\TotalVolume.csv', index=False)


if __name__ == '__main__':
    t1 = time()

    cls = Finra(outpath=r'E:\Github\dpool\bin\data', runpath=r'E:\Github\dpool\bin')
    cls.get_data()
    # df = cls.filter_by_vol(interval=7, volume_filter=10000000)
    # df.to_csv(r'E:\Github\dpool\bin\filter_data.csv', index=False)
    # cls.data_organize()

    print('Time = ', time() - t1)
