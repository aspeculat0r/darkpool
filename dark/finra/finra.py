import requests
from urllib.request import Request, urlopen
from dark.constants import months
from _datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os


class Finra(object):
    """Class to download the data from from the finra website for short sales on dark pools"""

    # File type for the short data
    filetype = 'CNMS'

    def __init__(self, outpath: str, runpath: str):
        self.outpath = outpath
        self.last_runpath = runpath
        month = datetime.today().strftime('%Y%m%d')[4:6]
        self.finra_url = "http://regsho.finra.org/regsho-{}.html".format(months[month])

    @property
    def last_run(self):
        """Get the last run date from the text file"""
        with open(self.last_runpath + r'\last_run.txt', 'r+') as file:
            last = file.readlines()[0]
        return last

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
        if date_today != self.last_run or int(time_now[0:2]) >= 17:
            print('Downloading files from Finra')

            self.get_files(url=self.finra_url, dtype=Finra.filetype, outpath=self.outpath)

            # Open the last_run file and write the latest download date
            with open(r'E:\Github\dpool\bin\last_run.txt', 'w+') as file:
                file.write(date_today)

        return 1

    def combine_data(self, interval: int = 25, volume_filter: int = 1000000):
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


if __name__ == '__main__':
    cls = Finra(outpath=r'E:\Github\dpool\bin\data', runpath=r'E:\Github\dpool\bin')
    cls.get_data()
    df = cls.combine_data()

    pass