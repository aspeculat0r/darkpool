import quandl
quandl.ApiConfig.api_key = 'WRyzB4Ssg3ajHB4gPGnV'

# quandl.ApiConfig.verify_ssl = False

data = quandl.get('WIKI/AAPL', start_date="2020-01-01", end_date="2021-04-13")