import time
import dateparser
import pytz
import json
import pandas as pd
import numpy as np
import math

from datetime import datetime
from dateutil import tz
#from tz import timezone
from binance.client import Client

def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)

#print(date_to_milliseconds("January 01, 2018"))
#print(date_to_milliseconds("11 hours ago UTC"))
print(date_to_milliseconds("now UTC"))

def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

#print(interval_to_milliseconds(Client.KLINE_INTERVAL_1MINUTE))
#print(interval_to_milliseconds(Client.KLINE_INTERVAL_30MINUTE))
#print(interval_to_milliseconds(Client.KLINE_INTERVAL_1WEEK))

def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # create the Binance client, no need for api key
    client = Client("", "")

    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        #if idx % 3 == 0:
        #    time.sleep(1)

    return output_data
    
def macd_flag(cross,pcross):
  
    if cross==1 and pcross==-1:
        return 'BUY'
    elif cross==-1 and pcross==1:
        return 'SELL'
    else:
        return 'None'

def vol_flag(volume,avg_volume):
  
    if volume > (2*avg_volume):
        return 'BUY'
    else:
        return 'None'

def conv_to_date(ms):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime((ms/1000)-18000))

def float_to_int(number):
    if math.isnan(number):
        return 0
    else:
        return int(number)


start = "23 Jan, 2018"
end = "now UTC"
#interval = Client.KLINE_INTERVAL_15MINUTE
#interval = Client.KLINE_INTERVAL_1DAY
interval = Client.KLINE_INTERVAL_1HOUR
print "running"
out_fields = ['Ticker','Period','Open','High','Low','Close','P_Close','Volume','P_Volume','Vol_21_EMA','3 EMA','5 EMA','Signal_Line','Indicator','Vol_Indicator']

client = Client("", "")
tickers =  client.get_all_tickers()
df_tickers = pd.DataFrame(tickers)
df_fields = ['Ticker','UTC_Time_Open','Open','High','Low','Close','Volume','UTC_Time_Close','Quote_asset_volume','Trades','TBBAV','TBQAV','IGNORE']
df = pd.DataFrame(columns=df_fields)
#for symbol in  ['ENJBTC','ENJETH']:
for symbol in  df_tickers['symbol'].unique():
    print symbol

    klines = get_historical_klines(symbol, interval, start, end)
    if len(klines) > 0:
        fields = ['UTC_Time_Open','Open','High','Low','Close','Volume','UTC_Time_Close','Quote_asset_volume','Trades','TBBAV','TBQAV','IGNORE']
        tmp_df = pd.DataFrame(klines)
        tmp_df.columns = fields
        tmp_df['Ticker'] = symbol
        df = df.append(tmp_df)
#    fields = ['UTC_Time_Open','Open','High','Low','Close','Volume','UTC_Time_Close','Quote_asset_volume','Trades','TBBAV','TBQAV','IGNORE']
#    tmp_df = pd.DataFrame(klines)
#    tmp_df.columns = fields
#    tmp_df['Ticker'] = symbol
#    df = df.append(tmp_df)

#fileName = "Binance_{}_{}-{}.json".format(
#        interval,
#        date_to_milliseconds(start),
#        date_to_milliseconds(end))
## open a file with filename including symbol, interval and start and end converted to milliseconds
#with open(
#    fileName, 'w' # set file write mode
#) as f:
#    f.write(json.dumps(klines))

#df_fields = ['UTC_Time_Open','Open','High','Low','Close','Volume','UTC_Time_Close','Quote_asset_volume','Trades','TBBAV','TBQAV','IGNORE']
#df =pd.read_json(fileName)
#df.columns = df_fields
#print df.dtypes
df['Period'] = df.apply(lambda row: conv_to_date(row['UTC_Time_Open']), axis=1)
print(date_to_milliseconds("now UTC"))
df = df.reset_index().sort_values(['Ticker','Period'], ascending=[True, True])
df['Volume']=df['Volume'].apply(float)
df['P_Volume'] = df['Volume'].shift(1)
df['P_Volume'] = df.apply(lambda row: float_to_int(row['P_Volume']), axis=1)
df['P_Close'] = df['Close'].shift(1)
df['5 EMA'] = df.groupby('Ticker')['Close'].apply(lambda x:x.ewm(span=5,min_periods=5).mean()) 
df['3 EMA'] = df.groupby('Ticker')['Close'].apply(lambda x:x.ewm(span=3,min_periods=3).mean())
df['Vol_21_EMA'] = df.groupby('Ticker')['Volume'].apply(lambda x:x.ewm(span=21,min_periods=21).mean())
df['MACD'] = (df['3 EMA'] - df['5 EMA'])
df['Signal_Line'] = df.groupby('Ticker')['MACD'].apply(lambda x:x.ewm(span=13,min_periods=13).mean())
df['MACD_SL_Crossover'] = np.where(df['MACD'] > df['Signal_Line'], 1, 0)
df['MACD_SL_Crossover'] = np.where(df['MACD'] < df['Signal_Line'], -1, df['MACD_SL_Crossover'])
df['P_MACD_Crossover'] = df['MACD_SL_Crossover'].shift(1)
#print df.dtypes

df['Indicator'] = df.apply(lambda row: macd_flag(row['MACD_SL_Crossover'],row['P_MACD_Crossover']), axis=1)
df['Vol_Indicator'] = df.apply(lambda row: vol_flag(row['Volume'],row['Vol_21_EMA']), axis=1)
df=df[out_fields]
df.to_csv(r"/home/ec2-user/binance/data/binance_data.csv", index=False)
tList = df['Period'].tail(1).values

