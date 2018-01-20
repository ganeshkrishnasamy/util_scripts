import urllib2
import pandas as pd
import re
import numpy as np
import math
from bs4 import BeautifulSoup
proxy_support = urllib2.ProxyHandler({"http":"http://user:pwd@proxy.com:port"})
opener = urllib2.build_opener(proxy_support)
urllib2.install_opener(opener)
url_inp = "https://coinmarketcap.com/all/views/all/"
request_headers = {
    "Accept-Language": "en-US,en;q=0.5",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "http://thewebsite1.com",
    "Connection": "keep-alive" 
    }
request = urllib2.Request(url_inp, headers=request_headers)
page = urllib2.urlopen(request).read()

start_date = '20171201'
end_date = '20180118'

#page = urllib2.urlopen(url_inp)
soup = BeautifulSoup(page, 'html.parser')
right_table=soup.find('table', class_='table js-summary-table')
curr_list = []
A=[]
B=[]
C=[]
D=[]
E=[]
F=[]
G=[]
dictBTC = dict()

def ret_pct(close,pclose):
    pc=pclose
    cl=close
#    pc=float(pclose)
#    cl=float(close)
    if pc > 0:
        return (cl-pc)/pc
    else:
        return 0

def vol_inc(volume,pvolume):
    pv = pvolume
    vo=volume
#    pv=int(re.sub('[,]', '', str(pvolume)))
#    vo=int(re.sub('[,]', '', str(volume)))
    if pv > 0:
        return (vo)/pv
    else:
        return 0

def price_vol_flag(Volume, P_Volume, Close, P_Close):
  
    if P_Volume <=0 :
        return 'None'
    else:
#        if int(re.sub('[,]', '', str(Volume))) >= 1.5*int(re.sub('[,]', '', str(P_Volume))) and float(Close) >= float(P_Close):
        if (Volume >= 1.5*P_Volume) and (Close >= P_Close):
            return 'BUY'
        elif(Volume >= 1.2*P_Volume) and (Close < P_Close):
            return 'SELL'
        else:
            return 'None'
        
def macd_flag(cross,pcross):
  
    if cross==1 and pcross==-1:
        return 'BUY'
    elif cross==-1 and pcross==1:
        return 'SELL'
    else:
        return 'None'
    
def str_to_int(number):
    if number == 'nan':
        return 0
    else:
        return int(re.sub('[,]', '', str(number)))
    
def float_to_int(number):
    if math.isnan(number):
        return 0
    else:
        return int(number)

for row in right_table.findAll("tr"):
    cells = row.get('id')
#    print type(cells)
    curr_list.append(str(cells).replace("id-",""))
#print curr_list
overall_df = pd.DataFrame() #creates a new dataframe that's empty
for items in curr_list[:300]:

    if items != 'None':
        url_inp = "https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s" %(items,start_date,end_date)
        request = urllib2.Request(url_inp, headers=request_headers)
        page = urllib2.urlopen(request).read()
        soup = BeautifulSoup(page, 'html.parser')
        right_table=soup.find('table', class_='table')

        A=[]
        B=[]
        C=[]
        D=[]
        E=[]
        F=[]
        G=[]
        for row in right_table.findAll("tr"):
            cells = row.findAll('td')
            states=row.findAll('th') #To store second column data
            if len(cells)==7: #Only extract table body not heading
                A.append(cells[0].find(text=True))
                B.append(cells[1].find(text=True))
                C.append(cells[2].find(text=True))
                D.append(cells[3].find(text=True))
                E.append(cells[4].find(text=True))
                F.append(cells[5].find(text=True))
                G.append(cells[6].find(text=True))
        df=pd.DataFrame(A,columns=['Date'])
        df['Currency'] = items
        df['Open']=B
        df['High']=C
        df['Low']=D
        df['usd_Close']=E
        df['Volume']=F
        df['Market_Cap']=G
#        print df['Date'].head(30)
#        df = df.head(365)      
        df = df.fillna('0')
        df['Date']=pd.to_datetime(df['Date']).dt.date
        df['usd_Close'] = df['usd_Close'].apply(float)
        if items == 'bitcoin':
            dictBTC = dict(zip(df.Date, df.usd_Close))
        df['btc_Close'] = df['Date'].map(dictBTC)
        df['Close'] = df['usd_Close']/df['btc_Close']
        
#        df[['Open','High','Low','Close','Volume','Market_Cap']] = df[['Open','High','Low','Close','Volume','Market_Cap']].applymap(str)
#        df[['Open','High','Low','Close','Volume','Market_Cap']] = df[['Open','High','Low','Close','Volume','Market_Cap']].applymap(pd.to_numeric)
        
        df['Volume'] = df.apply(lambda row: str_to_int(row['Volume']), axis=1)
        df['Close'] = df['Close'].apply(float)
        df['P_Volume'] = df['Volume'].shift(-1)
        df['P_Volume'] = df.apply(lambda row: float_to_int(row['P_Volume']), axis=1)
        df['P_Close'] = df['Close'].shift(-1)
        
        df = df.reset_index().sort_values(['Date'], ascending=True)
#        print df.head(5)
        df['26 ema'] = df.groupby('Currency')['Close'].apply(lambda x:x.ewm(span=5,min_periods=5).mean()) 
        df['12 ema'] = df.groupby('Currency')['Close'].apply(lambda x:x.ewm(span=3,min_periods=3).mean())
        df['MACD'] = (df['12 ema'] - df['26 ema'])
        df['MACD_Signal_Line'] = df['MACD'].ewm(span=13,min_periods=13).mean()
        df['MACD_Width'] = (df['MACD'] - df['MACD_Signal_Line'])/df['MACD_Signal_Line']
        df['MACD_SL_Crossover'] = np.where(df['MACD'] > df['MACD_Signal_Line'], 1, 0)
        df['MACD_SL_Crossover'] = np.where(df['MACD'] < df['MACD_Signal_Line'], -1, df['MACD_SL_Crossover'])
        df['P_MACD_Crossover'] = df['MACD_SL_Crossover'].shift(1)
        df['26 ema'] = df.groupby('Currency')['Close'].apply(lambda x:x.ewm(span=5,min_periods=5).mean()) 
        df['12 ema'] = df.groupby('Currency')['Close'].apply(lambda x:x.ewm(span=3,min_periods=3).mean())
        df['PPO'] = (df['12 ema'] - df['26 ema'])/df['26 ema']
        df['PPO_Signal_Line'] = df['PPO'].ewm(span=13,min_periods=13).mean()
        df['PPO_SL_Crossover'] = np.where(df['PPO'] > df['PPO_Signal_Line'], 1, 0)
        df['PPO_SL_Crossover'] = np.where(df['PPO'] < df['PPO_Signal_Line'], -1, df['PPO_SL_Crossover'])
        
        
        df['Price_Vol_Flag'] = df.apply(lambda row: price_vol_flag(row['Volume'],row['P_Volume'],row['Close'],row['P_Close']), axis=1)
        df['macd_Flag'] = df.apply(lambda row: macd_flag(row['MACD_SL_Crossover'],row['P_MACD_Crossover']), axis=1)
        df['Ret_Pct'] = df.apply(lambda row: ret_pct(row['Close'],row['P_Close']), axis=1)
        df['vol_inc'] = df.apply(lambda row: vol_inc(row['Volume'],row['P_Volume']), axis=1)
        
            
            
        overall_df = overall_df.append(df, ignore_index = True)
#print overall_df[(overall_df.Int_Flag == True) & (overall_df.Date == 'Dec 19, 2017') ]  
overall_df.sort_values(['Date', 'Volume'], ascending=[False, False],inplace=True)
overall_df.to_csv(r"coin.csv", index=False)
