import pandas as pd
import yfinance as yf
import ta
import talib
from binance.client import Client

def on_balance_volume(data, trend_periods=21, close_col='Close', vol_col='Volume'):
    
    for index, row in data.iterrows():
        if index > 1:
            last_obv = data.at[index - 1, 'obv']
            if float(row[close_col]) > float(data.at[index - 1, close_col]):
                current_obv = float(last_obv) + float(row[vol_col])
            elif float(row[close_col]) < float(data.at[index - 1, close_col]):
                current_obv = float(last_obv) - float(row[vol_col])
            else:
                current_obv = float(last_obv)
        else:
            last_obv = 0
            current_obv = row[vol_col]
            
        
        data.at[index, 'obv']= current_obv 

    
    
    return data

apikey = ""
secretkey = ""
client = Client(apikey, secretkey)
choose = int(input('1- TASI\n2- NASDAQ\n3- Binance\nChoose:'))

listOfApprovedStocks=[]
if choose == 1:
    stockList = pd.read_csv('stockList.csv')
elif choose == 2:
    stockList = pd.read_csv('nasdaqList.csv')
elif choose == 3:
    stockList = pd.read_csv('binanceList.csv')
else:
    quit()
    
    

for i in range(len(stockList)):
    if choose == 1:
        ticker = yf.Ticker(str(stockList.iloc[i].iloc[0])+'.SR')#
      
        df = ticker.history(period="max", interval='1d')
        
    elif choose == 2:
        ticker = yf.Ticker(str(stockList.iloc[i].iloc[0]))#+'.SR'
        df = ticker.history(period="max")
        
    elif choose == 3:
        bars = client.get_klines(symbol=str(stockList.iloc[i].iloc[0]),interval='1d', limit=1000)
       
        df = pd.DataFrame(bars,columns=['openTime','Open','High','Low','Close','Volume','CloseTime','qut','num','takerbase','takequt','igno'])
        df.to_csv('history.csv')
        del df['openTime']
        del df['CloseTime']
        del df['qut']
        del df['num']
        del df['takerbase']
        del df['takequt']
        del df['igno']
        df['Close'] = pd.to_numeric(df['Close'], downcast="float")

    try:    
        del df['Stock Splits'] 
        del df['Dividends']
    except:
        if choose != 3:
            continue
        else:
            pass
    
    rsiCal=ta.momentum.rsi(pd.Series(df['Close']), 14)
    df=df.join(rsiCal)
    df.to_csv('history.csv')
    df=pd.read_csv('history.csv')
    df = on_balance_volume(df)
    
    sma10=pd.Series(df['obv']).rolling(window=10).mean()
    sma10 = pd.DataFrame({'sma10':sma10})
    df = df.join(sma10)
    
    sma20=pd.Series(df['obv']).rolling(window=20).mean()
    sma20 = pd.DataFrame({'sma20':sma20})
    df = df.join(sma20)
    
    sma50=pd.Series(df['obv']).rolling(window=50).mean()
    sma50 = pd.DataFrame({'sma50':sma50})
    df = df.join(sma50)
    
    ema7 = talib.EMA(df['Close'], timeperiod=7)
    ema7 = pd.DataFrame({'ema7':ema7})
    df = df.join(ema7)
    
    sma20Bars=pd.Series(df['Close']).rolling(window=20).mean()
    sma20Bars = pd.DataFrame({'sma20Bars':sma20Bars})
    df = df.join(sma20Bars)
    
    
    if df.iloc[-1].iloc[7]>df.iloc[-1].iloc[8] and df.iloc[-1].iloc[8]>df.iloc[-1].iloc[9] and df.iloc[-1].iloc[9]>df.iloc[-1].iloc[10] and df.iloc[-1].iloc[11] > df.iloc[-1].iloc[12]:
        appendFlag = False
        for j in range(-3, 0):    
            if df.iloc[j].iloc[11] < df.iloc[j].iloc[12]:
                appendFlag = True
        if appendFlag:
            listOfApprovedStocks.append(stockList.iloc[i].iloc[0]) 
print('\n'*100)
for i in range(len(listOfApprovedStocks)):
    print(i+1,'- ',listOfApprovedStocks[i])