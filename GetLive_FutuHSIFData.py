from futu import *
import time
from datetime import date, timedelta
import sqlite3 as lite

HSIFCode = 'HK.HSImain'
InitTotalBar = 1000
TodayDate = date.today()
TestStartDate = (TodayDate + timedelta(-10)).strftime("%Y-%m-%d")
EndDate = TodayDate.strftime("%Y-%m-%d")

# comment added

conn = lite.connect('c:\\caphttpdata\\algotrades.db') # wsl -> c:
# conn = lite.connect('/home/fred/caphttpdata/algotrades.db') # Linux Cloud

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

ret, data = quote_ctx.request_trading_days(market=TradeDateMarket.HK, start=TestStartDate, end=EndDate)
if ret == RET_OK:
    StartDate = data[len(data)-3]['time']
else:
    print('error:', data)


ret, data, page_req_key = quote_ctx.request_history_kline(HSIFCode, start=StartDate, end=EndDate, ktype=KLType.K_1M, max_count=None)

if ret == RET_OK:

    data[['Date','Time']] = data.time_key.str.split(expand=True)
    data.drop('code', axis=1, inplace=True)
    data.drop('time_key', axis=1, inplace=True)
    data.drop('turnover_rate', axis=1, inplace=True)
    data.drop('turnover', axis=1, inplace=True)
    data.drop('change_rate', axis=1, inplace=True)
    data.drop('last_close', axis=1, inplace=True)
    data.drop('pe_ratio', axis=1, inplace=True)

    data.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Date', 'Time']

    data1 = data[data.Volume != 0]                   # Using logical condition

    #df = data1[['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']]
    print(data1.size)
    lastrow = len(data1['Date'])
    print(lastrow)

    data1 = data1.truncate(before=lastrow-InitTotalBar);
    #k = 472
    #data1.to_csv('test.txt', index = False, header=False)

    sql = "DELETE FROM pricebars"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    sql = "delete from sqlite_sequence where name='pricebars'"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    for i, row in data1.iterrows():
    # create a list representing the dataframe row
        print(i, row['Date'], row['Time'],row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
        TradingDateTime = row['Date'] + ' ' + row['Time'][0:5] + ':00'
        #print(TradingDateTime)
        Open = row['Open']
        High = row['High']
        Low = row['Low']
        Close = row['Close']
        Volume = row['Volume']

        sql = "REPLACE into pricebars (TradingDateTime, Code, Open, High, Low, Close, Volume, IsMinBar) values ('" + TradingDateTime + "', '" + "HSIF" + "', '" + str(Open) + "', '" + str(High) + "', '" + str(Low) + "', '" + str(Close) + "', '" + str(Volume) + "', 'Y')"
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        #time.sleep(0.01)
    # append row list to ls
    #ls.append(row_ls)


    PrevTradingDateTime = TradingDateTime[0:16]
    '''
    PrevOpen = Open
    PrevHigh = High
    PrevLow = Low
    PrevClose = Close
    PrevVolume = Volume
    '''
    LastBarOpen = Open
    LastBarHigh = High
    LastBarLow = Low
    LastBarClose = Close
    LastBarVolume = Volume



i=1
firsttickdata = 1
while i<100000 :
    #print(quote_ctx.get_market_snapshot('HK.HSI2110'))  # Get market snapshot for HK.00700
    # ret, pddata = quote_ctx.get_market_snapshot('HK.HSI2111')
    ret, pddata = quote_ctx.get_market_snapshot(HSIFCode)

    if ret == RET_OK:
        #print(pddata)

        print(pddata['update_time'][0], pddata['code'][0], 'Price =', pddata['last_price'][0], 'Volume = ', pddata['volume'][0])
        #print(pddata['code'][1], pddata['update_time'][1], pddata['last_price'][1], pddata['option_implied_volatility'][1],  pddata['ask_price'][1])
        #quote_ctx.close()
        #get_order_book('HK.HSI2203', num=10)
        CurrentTradingDateTime = pddata['update_time'][0][0:16]
        CurrentPrice = pddata['last_price'][0]
        CurrentDayVolume = pddata['volume'][0]

        TradingDateTime = CurrentTradingDateTime + ':00'
        #print(CurrentTradingDateTime, PrevTradingDateTime)

        if CurrentTradingDateTime != PrevTradingDateTime :
            if firsttickdata == 1 :
                firsttickdata = 0
                Volume = 1
                PrevDayVolume = CurrentDayVolume + 1
            else :
                #Close = TempClose
                Volume = (CurrentDayVolume - PrevDayVolume)
                #sql = "REPLACE into pricebars (TradingDateTime, Code, Open, High, Low, Close, Volume, IsMinBar) values ('" + TradingDateTime + "', '" + "HSIF" + "', '" + str(Open) + "', '" + str(High) + "', '" + str(Low) + "', '" + str(Close) + "', '" + str(Volume) + "', 'Y')"
                #cur = conn.cursor()
                #cur.execute(sql)
                #conn.commit()

            #Insert
            print('new bar')

            Open = CurrentPrice
            High = CurrentPrice
            Low = CurrentPrice
            Close = CurrentPrice

            sql = "REPLACE into pricebars (TradingDateTime, Code, Open, High, Low, Close, Volume, IsMinBar) values ('" + TradingDateTime + "', '" + "HSIF" + "', '" + str(Open) + "', '" + str(High) + "', '" + str(Low) + "', '" + str(Close) + "', '" + str(Volume) + "', 'Y')"
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            PrevTradingDateTime = CurrentTradingDateTime
            PrevDayVolume = CurrentDayVolume
        else :
            if firsttickdata == 1 :
                firsttickdata = 0
                Open = LastBarOpen
                High = LastBarHigh
                Low = LastBarLow

                Volume = LastBarVolume

                if CurrentPrice > High:
                    High = CurrentPrice
                if CurrentPrice < Low:
                    Low = CurrentPrice

                Close = CurrentPrice
                #Volume = LastBarVolume + 1

                sql = "REPLACE into pricebars (TradingDateTime, Code, Open, High, Low, Close, Volume, IsMinBar) values ('" + TradingDateTime + "', '" + "HSIF" + "', '" + str(Open) + "', '" + str(High) + "', '" + str(Low) + "', '" + str(Close) + "', '" + str(Volume) + "', 'Y')"
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()

                PrevDayVolume = CurrentDayVolume
            else :
                if PrevDayVolume != CurrentDayVolume :
                    print('UPDATE BAR DATA')
                    if CurrentPrice > High:
                        High = CurrentPrice
                    if CurrentPrice < Low:
                        Low = CurrentPrice

                    Close = CurrentPrice
                    Volume = Volume + (CurrentDayVolume - PrevDayVolume)
                    #TempClose = CurrentPrice

                    sql = "REPLACE into pricebars (TradingDateTime, Code, Open, High, Low, Close, Volume, IsMinBar) values ('" + TradingDateTime + "', '" + "HSIF" + "', '" + str(Open) + "', '" + str(High) + "', '" + str(Low) + "', '" + str(Close) + "', '" + str(Volume) + "', 'Y')"
                    cur = conn.cursor()
                    cur.execute(sql)
                    conn.commit()

                    PrevDayVolume = CurrentDayVolume


    else :
        print('error')

    time.sleep(0.5)


    i=i+1



quote_ctx.close()
