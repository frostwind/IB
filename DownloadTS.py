from ib_insync import *
from mysql.connector import (connection)
import time
import mysql
import timeit
from datetime import datetime,  timedelta
import xml.etree.ElementTree as ET
import tushare as ts
import pandas as pd
import random
from datetime import date


db_config = {
'user': 'root',
'password': 'xpxp',
'host': 'localhost',
'database': 'ibts',
'raise_on_warnings': True
}
insertKBarSQL = "replace into hist_price (symbol,exchange,barsize,trade_date,open,high,low,close,volumn) " \
                 "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
insertFQSQL = "replace into fuquan (symbol,exchange,trade_date,adj_factor) values (%s,%s,%s,%s)"
global pro
global rand_range
    
def markInactive(contract,ib,conn):
    cursor = conn.cursor()
    cursor.execute("update stock set active=False where symbol=%s and exchange=%s ", 
                   (contract.symbol,contract.exchange,))
    conn.commit()
    cursor.close()
    
def downloadFundamentalIfNeeded(contract,ib,conn):
    cursor = conn.cursor()
    cursor.execute("select * from stock where symbol=%s and exchange=%s ", 
                   (contract.symbol,contract.exchange,))
    
    for (symbol,exchange,category,marketcap,active,last_modified,name,) in cursor:
        pass
    cursor.close()
    
    
    if (marketcap is None or last_modified < datetime.now() - timedelta(days=30)):
        if random.randrange(rand_range)==0:
            print("downloading market cap for "+contract.symbol)
        result = ib.reqFundamentalData(contract, 'ReportSnapshot')
        mktcap = None
        if (len(result)>0):
            root = ET.fromstring(result)
            for ratio in root.iter('Ratio'):
                if (ratio.attrib['FieldName']=='MKTCAP'):
                    mktcap=float(ratio.text)
        
        if mktcap:
            cursor = conn.cursor()
            print("haha")
            cursor.execute("update stock set mktcap=%s , last_modified=current_timestamp where symbol=%s and exchange=%s ", 
                   (mktcap,contract.symbol,contract.exchange,))
            conn.commit()
            cursor.close()
            

def downloadFuquan(conn):
    cursor = conn.cursor()
    cursor.execute("select max(max_trdate) from "+
        "(select max(trade_date) as max_trdate, symbol,exchange from fuquan group by symbol,exchange ) fq,"+
        " (select * from stock where exchange in ('SEHK','SEHKNTL','SEHKSZSE','CHINEXT')) s "+
        " where fq.symbol=s.symbol and fq.exchange=s.exchange ")
    max_max_trdate = None
    for (trdate, ) in cursor:
        max_max_trdate=trdate
    cursor.close()
    
    begin_time = date.today() - timedelta(days=365)
    if max_max_trdate is not None:
        begin_time=max_max_trdate - timedelta(days=7)
#     begin_time=begin_time.replace(hour=0, minute=0, second=0)
    while begin_time < date.today():
        print("downloading Fuquan for :"+str(begin_time))
        df = pro.adj_factor(ts_code='', trade_date=begin_time.strftime("%Y%m%d"))
        saveFQToDB(df,conn)
        begin_time=begin_time+timedelta(days=1)
        
def saveFQToDB(df,conn):
    if df is None:
        return
    cursor = conn.cursor()
    cursor.execute("select symbol,exchange from stock where exchange in ('SEHK','SEHKNTL','SEHKSZSE','CHINEXT') ")
    pool =set()
    for (symbol,exchange,) in cursor:
        key = symbol+"-"+exchange
        pool.add(key)
    cursor.close()
    cursor = conn.cursor()
    for x in range(0 , len(df)):
        symbol,exchange=trimSymbol(df['ts_code'][x])
        key = symbol+"-"+exchange
        if key in pool:
            try:
                cursor.execute(insertFQSQL,(symbol,exchange,
                    df['trade_date'][x],float(df['adj_factor'][x]),))
            except Exception:
                print(df.iloc[[x]])
    conn.commit()
    cursor.close()

def downloadCNHKStockKbar(conn):
    cursor = conn.cursor()
    cursor.execute("select max(max_trdate) from "+
        "(select max(trade_date) as max_trdate, symbol,exchange from hist_price group by symbol,exchange ) hp,"+
        " (select * from stock where exchange in ('SEHK','SEHKNTL','SEHKSZSE','CHINEXT')) s "+
        " where hp.symbol=s.symbol and hp.exchange=s.exchange ")
    max_max_trdate = None
    for (trdate, ) in cursor:
        max_max_trdate=trdate
    cursor.close()
    
    begin_time = datetime.now() - timedelta(days=365)
    if max_max_trdate is not None:
        begin_time=max_max_trdate - timedelta(days=7)
    begin_time=begin_time.replace(hour=0, minute=0, second=0, microsecond=0)
    while begin_time < datetime.now():
        print("downloading kbar for :"+str(begin_time))
        bars=pro.hk_daily(trade_date=begin_time.strftime("%Y%m%d"))
        saveTSBarToDB(bars,conn)
        bars=pro.daily(trade_date=begin_time.strftime("%Y%m%d"))
        saveTSBarToDB(bars,conn)
        begin_time=begin_time+timedelta(days=1)
        
def saveTSBarToDB(df,conn):
    if df is None:
        return
    cursor = conn.cursor()
    cursor.execute("select symbol,exchange from stock where exchange in ('SEHK','SEHKNTL','SEHKSZSE','CHINEXT') ")
    pool =set()
    for (symbol,exchange,) in cursor:
        key = symbol+"-"+exchange
        pool.add(key)
    cursor.close()
    cursor = conn.cursor()
    for x in range(0 , len(df)):
        symbol,exchange=trimSymbol(df['ts_code'][x])
        key = symbol+"-"+exchange
        if key in pool:
            try:
                cursor.execute(insertKBarSQL,(symbol,exchange,
                  '1 day',df['trade_date'][x],float(df['open'][x]),float(df['high'][x]),float(df['low'][x]),
                    float(df['close'][x]),int(df['vol'][x]),))
            except Exception:
                print(df.iloc[[x]])
    conn.commit()
    cursor.close()
#ts_code trade_date   open   high    low  close  pre_close  change pct_chg         vol       amount 
        
    

    
def downloadStockKBar(contract, ib,conn):
    """download daily kbar and save to DB"""
#     headTS=ib.reqHeadTimeStamp(contract,whatToShow='TRADES',useRTH=True)
    if random.randrange(rand_range)==0:
        print("downloading kBar: "+contract.symbol+" "+contract.exchange)

    cursor=conn.cursor()
    if  True:
        max_trade_date = None
        cursor.execute("select max(trade_date) from hist_price where symbol=%s and exchange=%s",
                       (contract.symbol,contract.exchange,))
        for (trade_date,) in cursor:
            max_trade_date = trade_date
        cursor.close()
#         print(max_trade_date)
            
        days_threshold = 300  # if newest data is older than this days, we delete all record and redownload everything
        
        datediff = None
        if max_trade_date is not None:
            datediff = datetime.now()-max_trade_date
            if datediff.days> days_threshold:
                cursor=conn.cursor()
                cursor.execute("delete from hist_price where symbol=%s and exchange=%s",
                          (contract.symbol,contract.exchange,))
                cursor.close()


        
        if max_trade_date is None or datediff.days > days_threshold:
            #"""if no kBar in DB , or KBar in DB is too old, we are going to download most recent 1 year data """
            duration='250 D'
        else:
            duration=str(datediff.days+1)+' D'
            print(str(datediff.days+1)+' D')
            
            
        if  max_trade_date is None or (datediff is not None and datediff.days>0):
            bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting='1 Day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1)
            df = util.df(bars)
#             print("haha:"+contract.symbol+" "+contract.exchange +" "+str(len(df)))
#             print(df)
            saveKBarToDB(contract,df,conn)

    conn.commit()        
    cursor.close()

def saveKBarToDB(contract, df,conn):
    if  df is None:
        return
    cursor=conn.cursor()
    for x in range(0 , len(df)):
        cursor.execute(insertKBarSQL,(contract.symbol,contract.exchange,
              '1 day',df['date'][x],float(df['open'][x]),float(df['high'][x]),float(df['low'][x]),
                float(df['close'][x]),int(df['volume'][x]),))
    conn.commit()
    cursor.close()
            
def filterStock(conn):
    cursor = conn.cursor()
    cursor.execute("select * from stock s where exists " +
                   "(select 1 from hist_price hp where s.symbol=hp.symbol and s.exchange=hp.exchange "+
                   "and hp.barsize='1 day' and hp.trade_date> DATE_ADD(CURRENT_TIMESTAMP(),interval -7 day)) " +
                   " and((s.exchange='SMART' and s.mktcap>2000) or (s.exchange!='SMART' and s.mktcap>2000))")
    list = constructContract(cursor)
    print("开始打印上升趋势股票")
    uptrendList=[]
    for contract in list:
        kbarList=getKBarFromDB(contract,conn)
        ma13=MA(kbarList,13)
        ma34=MA(kbarList,34)
        ma50=MA(kbarList,50)
#         print(contract.symbol)
        if ma13[-1]>ma34[-1] and ma34[-1]>ma50[-1] and kbarList[-1]['close']<1.10*ma13[-1]:
            info=getStockInfoDict(contract,conn,kbarList,ma13,ma34,ma50)
            uptrendList.append(info)
    printUptrend(uptrendList)
    tobreakList=[]
    print("开始打印即将突破的股票")
    for contract in list:
        kbarList=getKBarFromDB(contract,conn)
        ma13=MA(kbarList,13)
        ma34=MA(kbarList,34)
        ma50=MA(kbarList,50)
#         print(contract.symbol)
        kbar_180D=kbarList[-100::]
        min_price=1000000.0
        min_day=0
        max_price=0.0
        max_day=0
        idx=0
        for kbar in kbar_180D:
            if (min_price>kbar['close']):
                min_day=idx
                min_price=min(min_price,kbar['close'])
            if (max_price<kbar['close']):
                max_day=idx
                max_price=max(max_price,kbar['close'])
            idx+=1
        if kbar_180D[-1]['close']/max_price >0.4 and kbar_180D[-1]['close']/max_price<0.7 :
            info=getChaoDieStockInfoDict(contract,conn,kbar_180D,min_day,max_day)
            tobreakList.append(info)
    printTobreak(tobreakList)
    
def getSymbolURL(symbol,exchange):
    if exchange=='SMART':
        return "https://finance.yahoo.com/quote/"+symbol
    elif exchange=='SEHK':
        while len(symbol)<5:
            symbol="0"+symbol
        return "https://xueqiu.com/S/"+symbol
    elif exchange=='SEHKSZSE':
        return "https://xueqiu.com/S/SZ"+symbol
    elif exchange=='SEHKNTL':
        return "https://xueqiu.com/S/SH"+symbol
    elif exchange=='CHINEXT':
        return "https://xueqiu.com/S/SZ"+symbol
        
            
def printUptrend(uptrendList):
  with open("c:\\dev\\uptrend.html","w",encoding="utf-8") as writer: 
    writer.write('<html><meta charset="UTF-8">')
    writer.write("==================================================<br>\n")
    writer.write("上升趋势股票<br>\n")
    writer.write("==================================================<br>\n")
    writer.write("<table border=\"1\">\n"
    "<tr>\r\n"
    "    <th>Symbol</th>\r\n" 
    " <th>名字</th>\r\n" 
    "    <th>交易所</th>\r\n" 
    "    <th>市值(百万)</th>\r\n" 
    " <th>股价位于13日线上方百分比</th>\r\n" 
    "<th>股价位于34日线上方百分比</th>\r\n" 
    "<th>股价位于50日线上方百分比</th>\r\n" 
    "  </tr>");
    
    for info in uptrendList:
        writer.write("<tr>\n<td><a href=\""+getSymbolURL(info['symbol'],info['exchange'])+"\">"+info['symbol']+"</a><br></td>\n")
        if info['name'] is not None:
            writer.write("<td>"+info['name']+"<br></td>\n")
        else:
            writer.write("<td>"+" "+"<br></td>\n")
        writer.write("<td>"+info['exchange']+"<br></td>\n")
        writer.write("<td>"+str(info['marketcap'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价位于13日线上方百分比'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价位于34日线上方百分比'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价位于50日线上方百分比'])+"<br></td>\n")
        writer.write("</tr>\n")
    
    writer.write("</table>\n");
    writer.write("</html>\n");
    
            
def printTobreak(tobreakList):
  with open("c:\\dev\\tobreak.html","w",encoding="utf-8") as writer: 
    writer.write('<html><meta charset="UTF-8">')
    writer.write("==================================================<br>\n")
    writer.write("有突破潜力的股票<br>\n")
    writer.write("==================================================<br>\n")
    writer.write("<table border=\"1\">\n"
    "<tr>\r\n"
    "    <th>Symbol</th>\r\n" 
    " <th>名字</th>\r\n" 
    "    <th>交易所</th>\r\n" 
    "    <th>市值(百万)</th>\r\n" 
    " <th>股价自180天高点下跌百分比</th>\r\n" 
    "<th>股价自180天最低点上升百分比</th>\r\n" 
    "<th>股价自180天最低点盘整天数</th>\r\n" 
    "  </tr>");
    
    for info in tobreakList:
        writer.write("<tr>\n<td><a href=\""+getSymbolURL(info['symbol'],info['exchange'])+"\">"+info['symbol']+"</a><br></td>\n")
        if info['name'] is not None:
            writer.write("<td>"+info['name']+"<br></td>\n")
        else:
            writer.write("<td>"+" "+"<br></td>\n")
        writer.write("<td>"+info['exchange']+"<br></td>\n")
        writer.write("<td>"+str(info['marketcap'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价自180天高点下跌百分比'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价自180天最低点上升百分比'])+"<br></td>\n")
        writer.write("<td>"+str(info['股价自180天最低点盘整天数'])+"<br></td>\n")
        writer.write("</tr>\n")
    
    writer.write("</table>\n");
    writer.write("</html>\n");
    
def getChaoDieStockInfoDict(contract,conn,kbar_180D,min_day,max_day):
    cursor = conn.cursor()
    info={}
    cursor.execute("select * from stock where symbol=%s and exchange=%s",(contract.symbol,contract.exchange,))
    for (symbol,exchange,category,marketcap,active,last_modified,name) in cursor:
        if (exchange == 'SMART'):
            currency = 'USD'
        elif(exchange == 'SEHK'):
            currency = 'HKD'
        elif exchange in ['SEHKNTL','SEHKSZSE','CHINEXT']:
            currency = 'CNH'
        info['symbol']=symbol
        info['exchange']=exchange
        info['marketcap']=str(marketcap/100)+"亿"
        info['name']=name
        info['股价自180天高点下跌百分比']=round((kbar_180D[-1]['close']/kbar_180D[max_day]['close'])*100-100,1)
        info['股价自180天最低点上升百分比']=round((kbar_180D[-1]['close']/kbar_180D[min_day]['close'])*100-100,1)
        info['股价自180天最低点盘整天数']=len(kbar_180D)-min_day
        stocklist.append(contract)    
        cursor.close()
    return info

def getStockInfoDict(contract,conn,kbarList,ma13,ma34,ma50):
    cursor = conn.cursor()
    info={}
    cursor.execute("select * from stock where symbol=%s and exchange=%s",(contract.symbol,contract.exchange,))
    for (symbol,exchange,category,marketcap,active,last_modified,name) in cursor:
        if (exchange == 'SMART'):
            currency = 'USD'
        elif(exchange == 'SEHK'):
            currency = 'HKD'
        elif exchange in ['SEHKNTL','SEHKSZSE','CHINEXT']:
            currency = 'CNH'
        info['symbol']=symbol
        info['exchange']=exchange
        info['marketcap']=str(marketcap/100)+"亿"
        info['name']=name
        info['股价位于13日线上方百分比']=round((kbarList[-1]['close']/ma13[-1])*100-100,1)
        info['股价位于34日线上方百分比']=round((kbarList[-1]['close']/ma34[-1])*100-100,1)
        info['股价位于50日线上方百分比']=round((kbarList[-1]['close']/ma50[-1])*100-100,1)
        stocklist.append(contract)    
        cursor.close()
    return info
    
        
def getKBarFromDB(contract,conn):
    cursor = conn.cursor()
    cursor.execute("select hp.symbol,hp.exchange,hp.barsize,hp.trade_date, hp.open*ifnull(fq.adj_factor, 1), " \
                   " hp.high*ifnull(fq.adj_factor, 1), hp.low*ifnull(fq.adj_factor, 1),"\
                   " hp.close*ifnull(fq.adj_factor, 1),hp.volumn,fq.adj_factor " \
                   " from hist_price hp left outer join fuquan fq "\
                   " on hp.symbol=fq.symbol and hp.exchange=fq.exchange and hp.trade_date=fq.trade_date"\
                   " where hp.symbol=%s and hp.exchange=%s  and barsize='1 day' order by hp.trade_date",
                  (contract.symbol,contract.exchange,))
    kbarList = []
    idx = 0
    for (symbol,exchange,barsize,trade_date,open,high,low,close,volumn,adj) in cursor:
        if contract.exchange!='SMART' and adj is None and len(kbarList)>0 and kbarList[-1]['adj'] is not None :
            kbarList.append({'symbol':symbol,'exchange':exchange,'barsize':barsize,'trade_date':trade_date,
                     'open':open*kbarList[-1]['adj'],'high':high*kbarList[-1]['adj'],
                             'low':low*kbarList[-1]['adj'],'close':close*kbarList[-1]['adj'],'volumn':volumn,
                             "adj":kbarList[-1]['adj']})
        else:
            kbarList.append({'symbol':symbol,'exchange':exchange,'barsize':barsize,'trade_date':trade_date,
                     'open':open,'high':high,'low':low,'close':close,'volumn':volumn,"adj":adj})
    cursor.close()
    return kbarList

def MA(kbarList,days):
    ma = [0.0 for _ in range(len(kbarList))]
    sum =0.0
    for i in range(0 , len(kbarList)):
        if (i< days):
            sum+=kbarList[i]['close']
            ma[i]= sum/(i+1)
        else:
            sum+= kbarList[i]['close']-kbarList[i-days]['close']
            ma[i]= sum/days
    return ma

    
def constructContract(cursor):
    stocklist=[]
    for (symbol,exchange,category,marketcap,active,last_modified,name) in cursor:
        if (exchange == 'SMART'):
            currency = 'USD'
        elif(exchange == 'SEHK'):
            currency = 'HKD'
        elif exchange in ['SEHKNTL','SEHKSZSE','CHINEXT']:
            currency = 'CNH'
        contract = Stock(symbol, exchange, currency)
        stocklist.append(contract)    
    cursor.close()
    return stocklist
            
def trimSymbol(symbol):
    if symbol is None: return symbol
    list=symbol.split(".")
    if len(list)==1:
        return list
    else:
        short_symbol=list[0]
        market=list[1]
        exchange=""
        if market=='HK':
            while (short_symbol.startswith("0")):
                short_symbol=short_symbol[1::]
            exchange="SEHK"
        elif market=='SZ':
            if short_symbol.startswith("3"):
                exchange="CHINEXT"
            elif short_symbol.startswith("0"):
                exchange="SEHKSZSE"
        elif market=='SH':
            exchange="SEHKNTL"
        else:
            print("no such market:"+market)
        
        res= (short_symbol,exchange)
        return res
    
def saveStockListToDB(symbol,name, exchange,currency,conn):
    cursor = conn.cursor()
    cursor.execute("select count(*) from stock where symbol=%s and exchange=%s",(symbol,exchange,))

    exist = 0
    for (cnt,) in cursor:
        exist = cnt
    cursor.close()
    if  exist == 0:
        cursor = conn.cursor()
#         print("haha",symbol,name, exchange,currency)
        cursor.execute("insert into stock (symbol,exchange,category,mktcap,active,last_modified,name) "+
                       "values (%s,%s,%s,%s,%s,%s,%s) ", (symbol,exchange,exchange,None,1,None,name,))
        conn.commit()
        cursor.close()
        
def downloadSymbolList(conn):
  df = pro.hk_basic()
# print(df)

  for x in range(0 , len(df)):
#     print(df['ts_code'][x]+" "+trimSymbol(df['ts_code'][x])+" "+df['name'][x]+" "+df['market'][x])
    symbol,ex = trimSymbol(df['ts_code'][x])
#     symbol = df['ts_code'][x]
    name = df['name'][x]
    market = df['market'][x]
    currency='HKD'
    exchange='SEHK'
#     print(symbol,name, exchange,currency)
    saveStockListToDB(symbol,name, exchange,currency,conn)
    
  for hs in ['H','S']: #沪深
    print(hs)
    df = pro.stock_basic(exchange='', list_status='L',is_hs=hs, 
                     fields='ts_code,symbol,name,area,industry,list_date,is_hs,exchange,market')
    for x in range(0 , len(df)):
#      print(df['ts_code'][x]+" "+trimSymbol(df['ts_code'][x])+" "+df['name'][x]+" "+df['exchange'][x]+" "+df['market'][x])
      symbol,ex = trimSymbol(df['ts_code'][x])
#       symbol = df['ts_code'][x]
      name = df['name'][x]
      market = df['market'][x]
      df_exchange=df['exchange'][x]
      currency='CNH'
      exchange=None
      if df_exchange=='SSE': #上交所
        exchange='SEHKNTL'
      elif df_exchange=='SZSE':  #深交所
        if market=='主板' or market=='中小板':
          exchange='SEHKSZSE'
        elif market=='创业板':
          exchange='CHINEXT'
      else:
          print("没有这个交易所！！！！！！！！！！！！！！！！！！！！！！！！！:"+df_exchange)
#       print(symbol,name, exchange,currency)
      saveStockListToDB(symbol,name, exchange,currency,conn)
    
#util.startLoop()
ib = IB()
ib.connect('127.0.0.1', 4001, clientId=4)
rand_range=10
try:


  conn = mysql.connector.connect(**db_config)
  cursor = conn.cursor()
    
  pro = ts.pro_api()
  downloadSymbolList(conn)
        

  query = ("SELECT * from stock where exchange in ('SEHK','SEHKNTL','SEHKSZSE','SMART') and symbol!='xx' "+
#   query = ("SELECT * from stock where exchange in ('SEHKNTL') and symbol!='300783' "+ 
         "  and active=True ")
  
  cursor.execute(query)

  start_time=time.time()
  stocklist = constructContract(cursor)
  
#   qualifiedContracts=[]
#   for contract in stocklist:
#     qualifiedContract = ib.qualifyContracts(contract)
#     ib.sleep(1)
#     if qualifiedContract is not None and len(qualifiedContract)==1:
#         qualifiedContracts.append(qualifiedContract)
#     else:
#         markInactive(contract,ib,conn)
        
    
  end_time=time.time()
  print("find qualified contract time cost:"+str(end_time-start_time)+" sec" )
  start_time=time.time()

  for contract in stocklist:
    None
#     downloadFundamentalIfNeeded(contract,ib,conn)
  
  
  end_time=time.time()
  print("download fundamental time cost:"+str(end_time-start_time)+" sec" )
  start_time=time.time()

  for contract in stocklist:
    None
    if contract.exchange=='SMART':
        None
        downloadStockKBar(contract,ib,conn)
  
#   downloadCNHKStockKbar(conn)
  downloadFuquan(conn)
    

    
    
  end_time=time.time()
  print("download KBar time cost:"+str(end_time-start_time)+" sec" )
  start_time=time.time()

  cursor.close()
  filterStock(conn)
  end_time=time.time()
  print("filter stock time cost:"+str(end_time-start_time)+" sec" )
  conn.close()  
except Exception:
  raise Exception("exception happen")
finally:
  ib.disconnect()

