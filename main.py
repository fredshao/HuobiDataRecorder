from API.Huobi import HuobiServices
import os
import json
import time
import threading
from decimal import *
from enum import Enum
import requests
import sqlite3
import pytz
from datetime import date, datetime
import sys


# ======================== Utils Function ========================
def AppendTextToFile(filePath,text):
    with open(filePath,'a') as f:
        f.write(text)

def WriteTextToFile(filePath, text):
    with open(filePath,'w') as f:
        f.write(text)

def ReadTextFromFile(filePath):
    if not os.path.exists(filePath):
        return ""
    with open(filePath,'r') as f:
        return f.read()

# ======================== Utils Function End ========================





coinInfoDict = {}
recorderDict = {}

class CoinInfo(object):
    def __init__(self,coinName, symbol):
        self.coinName = coinName
        self.symbol = symbol


class CoinRecorder(object):
    def __init__(self,coinName,symbol):
        self.coinName = coinName
        self.symbol = symbol
        #self.__DoWork()
        
    def __ConnectDB(self):
        dbName = self.symbol + ".db"
        if os.path.exists(dbName) is False:
            print("Create Database file:",self.symbol)
            self.conn = sqlite3.connect(dbName)
            self.cursor = self.conn.cursor()
            self.cursor.execute('''
            CREATE TABLE depthData(
            bid1 DOUBLE NOT NULL,
            bid2 DOUBLE NOT NULL,
            bid3 DOUBLE NOT null,
            bid4 double not null,
            bid5 double not null,
            bid6 double not null,
            bid7 double not null,
            ask1 DOUBLE NOT NULL,
            ask2 double not null,
            ask3 double not null,
            ask4 double not null,
            ask5 double not null,
            ask6 double not null,
            ask7 double not null,
            created_at datetime);
            ''')
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(dbName)
            self.cursor = self.conn.cursor()

    def CloseDB(self):
        self.conn.close()
            
    def __GetShanghaiTime(self):
        tz = pytz.timezone('Asia/Shanghai')
        ts = int(time.time())
        dt = datetime.fromtimestamp(ts,tz)
        return datetime(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond)
            
    
    def DoWork(self):
        print("Start Record:",self.coinName,'-',self.symbol)
        #t = threading.Thread(target=self.__GetTradeDataThread, name="GetTradeData_" + self.symbol)
        #t.setDaemon(True)
        #t.start()
        self.__GetTradeDataThread()
    
    def __GetTradeDataThread(self):
        
        while(True):
            try:
                depthData = HuobiServices.get_depth(self.symbol,'step0')
                bidsPrice = -2.0
                asksPrice = -2.0
                
                bid1 = depthData['tick']['bids'][0][0]
                bid2 = depthData['tick']['bids'][1][0]
                bid3 = depthData['tick']['bids'][2][0]
                bid4 = depthData['tick']['bids'][3][0]
                bid5 = depthData['tick']['bids'][4][0]
                bid6 = depthData['tick']['bids'][5][0]
                bid7 = depthData['tick']['bids'][6][0]

                ask1 = depthData['tick']['asks'][0][0]
                ask2 = depthData['tick']['asks'][1][0]
                ask3 = depthData['tick']['asks'][2][0]
                ask4 = depthData['tick']['asks'][3][0]
                ask5 = depthData['tick']['asks'][4][0]
                ask6 = depthData['tick']['asks'][5][0]
                ask7 = depthData['tick']['asks'][6][0]
                
                self.__SaveData(bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7)
                
                #print(self.coinName,'-',self.symbol,bidsPrice,asksPrice)
            except Exception as e:
                print(self.symbol," Exception:", e)
                self.__SaveData(-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1)
                #print("------------------------------------------Get Price Faild:",self.coinName,self.symbol)
            time.sleep(0.5)

            
    def __SaveData(self,bid1,bid2,bid3,bid4,bid5,bid6,bid7, ask1,ask2,ask3,ask4,ask5,ask6,ask7):
        self.__ConnectDB()
        self.cursor.execute("insert into depthData (bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7,created_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" , (bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7,self.__GetShanghaiTime()))
        self.conn.commit()
        self.CloseDB()
    


def InitCoinInfo():
    coinsJson = ReadTextFromFile('coins.json')
    coinData = json.loads(coinsJson)
    global coinInfoDict
    coinInfoDict = {}
    for item in coinData:
        coinName = item['currency']
        symbol = item['symbol']
        if coinInfoDict.__contains__(coinName) is False:
            coinInfo = CoinInfo(coinName, symbol)
            coinInfoDict[symbol] = coinInfo
            
def StartRecordData():
    for coin,coinInfo in coinInfoDict.items():
        global recorderDict
        if recorderDict.__contains__(coinInfo.symbol) is False:
            recorder = CoinRecorder(coinInfo.coinName,coinInfo.symbol)
            recorderDict[coinInfo.symbol] = recorder


coinName = 'btc'
symbol = 'btcusdt'
recorder = CoinRecorder(coinName,symbol)
recorder.DoWork()

#InitCoinInfo()
#StartRecordData()   

#while(True):
#    try:
#        pass
#    except KeyboardInterrupt:
#        print("Should Close database")
#        sys.exit()


    
