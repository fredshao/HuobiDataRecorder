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
        self.__DoWork()
        
    def __ConnectDB(self):
        dbName = self.symbol + ".db"
        if os.path.exists(dbName) is False:
            print("Create Database file:",self.symbol)
            self.conn = sqlite3.connect(dbName)
            self.cursor = self.conn.cursor()
            self.cursor.execute('''
            CREATE TABLE depthData(
            bids DOUBLE NOT NULL,
            asks DOUBLE NOT NULL,
            created_at datetime);
            ''')
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(dbName)
            self.cursor = self.conn.cursor()
            
    def __GetShanghaiTime(self):
        tz = pytz.timezone('Asia/Shanghai')
        ts = int(time.time())
        dt = datetime.fromtimestamp(ts,tz)
        return datetime(dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second,dt.microsecond)
            
    
    def __DoWork(self):
        print("Start Record:",self.coinName,'-',self.symbol)
        t = threading.Thread(target=self.__GetTradeDataThread, name="GetTradeData_" + self.symbol)
        t.setDaemon(True)
        t.start()
    
    def __GetTradeDataThread(self):
        self.__ConnectDB()
        while(True):
            try:
                depthData = HuobiServices.get_depth(self.symbol,'step0')
                bidsPrice = -2.0
                asksPrice = -2.0
                if(len(depthData['tick']['bids']) > 0 and len(depthData['tick']['bids'][0]) > 0):
                    bidsPrice = depthData['tick']['bids'][0][0]
            
                if(len(depthData['tick']['asks']) > 0 and len(depthData['tick']['asks'][0]) > 0):
                    asksPrice = depthData['tick']['asks'][0][0]
                
                self.__SaveData(bidsPrice, asksPrice)
                
                bidsPrice = Decimal(bidsPrice).quantize(Decimal('0.00000000'))
                asksPrice = Decimal(asksPrice).quantize(Decimal('0.00000000'))
                
                print(self.coinName,'-',self.symbol,bidsPrice,asksPrice)
            except:
                self.__SaveData(-1.0, -1.0)
                print("------------------------------------------Get Price Faild:",self.coinName,self.symbol)
            time.sleep(0.5)
            
    def __SaveData(self,bidsPrice, asksPrice):
        self.cursor.execute("insert into depthData (bids,asks,created_at) values(?,?,?)" , (bidsPrice,asksPrice,self.__GetShanghaiTime()))
        self.conn.commit()
    


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
            coinInfoDict[coinName] = coinInfo
            
def StartRecordData():
    for coin,coinInfo in coinInfoDict.items():
        global recorderDict
        if recorderDict.__contains__(coinInfo.symbol) is False:
            recorder = CoinRecorder(coinInfo.coinName,coinInfo.symbol)
            recorderDict[coinInfo.symbol] = recorder
            

while(True):
    print("Refresh Recorder")
    InitCoinInfo()
    StartRecordData()
    time.sleep(10)


    
