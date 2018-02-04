from utils.dbutil import DBModel
from utils import ioutil
import time
import pytz
from datetime import date, datetime
import threading
import sys
import json
from API.Huobi import HuobiServices

class Recorder(DBModel):
	def __init__(self,symbol):
		super(Recorder,self).__init__(symbol)
		self.symbol = symbol
		self.tableName = 'depthData'
		self.dbName = None

	def InitDB(self):
		sql = '''
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
			  '''
		self.DBInit(self.dbName,sql)
		
	def DoWork(self):
		self.working = True
		t = threading.Thread(target=self.__WorkingThread)
		t.setDaemon(True)
		t.start()
		
	def StopWork(self):
		self.working = False
		
	def __RefreshDbConnect(self):
		now = self.GetShanghaiTime()
		dbName = "{}_{}_{}_{}.db".format(self.symbol,now.year,now.month,now.day)
		if self.dbName is not None:
			if self.dbName != dbName:
				self.DBClose()
				print("Close Previous DB:",self.dbName)
		
		if self.dbName is None or self.dbName != dbName:
			self.dbName = dbName
			self.InitDB()
			self.DBConnect(self.dbName)
			print("Connect DB:",self.dbName)
		
	def __WorkingThread(self):
		print("WorkStarted:",self.symbol)
		while self.working:
			self.__RefreshDbConnect()
			try:
				depthData = HuobiServices.get_depth(self.symbol,'step0')
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
				now = self.GetShanghaiTime()
				self.DBSaveData(self.tableName,'(bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7,created_at)',(bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7,now))
				time.sleep(0.5)
			except:
				now = self.GetShanghaiTime()
				self.DBSaveData(self.tableName,'(bid1,bid2,bid3,bid4,bid5,bid6,bid7,ask1,ask2,ask3,ask4,ask5,ask6,ask7,created_at)',(-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,now))
				time.sleep(0.5)
		self.DBClose()
		print("Work terminated:",self.symbol)


coinInfoDict = {}
recorderDict = {}

def InitCoinInfo():
	global coinInfoDict
	coinInfoDict = {}
	coinJson = ioutil.ReadTextFromFile('coins.json')
	if coinJson is None:
		for symbol in recorderDict:
			recorderDict[symbol].StopWork()
		print("Terminate all work")
		time.sleep(5)
		print("Exit system")
		sys.exit()
		return
	
	coinData = json.loads(coinJson)
	for item in coinData:
		coinName = item['currency']
		symbol = item['symbol']
		if coinInfoDict.__contains__(symbol) is False:
			coinInfoDict[symbol] = symbol
			
def RefreshWork():
	# stop some work
	stopedWorkList = []
	for symbol in recorderDict:
		if coinInfoDict.__contains__(symbol) is False:
			recorderDict[symbol].StopWork()
			stopedWorkList.append(symbol)
			
	for symbol in stopedWorkList:
		del recorderDict[symbol]
			
	for symbol in coinInfoDict:
		if recorderDict.__contains__(symbol) is False:
			recorder = Recorder(symbol)
			recorderDict[symbol] = recorder
			recorder.DoWork()
		

#recorder = Recorder('usdtbtc')
#recorder.InitDB()
#recorder.SaveData()
#recorder.DoWork()

index = 0
while True:
	if index % 10 == 0:
		InitCoinInfo()
		RefreshWork()
	index += 1
	time.sleep(1)
