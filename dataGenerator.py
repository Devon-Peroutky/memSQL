#!/usr/bin/python
'''
CREATE TABLE SCANS
(
    SCAN_ID                BIGINT NOT NULL,
    SCAN_HASH              VARCHAR(11) NOT NULL,
    SCAN_TYPE              VARCHAR(3),
    SCAN_COUNT             INT,
    MACHINE_TYPE           VARCHAR(10),
    SEQUENCE_CODE          VARCHAR(5),
    LOAD_DATE              TIMESTAMP,
    PRIMARY KEY (SCAN_ID)
)
'''
from collections import defaultdict
import hashlib
import random
from dateutil import rrule
from datetime import datetime, timedelta
import sys
from dbConnector import dbConnector

class DataGenerator:
	cursor = None
	connection = None
	table = "SCANS"

	# Constructor
	def __init__(self) :
		self.connection, self.cursor = dbConnector().getConnection()

	def getCursor(self):
		if(self.connection == None) :
			print("Connection is None")
			return
		self.cursor = self.connection.cursor()

	# BEGIN execute
	def select(self, *arg):
		columns = "*" if len(arg) is 0 else ','.join(str(i) for i in arg)
		query = "SELECT " + columns + " FROM " + self.table
		self.getCursor()
		if(self.cursor!=None) :
			self.cursor.execute(query)
			for row in self.cursor.fetchall() :
				print [str(entry) for entry in row]
		else:
			print("Cursor is 'None'")

	def insert(self, query):
		self.getCursor()
		if(self.cursor==None):
			print "Cursor is None"
			return
		else:
			try:		
				self.cursor.execute(query)
				self.connection.commit()
			except:
				print "Failed to execute: \'" + str(query) + "\';"

	def buildInsert(self, values):
		queries = []
		query = "INSERT INTO " + self.table + " (SCAN_ID, SCAN_HASH, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_CODE, LOAD_DATE) VALUES ("+values[0]+", \'" + values[1]+"\', \'" + values[2] +"\', " + values[3] +", \'" + values[4] +"\', \'" + values[5] +"\', \'" + values[6] + "\')"
		print query
		return query

	def executeInserts(self, queries):
		for query in queries:
			self.insert(query)

	def loadQueries(self, numDays):
		# Declarations
		scanNum=1
		rollover=[]
		scanEvent=0
		i=0
		queries=[]

		# Intitalize the LoadDates
		days = getloadTimes(numDays)

		# Initialize parcels
		for day in days:
			parcels, rollover = getParcels(i, rollover)
			scans = days[day]
			scanNum=1
			for scan in scans:
				scanTime = scan
				for parcelID in parcels:		
					if scanNum>=parcels[parcelID][0] and scanNum<=parcels[parcelID][1]:
						scanID = scanEvent
						scanHash = parcelID
						scanType = getScanType()
						scanCount = scanNum
						machineType = getMachineType()
						sequenceCode = getSequenceCode()
						loadDate = scanTime

						# Update
						scanTime = scanTime + timedelta(seconds=3) 
						scanEvent+=1

						# Insert into Database
						query = self.buildInsert([str(scanID), str(scanHash), scanType, str(scanCount), machineType, sequenceCode, str(loadDate)])
						queries.append(query)
				scanNum+=1
			i+=1

		return queries


def getloadTimes(numDays):
	loadTimes = defaultdict(list)
	now = datetime.now()
	then = now + timedelta(days=numDays)

	for dt in rrule.rrule(rrule.DAILY, dtstart=now, until=then):
		day = dt.day
		hours = sorted([random.randint(1,12) for r in xrange(7)])
		minutes = sorted([random.randint(0,59) for r in xrange(7)])
		seconds = sorted([random.randint(0,59) for r in xrange(7)])

		for i in xrange(7):
			loadTimes[day].append(datetime(dt.year, dt.month, dt.day, hours[i], minutes[i], seconds[i]))
	return loadTimes

def getMachineType():
	mtype = random.randint(1,5)

	if mtype==1:
		return "ForkLift"
	elif mtype==2:
		return "Crane"
	elif mtype==3:
		return "BullDozer"
	elif mtype==4:
		return "Tractor"
	else:
		return "Zamboni"

def getScanType():
	stype = random.randint(1,3)
	if stype==1:
		return "abc"
	elif stype==2:
		return "xyz"
	else:
		return "nop"

def getSequenceCode():
	sectype = random.randint(1,4)
	if sectype==1:
		return "411"
	elif sectype==2:
		return "911"
	elif sectype==3:
		return "123"
	else:
		return "000"

def getParcels(i, previous=[]):
	low = (100*i)+len(previous)
	high = 100*(i+1)
	keep=[]
	parcels={}

	for i in previous:
		start = 1
		end = random.randint(start, 7)
		parcels[(i)]=(start,end)
		if end==7:
			keep.append(i)

	for i in xrange(low,high):
		start = random.randint(1,7)
		end = random.randint(start, 7)
		parcels[(i)]=(start,end)
		if end==7:
			keep.append(i)
	return parcels, keep

def writeQueriesToFile(queries):
	file = open('sql-benchmark/java/queries.txt', 'w+')
	for query in queries:
		file.write(query)
		file.write("\n")

def main(numDays):
	# Initialize Database Controller
	print "Logging in..."
	sampleGenerator = DataGenerator()

	print "Logged in correctly?"

	# Load list of all the Inserts
	queries = sampleGenerator.loadQueries(numDays)	

	# Execute INSERT commands
	sampleGenerator.executeInserts(queries)

	# Write Queries to File
	#writeQueriesToFile(queries)

if __name__ == '__main__':
	if len(sys.argv) is 1:
		print "How many days should we simulate?"
	else:
		main(int(sys.argv[1]))
