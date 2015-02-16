#!/usr/bin/python
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
	query = ""
	queryList = []
	insertHeader = "INSERT INTO " + table + " (SCAN_ID, SCAN_HASH, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_CODE, LOAD_DATE) VALUES "

	# Constructor
	def __init__(self) :
		connector = dbConnector()
		self.connection, self.cursor = connector.getConnection()
		self.table = connector.table

	# BEGIN execute
	def select(self, *arg):
		columns = "*" if len(arg) is 0 else ','.join(str(i) for i in arg)
		query = "SELECT " + columns + " FROM " + self.table
		if(self.cursor!=None) :
			self.cursor.execute(query)
			for row in self.cursor.fetchall() :
				print [str(entry) for entry in row]
		else:
			print("Cursor is 'None'")

	def insert(self, query):
		print "INSERTING.................................."
		if(self.cursor==None):
			print "Cursor is None"
		else:
			try:		
				self.cursor.execute(query)
			except:
				print "Failed to execute insert: "#\'" + str(query) + "\';"

	def buildGroupInsert(self, values):	
		# Ugly, but I don't see how to use a list comprehension we some columns need to be enclosed by '' and others don't	
		query = values[0]+", \'" + values[1]+"\', \'" + values[2] +"\', " + values[3] +", \'" + values[4] +"\', \'" + values[5] +"\', \'" + values[6] + "\'"
		self.queryList.append("(" + query + ")")

	def finalizeGroupInsert(self):
		query = ','.join(self.queryList)
		return ''.join([self.insertHeader,query])

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
				scanType = getScanType()
				machineType = getMachineType()
				sequenceCode = getSequenceCode()
				scanCount = scanNum

				for parcelID in parcels:		
					if scanNum>=parcels[parcelID][0] and scanNum<=parcels[parcelID][1]:
						# Update
						scanTime = scanTime + timedelta(seconds=3) 
						scanEvent+=1

						# Add to Query List 
						queries.append(",".join([str(scanEvent), str(parcelID), scanType, str(scanCount), machineType, sequenceCode, str(scanTime)]))
						self.buildGroupInsert([str(scanEvent), str(parcelID), scanType, str(scanCount), machineType, sequenceCode, str(scanTime)])
				scanNum+=1
			i+=1
		print scanEvent
		return self.finalizeGroupInsert(), queries


def getloadTimes(numDays):
	loadTimes = defaultdict(list)
	now = datetime.now()
	then = now + timedelta(days=numDays-1)

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

def getParcels(i, previous=[], number=50000000):
	low = (number*i)+len(previous)
	high = number*(i+1)
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

def writeINFILE(queries):
	file = open('sql-benchmark/values.txt', 'w+')
	file.write("SCAN_ID, SCAN_HASH, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_CODE, LOAD_DATE\n")
	for query in queries:
		file.write(query)
		file.write("\n")

# DEPRECIATED
def writeQueryToFile(query):
	try:
		file = open('sql-benchmark/queries.txt', 'w+')
		file.write(query)
		file.write("\n")
		file.close()
	except:
		print "Error Opening/Writing to the sql-benchmark/queries.txt"

def main(numDays):
	# Initialize Database Controller
	sampleGenerator = DataGenerator()

	# Load list of all the Inserts
	groupInsert, queries = sampleGenerator.loadQueries(numDays)	

	# Insert the groupInsert into the Database
	sampleGenerator.insert(groupInsert)

	# Write Values to File that can be used for LOAD DATA INFILE
	writeINFILE(queries)

if __name__ == '__main__':
	if len(sys.argv) is 1:
		print "How many days should we simulate?"
	else:
		main(int(sys.argv[1]))
