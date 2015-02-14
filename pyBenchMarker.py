from __future__ import print_function
import time
from dbConnector import dbConnector

def benchMarker():
	connection = None
	cursor = None

	def __init__(self):
		self.connection = dbConnector().getConnection()

	def getCursor(self):
		if(self.connection == None) :
			print("Connection is None")
			return
		self.cursor = self.connection.cursor()

	def benchMarkInserts():
		print "Benchmark purely inserts"

	def benchMarkSelects():
		print "Benchmark purely selects"

	def benchMarkMixed():
		print "Benchmark mixed queries"

def main():
	app = benchMarker()


if __name__ == '__main__':
	main()