import MySQLdb
import mysql.connector
from mysql.connector import errorcode

class dbConnector:
	configs = {
		"user":"root",
		"host":"127.0.0.1",
		"db":"MemEx",
	}
	connection = None
	cursor = None
	table = "SCANS"

	# Constructor
	def __init__(self, configs = {}) :
		# Load Configurations
		if(any(configs)!=False) :
			self.configs = configs

		# Initialize Connection
		try:
			self.connection = MySQLdb.Connect(**self.configs)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exists")
			else:
				print(err)

		# Instantiate the Cursor
		try:
			self.cursor = self.connection.cursor()
		except err:
			print err

		# Set autocommit to be True
		self.connection.autocommit(True) 

	def getConnection(self):
		return self.connection, self.cursor
