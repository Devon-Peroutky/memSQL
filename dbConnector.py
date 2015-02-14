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
		if(any(configs)!=False) :
			self.configs = configs
		try:
			self.connection = MySQLdb.Connect(**self.configs)
			self.cursor = self.connection.cursor()
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exists")
			else:
				print(err)

	def getConnection(self):
		return self.connection, self.cursor
