import MySQLdb
import mysql.connector
from mysql.connector import errorcode

class dbConnector:
	configs = {
		"user":"root",
		"password":"",
		"host":"127.0.0.1",
		"database":"memSQL",
		"raise_on_warnings": True
	}
	connection = None
	table = "SCANS"

	# Constructor
	def __init__(self, configs = {}) :
		if(any(configs)!=False) :
			self.configs = configs
		try:
			self.connection = mysql.connector.connect(**self.configs)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exists")
			else:
				print(err)

	def getConnection(self):
		return self.connection