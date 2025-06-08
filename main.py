import mysql.connector 
import os

#Establish connection to db using username and password provided in the .env file
db = mysql.connector.connect(host="localhost", user=os.getenv("DB_USER"), passwd=os.getenv("DB_PASS"))


