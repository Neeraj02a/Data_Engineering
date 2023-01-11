import pymysql
import pandas as pd

connection = pymysql.connect(user='root', passwd='Welcome@123', host='localhost')
cursor = connection.cursor()
query = ("show databases")
cursor.execute(query)
for i in cursor:
	print(i)

connection.close()
