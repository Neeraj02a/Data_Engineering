import pymysql
import pandas as pd

connection = pymysql.connect(user='root', passwd='Welcome@123', host='localhost')
cursor = connection.cursor()
query = ("show databases")
cursor.execute(query)
for i in cursor:
	print(i)

connection.close()

#=============================================================================================================================================================================
print("#============================================================ show all the table in the DB ===========================================================================")

import pymysql
import pandas as pd

try:
    myconn = pymysql.connect(user='root',password='Welcome@123',host='localhost', database='neeraj_db')
    cursor = myconn.cursor()
    query = "show tables;"
    result_dataFrame = pd.read_sql(query, myconn)
    print(result_dataFrame)
    myconn.close() #close the connection
except Exception as e:


    myconn.close()
    print(str(e))