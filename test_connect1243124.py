import pyodbc
import sys
import py4j
import pandas as pd

from datetime import datetime

from pandas import DataFrame

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import lit



day = datetime.now().day

month = datetime.now().month

year = datetime.now().year

tables = "SalesLT.Product"



spark = SparkSession.builder.appName(name="test").getOrCreate()



conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'

'Server=tcp:ms-sql-srv01.demo.b-one.team;'

'Port=1433;'

'Database=AdventureWorksLT2019;'

'UID=sa;'

'PWD=sql4bone!;')

cursor = conn.cursor()



query = ("SELECT * FROM {tab};".format(tab = tables))

df = pd.read_sql_query(query, conn)

df.to_csv (r'C:\\test\SalesLT.Product\\2022\\10\\27\\test.csv', index = None, header=True) 


df2 =  pd.read_csv('C:\\test\SalesLT.Product\\2022\\10\\27\\test.csv', header='infer')

df2.to_parquet('C:\\test\SalesLT.Product\\2022\\10\\27\\test.parquet', engine='pyarrow')

df3 = pd.read_parquet('C:\\test\SalesLT.Product\\2022\\10\\27\\test.parquet')

print(df3)

#df = cursor.execute('SELECT * FROM dbo.manager;').load()

#df = spark.read.option("header","true").option("inferSchema", "true").jdbc(url=cursor, table=tables, properties=query)

#jdbcDF = spark.read.format("jdbc") \

#    .option("url", f"tcp:ms-sql-srv01.demo.b-one.team") \

#    .option("dbtable", tables) \

#    .option("user", "sa") \

#    .option("password", "sql4bone"!) \

#    .option("driver", "{ODBC Driver 17 for SQL Server}") \

#    .load()

#df = spark.read.format("jdbc").option("url", "jdbc:sqlserver://ms-sql-srv01.demo.b-one.team;").option("query", query).load()

#sc = SparkContext()
#spark = SparkSession(sc)
#df = spark.read \
 #    .format('jdbc') \
  #   .option('url', 'jdbc:postgressql://ms-sql-srv01.demo.b-one.team:1433') \
   #  .option('user', 'sa') \
   #  .option('password', 'sql4bone!') \
   #  .option('dbtable', '(SELECT * FROM SalesLT.Product;)') \
   #  .load()


#print(type(df2))
#df.write.parquet("D:/AW2019LT/{tab}/{y}/{m}/{d}"

    #.format(tab = table).format(y = year).format(m = month).format(d = day))
    
    
