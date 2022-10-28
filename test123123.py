#import pyodbc
import pyspark
#import pandas as pd

from datetime import datetime

#from pandas import DataFrame

from pyspark.sql import SparkSession

from pyspark.sql.functions import lit



day = datetime.now().day

month = datetime.now().month

year = datetime.now().year

tables = "SalesLT.Product"



spark = SparkSession.builder.appName(name="test").getOrCreate()



#conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
#'Server=tcp:ms-sql-srv01.demo.b-one.team;'
#'Port=1433;'
#'Database=AdventureWorksLT2019;'
#'UID=sa;'
#'PWD=sql4bone!;')

#cursor = conn.cursor()



query = ("SELECT * FROM SalesLT.Product{tab};".format(tab = tables))



#df = spark.read.option("header","true").option("inferSchema", "true").jdbc(url=cursor, table=tables, properties=query)

jdbcDF = spark.read.format("jdbc") \
.option("url", f"tcp:ms-sql-srv01.demo.b-one.team") \
.option("dbtable", tables) \
.option("user", "sa") \
.option("password", "sql4bone!") \
.load()

#df = spark.read.format("odbc").option("url", cursor).option("query", query).load()






#df.write.parquet("D:/AW2019LT/{tab}/{y}/{m}/{d}"

    #.format(tab = table).format(y = year).format(m = month).format(d = day))
    
    
#jdbcDF = spark.read \
#    .format("jdbc") \
#    .option("url", "ms-sql-srv01.demo.b-one.team") \
#    .option("dbtable", "SalesLT.Product") \
#    .option("user", "sa") \
 #   .option("password", "sql4bone!") \
 #   .load()
    
    
print(type(jdbcDF))


from py4jdbc import connect

conn = connect("jdbc:postgresql://localhost/postgres, user="cow", password="moo")
cur = conn.cursor()
cur.execute("select 1 as cow;")