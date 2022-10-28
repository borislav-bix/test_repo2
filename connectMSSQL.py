import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
sc = SparkContext('xx')
spark = SparkSession(sc)

#conn = pymssql.connect(server='ms-sql-srv01.demo.b-one.team', user='sa', password='sql4bone!', database='AdventureWorksLT2019')


sc = SparkContext()
spark = SparkSession(sc)
df = spark.read \
     .format('jdbc') \
     .option('url', 'jdbc:sqlserver://ms-sql-srv01.demo.b-one.team:1433') \
     .option('user', 'sa') \
     .option('password', 'sql4bone!') \
     .option('dbtable', '(SELECT * FROM SalesLT.Product)') \
         .load()