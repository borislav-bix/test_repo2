#from py4jdbc import connect

#conn = connect("jdbc:ms-sql-srv01.demo.b-one.team, user=sa, password=sql4bone!")
#cur = conn.cursor()
#cur.execute("select 1 as cow;")



import pymssql  
    
conn = pymssql.connect(server='ms-sql-srv01.demo.b-one.team', user='sa', password='sql4bone!', database='AdventureWorksLT2019')

cursor = conn.cursor()  

cursor.execute('SELECT * FROM SalesLT.Product;')  


row = cursor.fetchone()  
while row:  
        print(str(row[0]) + " " + str(row[1]) + " " + str(row[2]))     
        row = cursor.fetchone()  
        
        
        