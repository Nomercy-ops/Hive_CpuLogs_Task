# Hive_CpuLogs_Task

* hive version 3.1.2
* pyhive version 2.6.1 

# Hive_Cpulog_task

## Program Aim:
Program to insert a cpu log data.csv file from hdfs into hive database using pyhive library,and perform different query and also do visualization of the result.
Columns to consider user_name,DateTime,keyboard,mouse
Display users and their record counts
Finding users with highest number of average hours
Finding users with lowest number of average hours
Finding users with highest numbers of idle hours
 
## Step1:
Open terminal and start hadoop daemons: 
start-all-sh
Copy the csv file into hdfs directory into a folder
Load all csv into hdfs using put command into a folder.
 
 
## Step2:
Start hiveserver2 on a terminal
Import the necessary library and credentials to make a connection.
 
from loghandler import logger
from pyhive import hive
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(".env")
 
insert  csv data into hive databasea  by loading a csv file from hdfs:
 
NOW CREATE A DATABASE AND LOAD THE CSV FILE BY CREATING TABLE:
First create a connection and then create database:
 
def createConnection():   
"""
   Description:
       This method is used to create a connection with hive.
   """
   conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                          database=database,  auth='CUSTOM')
 
   return conn
 
 
def create_database(db_name):
   """
   Description:
       This method is used to create a hive database.
   Parameter:
       It takes a database name as a parameter for creating a database.
 
   """
   try:
       connection = hive.Connection(host=host_name, port=port, username=user, password=password,
                                    auth='CUSTOM')
 
       cur = connection.cursor()
       cur.execute("CREATE DATABASE IF NOT EXISTS  {}".format(db_name))
       print("Database created successfully")
 
   except Exception as e:
       logger.error(e)
 
# creating table and loading csv file from hdfs
def create_table_and_load_csv():
   """
Description:
   This method is used to create a table in a hive database.
 
"""
   try:
       connection = createConnection()
       cur = connection.cursor()
       cur.execute("create table log_data(DateTime date,c1 string,c2 string,c3 string,c4 string,c5 string,c6 string,c7 string,c8 string,c9 string,c10 string,c11 string,c12 string,c13 string,c14 string,c15 string,c16 string,c17 string,c18 string,c19 string,c20 string,c21 string,c22 string,c23 string,c24 string,c25 string,c26 string,c27 string,c28 string,c29 string,c30 string,c31 string,c32 string,c33 string,c34 string,c35 string,c36 string,c37 string,c38 string,c39 string,c40 string,user_name string,keyboard string,mouse string,c44 string,c45 string) row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/csv_data' tblproperties('skip.header.line.count'='1')")
       print("Table has been created successfully")
 
   except Exception as e:
       logger.error(e)
 
 
 
## Step3: Now completing first task:
Create a new ipynb file and create a new connection and import the following.
from pyhive import hive
import seaborn as sns
import pandas as pd
from loghandler import logger
import matplotlib.pyplot as plt
 
try:
   conn = hive.Connection(host='localhost',port=10000,database='default')
except Exception as e:
   logger.error(e)
 
Display users and their record counts:
Here we are creating a panda dataframe by firing a query.
user_count=pd.read_sql("select user_name, count('') as total_count from logs group by user_name",conn)
print(user_count)
 
 
 
user_name  			total_count
0  bhagyashrichalke21@gmail.com          482
1         damodharn21@gmail.com          253
2       deepshukla292@gmail.com          565
3            iamnzm@outlook.com          614
4     markfernandes66@gmail.com          508
5         rahilstar11@gmail.com          551
6      salinabodale73@gmail.com          569
7         sharlawar77@gmail.com          580
 
 
## Step4:
Finding users with highest number of average hours:
First getting count for user
df = pd.read_sql("select user_name ,count('') as total from logs where keyboard !=0.0 or mouse!=0.0 group by user_name",conn)
  print(df)
 
 
user_name  		total
0  bhagyashrichalke21@gmail.com    361
1         damodharn21@gmail.com    191
2       deepshukla292@gmail.com    475
3            iamnzm@outlook.com    459
4     markfernandes66@gmail.com    389
5         rahilstar11@gmail.com    399
6      salinabodale73@gmail.com    440
7         sharlawar77@gmail.com    457
 
Now getting highest working seconds by using a query
 
Now :
We know that the diff of every record is with difference of 5 min we multiply by 5
We need the average time in Hours therefore we multiply by 60
There are 6 days of data available, therefore we divide them by 6
Here is the query for that:
highest_work_sec=pd.read_sql("select user_name ,((((count('')-1)*5)*60)/6) as working_sec from logs where keyboard!=0.0 or mouse!=0.0 group by user_name",conn)
highest_work_sec= highest_work_sec.sort_values(by='working_sec',ascending=False)
print(highest_work_sec)
Here,count-1 is used to skip the first row.
            user_name  		working_sec
2       deepshukla292@gmail.com      23700.0
3            iamnzm@outlook.com      22900.0
7         sharlawar77@gmail.com      22800.0
6      salinabodale73@gmail.com      21950.0
5         rahilstar11@gmail.com      19900.0
4     markfernandes66@gmail.com      19400.0
0  bhagyashrichalke21@gmail.com      18000.0
1         damodharn21@gmail.com       9500.0
Now convert these working_sec column into hh:mm format,
And printing the final result:
highest_average_work_hour = highest_work_sec[["user_name","working_sec"]]
highest_average_work_hour["working_sec"]= pd.to_datetime(highest_average_work_hour['working_sec'] ,unit='s').dt.strftime("%H:%M")
highest_average_work_hour = highest_average_work_hour.sort_values('working_sec',ascending=False)
highest_average_work_hour.rename(columns={'working_sec': 'working_hour'}, inplace=True)
print(highest_average_work_hour)
 
Output:highest_avg_hour.show():
                user_name 		working_hour
2       deepshukla292@gmail.com        06:35
3            iamnzm@outlook.com        06:21
7         sharlawar77@gmail.com        06:20
6      salinabodale73@gmail.com        06:05
5         rahilstar11@gmail.com        05:31
4     markfernandes66@gmail.com        05:23
0  bhagyashrichalke21@gmail.com        05:00
1         damodharn21@gmail.com        02:38
 
## Step5:
Finding users with lowest number of average hours:
For getting the lowest average hour we have to get average secs by firing this query.
lowest_work_sec=pd.read_sql("select user_name ,((((count('')-1)*5)*60)/6) as working_sec from logs where keyboard!=0.0 or mouse!=0.0 group by user_name",conn)
lowest_average_work_hour = lowest_work_sec.sort_values('working_sec')
print(lowest_average_work_hour)
user_name  		working_sec
1         damodharn21@gmail.com       9500.0
0  bhagyashrichalke21@gmail.com      18000.0
4     markfernandes66@gmail.com      19400.0
5         rahilstar11@gmail.com      19900.0
6      salinabodale73@gmail.com      21950.0
7         sharlawar77@gmail.com      22800.0
3            iamnzm@outlook.com      22900.0
2       deepshukla292@gmail.com      23700.0
Now  Converting working seconds to hour:
lowest_average_work_hour = lowest_work_sec[["user_name","working_sec"]]
lowest_average_work_hour["working_sec"]= pd.to_datetime(lowest_average_work_hour['working_sec'] ,unit='s').dt.strftime("%H:%M")
lowest_average_work_hour = lowest_average_work_hour.sort_values('working_sec',ascending=False)
lowest_average_work_hour.rename(columns={'working_sec': 'working_hour'}, inplace=True)
print(lowest_average_work_hour)
 
Output: 
user_name 		 working_hour
1         damodharn21@gmail.com        02:38
0  bhagyashrichalke21@gmail.com        05:00
4     markfernandes66@gmail.com        05:23
5         rahilstar11@gmail.com        05:31
6      salinabodale73@gmail.com        06:05
7         sharlawar77@gmail.com        06:20
3            iamnzm@outlook.com        06:21
2       deepshukla292@gmail.com        06:35
 
## Step6:
Finding users with highest numbers of idle hours:
Now finding the user with the highest number of idle hours we will be creating a panda dataframe from a query.
idle_count = pd.read_sql("select user_name ,count('') as total from logs where keyboard=0.0 and mouse=0.0 group by user_name",conn)
print(idle_count)
                 
 user_name 		total
0  bhagyashrichalke21@gmail.com    121
1         damodharn21@gmail.com     62
2       deepshukla292@gmail.com     90
3            iamnzm@outlook.com    155
4     markfernandes66@gmail.com    119
5         rahilstar11@gmail.com    152
6      salinabodale73@gmail.com    129
7         sharlawar77@gmail.com    123
 
Now :getting the idle time in secs
We know that the diff of every record is with difference of 5 min we multiply by 5
We need the average time in Hours therefore we multiply by 60
There are 6 days of data available, therefore we divide them by 6
Here is the query for that:
highest_idle_time = pd.read_sql("select user_name ,((((count('')-1)*5)*60)/6) as idle_sec from logs where keyboard=0.0 and mouse=0.0 group by user_name",conn)
highest_idle_time=highest_idle_time.sort_values(by='idle_sec',ascending=False)
print(highest_idle_time)
Here,count-1 is used to skip the first row.
 
                  user_name       idle_sec
3            iamnzm@outlook.com    7700.0
5         rahilstar11@gmail.com    7550.0
6      salinabodale73@gmail.com    6400.0
7         sharlawar77@gmail.com    6100.0
0  bhagyashrichalke21@gmail.com    6000.0
4     markfernandes66@gmail.com    5900.0
2       deepshukla292@gmail.com    4450.0
1         damodharn21@gmail.com    3050.0
Now convert these idle_sec into hh:mm format,
And printing the final result:
idle_hour = highest_idle_time[["user_name","idle_sec"]]
idle_hour["idle_sec"]= pd.to_datetime(idle_hour['idle_sec'] ,unit='s').dt.strftime("%H:%M")
idle_hour = idle_hour.sort_values('idle_sec',ascending=False)
idle_hour.rename(columns={'idle_sec': 'idle_hour'}, inplace=True)
print(idle_hour)
 
Now showing the output
Output:
                user_name        idle_hour
3            iamnzm@outlook.com     02:08
5         rahilstar11@gmail.com     02:05
6      salinabodale73@gmail.com     01:46
7         sharlawar77@gmail.com     01:41
0  bhagyashrichalke21@gmail.com     01:40
4     markfernandes66@gmail.com     01:38
2       deepshukla292@gmail.com     01:14
1         damodharn21@gmail.com     00:50

