# Hive_with_spark

## firstly setup the spark with hive:
* step1:
* add the following path on the hive-site.xml settings otherwise we will get error:
* <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/home/rikesh/metastore_db;create=true</value>
    <description>
      JDBC connect string for a JDBC metastore.
      To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection URL.
      For example, jdbc:postgresql://myhost/db?ssl=true for postgres database.
    </description>
  </property>
  
 * step 2:
 * go inside the spark directory and go insoide jar folder and delete the old derby driver:
 * search for derby and delete it now after that,
 * copy the derby driver from the hive lib folder and paste it to spark jar folder
 * now we are ready to run our program.
 
#S codes:

## # creating spark sesion with hive support enable
 
* from pyspark.sql import SparkSession
* from loghandler import logger
* try:
*   appName = "PySpark MySQL Example - via mysql.connector"
*     master = "local"
*    spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
* except Exception as e:
 *   logger.error(e)
 
##  creating dataframe of csv file using spark and selecting only 4 columns from csv

 try:
    df = spark.read.csv("hdfs://localhost:9000/csv_data/*.csv",header=True)
    df2 = df.select("user_name","DateTime","keyboard","mouse")
    df2.createOrReplaceTempView('myview')
except Exception as e:
    logger.error(e)
    
# Now Adding these dataframe into hive table using different method

## Method 1

try:
    # Save df to a new table in Hive we just need database table name we can give anything
    df2.write.mode("overwrite").saveAsTable("test.test_data")

except Exception as e:
    logger.error(e)
    
## Method 2

try:
    # here test is a database name and just give anything for table name it will create automatically
    spark.sql("create table test.test2 as select * from myview")

except Exception as e:
    logger.error(e)
    
##  Showing THe Hive Table

try:
    
    dfs = spark.sql("use test")
    dfs1 = spark.sql("show tables")
    dfs1.show()

except Exception as e:
    logger.error(e)
 
   
