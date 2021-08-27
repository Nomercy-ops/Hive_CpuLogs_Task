"""

@Author: Rikesh Chhetri

@Date: 2021-08-26

@Last Modified by: Rikesh Chhetri

@Last Modified time: 2021-08-26 07:03:30

@Title : Program to insert a cpu log data.csv file from hdfs into hive database using pyhive library,and perform different query and also do visualization of the result.

"""
from loghandler import logger
from pyhive import hive
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(".env")


host_name = os.getenv("HOST_NAME")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")


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
        It takes database name as a parameter for creating database.

    """
    try:
        connection = hive.Connection(host=host_name, port=port, username=user, password=password,
                                     auth='CUSTOM')

        cur = connection.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS  {}".format(db_name))
        print("Database created successfully")

    except Exception as e:
        logger.error(e)


def create_table_and_load_csv():
    """
Description:
    This method is used to create a table in a hive database.

"""
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("create external table logs(DateTime date,c1 string,c2 string,c3 string,c4 string,c5 string,c6 string,c7 string,c8 string,c9 string,c10 string,c11 string,c12 string,c13 string,c14 string,c15 string,c16 string,c17 string,c18 string,c19 string,c20 string,c21 string,c22 string,c23 string,c24 string,c25 string,c26 string,c27 string,c28 string,c29 string,c30 string,c31 string,c32 string,c33 string,c34 string,c35 string,c36 string,c37 string,c38 string,c39 string,user_name string,keyboard string,mouse string,c44 string,c45 string) row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/csv_data' tblproperties('skip.header.line.count'='1')")
        print("Table has been created successfully")

    except Exception as e:
        logger.error(e)

def load__hive_data_into_panda_df():
    """
Description:
    This method is used to create a panda dataframe by doing query with a hive database.

"""
    try:
        conn = createConnection()
        df = pd.read_sql("select * from log_data",conn)
        df.head(5)
        print(df)

    except Exception as e:
        logger.error(e)

def drop_hive_table(tbl_name):
    """
Description:
    This method is used to delete a hive database table.

"""
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("drop table {}".format(tbl_name))
        print("Table Deleted Successfully")

    except Exception as e:
        logger.error(e)



# create_database("test")
# create_table_and_load_csv()
# load_data_into_panda_df()
# drop_hive_table("logs")

