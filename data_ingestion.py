import pandas as pd
import logging
from db_config import Config
from queries import Queries
import sys

config = Config()
sql_query = Queries()


logging.basicConfig(level=logging.DEBUG)
logging.info('Reading the input data')
df = pd.read_csv('/Users/harsha/Documents/Data/incubyte_assignment/medical_data.txt', delimiter='|')
df = df.drop(df.columns[:2], axis=1)
df['DOB'] = pd.to_datetime(df['DOB'], format='%m%d%Y').dt.date


logging.info('Performing Quality Checks on the input data')
#Rejected data can be used in error reports if needed. 
#The data can be further used for other purposes which are not implemented right now.
rejected_data = df[(df['Customer_Name']==None) | (df['Customer_Id'] == None) | (df['Open_Date'] == None)]


#Removing data that does not meet the requirements
df = df[(df['Customer_Name'] != None) | (df['Customer_Id'] != None) | (df['Open_Date'] != None)]

#Creating a Connection to execute queries
connection = config.sql_connect(config.db_config)


logging.info('Writing data into the Stage Table')
df.to_sql(con=connection, name = config.stage_tbl ,if_exists='replace', index = False)


#Fetching Country List from Customer_Details_Stage Table
result = connection.execute("SELECT DISTINCT(Country) FROM customer_details_stage")
res = result.fetchall()
country_list = [i[0] for i in res]



#Writing the data to their respective Country Tables  (WITH INCREMENTAL INSERT)
#A more robust approach to handle incremental inserts (which may have duplicates)

logging.info('Inserting Data to respective Country Table')
try:
    for country in country_list:
        if country in config.countries_lookup:
            #Inserting only New Records into Country Tables
            insert_query = sql_query.query_gen('insert',country)
            result = connection.execute(insert_query)
except:
    #To be implemented based on the use case
    print(sys.exc_info()[0], "occurred.")

logging.info('Data Ingestion Complete')
