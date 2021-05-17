# Data Pipeline implementation for ingesting medical records to SQL Tables

## Objective
The main objective of this project is to ingest Pipe Delimited Files into MySql Tables.

## Data
The Pipe Delimited Files contain data of Cutomer Records from a Hospital. 
The data needs to be ingested into Country specific SQL Tables based on the Customer Country.

## Workflow

1. The Pipe Delimited File is first loaded by the Python Script
2. The data is then ingested into the Stage Table
3. The data from the Stage Table is then inserted to the Country Tables based on the Country mentioned in the Country Column in each record.
![image](https://user-images.githubusercontent.com/43230261/117928612-6da4c000-b319-11eb-9087-c98d50f2b044.png)


### Required Packages
1. pymysql
2. sqlalchemy
3. pandas

### Contents
1. db_config.py: This file contains a class which is used to create connections for executing queries. It also contains the Database Credentials, Stage Table name and Country Table names. All the changes regarding Database Credentials and Table Names must be done in this class.
2. queries.py: This file contains a class which has the queries for insert and update. Any change in the queries must be done in this class.
3. data_ingestion.py: This file contains the code to execute the data ingestion, from loading the Pipe Delimited Files to ingesting the data to Stage Table and finally to the Country Tables from the Stage Table.
4. sql_queries.txt: This file contains all the SQL queries used in this project. Also contains Table creation queries. The file is created only for reference.

### Data Ingestion Approach

To handle incremental inserts (which may have duplicates), the following query is written:

The insert query first does a comparison between the Stage Table and Country Table and inserts only New Records into the Country Table.

### Steps to run the code
1. Make sure all the required packages are installed.
2. Run data_ingestion.py

### Results Screenshots

#### Stage Table
![image](https://user-images.githubusercontent.com/43230261/117933335-16a1e980-b31f-11eb-94bf-8b30b9dededd.png)

#### Country Tables

##### Country Table - India
![image](https://user-images.githubusercontent.com/43230261/117933720-7e583480-b31f-11eb-94d0-6595f39d5284.png)

##### Country Table - USA
![image](https://user-images.githubusercontent.com/43230261/117933786-9334c800-b31f-11eb-91a9-d2f4feb458f9.png)

##### Country Table - Phil
![image](https://user-images.githubusercontent.com/43230261/117933862-a6479800-b31f-11eb-996c-30a696175089.png)



