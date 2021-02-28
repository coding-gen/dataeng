# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv

DBname = "storact"
DBuser = "sauser"
DBpwd = "userasaur"
# Target table: CensusData
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015

def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  Year = args.year

# connect to the database
def dbconnect():
  connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
  )
  connection.autocommit = True
  return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

  with conn.cursor() as cursor:
    cursor.execute(f"""
          SET temp_buffers = 32768;
          DROP TABLE IF EXISTS {TableName};
          CREATE TABLE {TableName} (
              Year                INTEGER,
              CensusTract         NUMERIC,
              State               TEXT,
              County              TEXT,
              TotalPop            INTEGER,
              Men                 INTEGER,
              Women               INTEGER,
              Hispanic            DECIMAL,
              White               DECIMAL,
              Black               DECIMAL,
              Native              DECIMAL,
              Asian               DECIMAL,
              Pacific             DECIMAL,
              Citizen             DECIMAL,
              Income              DECIMAL,
              IncomeErr           DECIMAL,
              IncomePerCap        DECIMAL,
              IncomePerCapErr     DECIMAL,
              Poverty             DECIMAL,
              ChildPoverty        DECIMAL,
              Professional        DECIMAL,
              Service             DECIMAL,
              Office              DECIMAL,
              Construction        DECIMAL,
              Production          DECIMAL,
              Drive               DECIMAL,
              Carpool             DECIMAL,
              Transit             DECIMAL,
              Walk                DECIMAL,
              OtherTransp         DECIMAL,
              WorkAtHome          DECIMAL,
              MeanCommute         DECIMAL,
              Employed            INTEGER,
              PrivateWork         DECIMAL,
              PublicWork          DECIMAL,
              SelfEmployed        DECIMAL,
              FamilyWork          DECIMAL,
              Unemployment        DECIMAL
          );  
      """)

    print(f"Created {TableName}")

def load(conn, Datafile, TableName):

  with conn.cursor() as cursor:
    print(f"Loading {len(Datafile)} rows")
    start = time.perf_counter()

    f = open(Datafile, mode="r")
    next(f)

    cursor.copy_from(f, TableName, sep=',', null='', size=32768, columns=( \
      'CensusTract','State','County','TotalPop','Men','Women',\
      'Hispanic','White','Black','Native','Asian','Pacific','Citizen',\
      'Income','IncomeErr','IncomePerCap','IncomePerCapErr','Poverty',\
      'ChildPoverty','Professional','Service','Office','Construction',\
      'Production','Drive','Carpool','Transit','Walk','OtherTransp',\
      'WorkAtHome','MeanCommute','Employed','PrivateWork','PublicWork',\
      'SelfEmployed','FamilyWork','Unemployment')
    )
    f.close()
    total_time = time.perf_counter() - start
    print(f'Finished Loading. \n' + \
      f'Total elapsed Time: {total_time:0.4} seconds \n' )


def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
      createTable(conn)

    load(conn, Datafile, TableName)

if __name__ == "__main__":
    main()