# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
from psycopg2 import sql
import argparse
import re
import csv

DBname = "storact"
DBuser = "sauser"
DBpwd = "userasaur"
# Target table: CensusData
TableName = 'CensusStaging'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015

def row2vals(row):
  # handle the null vals
  for key in row:
    if not row[key]:
      row[key] = 0
    row['County'] = row['County'].replace('\'','')  # eliminate quotes within literals

  ret = (
       Year,                          # -- Year
       row['CensusTract'],            # -- CensusTract
       str(row['State']),                # -- State
       str(row['County']),               # -- County
       row['TotalPop'],               # -- TotalPop
       row['Men'],                    # -- Men
       row['Women'],                  # -- Women
       row['Hispanic'],               # -- Hispanic
       row['White'],                  # -- White
       row['Black'],                  # -- Black
       row['Native'],                 # -- Native
       row['Asian'],                  # -- Asian
       row['Pacific'],                # -- Pacific
       row['Citizen'],                # -- Citizen
       row['Income'],                 # -- Income
       row['IncomeErr'],              # -- IncomeErr
       row['IncomePerCap'],           # -- IncomePerCap
       row['IncomePerCapErr'],        # -- IncomePerCapErr
       row['Poverty'],                # -- Poverty
       row['ChildPoverty'],           # -- ChildPoverty
       row['Professional'],           # -- Professional
       row['Service'],                # -- Service
       row['Office'],                 # -- Office
       row['Construction'],           # -- Construction
       row['Production'],             # -- Production
       row['Drive'],                  # -- Drive
       row['Carpool'],                # -- Carpool
       row['Transit'],                # -- Transit
       row['Walk'],                   # -- Walk
       row['OtherTransp'],            # -- OtherTransp
       row['WorkAtHome'],             # -- WorkAtHome
       row['MeanCommute'],            # -- MeanCommute
       row['Employed'],               # -- Employed
       row['PrivateWork'],            # -- PrivateWork
       row['PublicWork'],             # -- PublicWork
       row['SelfEmployed'],           # -- SelfEmployed
       row['FamilyWork'],             # -- FamilyWork
       row['Unemployment']            # -- Unemployment
  )
  return ret


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

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
  print(f"readdata: reading from File: {fname}")
  with open(fname, mode="r") as fil:
    dr = csv.DictReader(fil)
    headerRow = next(dr)
    # print(f"Header: {headerRow}")

    rowlist = []
    for row in dr:
      rowlist.append(row)

  return rowlist

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
          CREATE TEMP TABLE {TableName} (
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

def load(conn, rowlist):

  with conn.cursor() as cursor:
    print(f"Loading {len(rowlist)} rows")
    start = time.perf_counter()
  
    cmd = sql.SQL("INSERT INTO CensusStaging VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);")

    valstr = []
    for row in rowlist:
      valstr.append(row2vals(row))

    cursor.executemany(cmd,valstr)

    load_time = time.perf_counter() - start

    cursor.execute(f"""
      INSERT INTO CensusData SELECT * FROM CensusStaging;
      DROP TABLE CensusStaging;
    """)

  total_time = time.perf_counter() - start
  append_time = total_time - load_time
  print(f'Finished Loading. \n' + \
    f'Total elapsed Time: {total_time:0.4} seconds \n' + \
    f'Load time: {load_time:0.4} seconds \n' + \
    f'Append and drop stage time: {append_time:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)

    if CreateDB:
      createTable(conn)

    load(conn, rlis)

if __name__ == "__main__":
    main()