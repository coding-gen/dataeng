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
Year = 2017

def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--database", required=True)
  args = parser.parse_args()

  global DBname
  DBname = args.database

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
def createAggregateTable(conn):

  with conn.cursor() as cursor:
    name_of_table = str(TableName) + "aggregated"
    cursor.execute(f"""
          DROP TABLE IF EXISTS {name_of_table};
          CREATE TABLE {name_of_table} (
              Year                INTEGER,
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

    print(f"Created {name_of_table}")

def aggregateData(conn):
  name_of_table = str(TableName) + "aggregated"
  with conn.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {name_of_table}
      select
        Year, 
        State, 
        REGEXP_REPLACE(
             County, '(.*) (County|Municipio|Parish)$', 
             '\\1'
        ) as County, 
        sum(totals.TotalPop) as TotalPop, 
        sum(Men) as Men, 
        sum(Women) as Women, 
        round(sum(Hispanic)/sum(totals.TotalPop),1) as Hispanic_pc, 
        round(sum(White)/sum(totals.TotalPop),1) as White_pc, 
        round(sum(Black)/sum(totals.TotalPop),1) as Black_pc, 
        round(sum(Native)/sum(totals.TotalPop),1) as Native_pc, 
        round(sum(Asian)/sum(totals.TotalPop),1) as Asian_pc, 
        round(sum(Pacific)/sum(totals.TotalPop),1) as Pacific_pc, 
        sum(Citizen) as Citizen, 
        round(sum(Income)/sum(totals.TotalPop),0) as Income_avg, 
        round(sum(IncomeErr)/sum(totals.TotalPop),0) as IncomeErr_avg, 
        round(sum(IncomePerCap)/sum(totals.TotalPop),0) as IncomePerCap_avg, 
        round(sum(IncomePerCapErr)/sum(totals.TotalPop),0) as IncomePerCapErr_avg,
        round(sum(Poverty)/sum(totals.TotalPop),1) as Poverty_pc, 
        round(sum(ChildPoverty)/sum(totals.TotalPop),1) as ChildPoverty_pc, 
        round(sum(Professional)/sum(totals.TotalPop),1) as Professional_pc, 
        round(sum(Service)/sum(totals.TotalPop),1) as Service_pc, 
        round(sum(Office)/sum(totals.TotalPop),1) as Office_pc, 
        round(sum(Construction)/sum(totals.TotalPop),1) as Construction_pc, 
        round(sum(Production)/sum(totals.TotalPop),1) as Production_pc, 
        round(sum(Drive)/sum(totals.TotalPop),1) as Drive_pc, 
        round(sum(Carpool)/sum(totals.TotalPop),1) as Carpool_pc, 
        round(sum(Transit)/sum(totals.TotalPop),1) as Transit_pc, 
        round(sum(Walk)/sum(totals.TotalPop),1) as Walk_pc, 
        round(sum(OtherTransp)/sum(totals.TotalPop),1) as OtherTransp_pc, 
        round(sum(WorkAtHome)/sum(totals.TotalPop),1) as WorkAtHome_pc, 
        round(sum(MeanCommute)/sum(totals.TotalPop),1) as MeanCommute_pc, 
        sum(Employed) as Employed, 
        round(sum(PrivateWork)/sum(totals.TotalPop),1) as PrivateWork_pc, 
        round(sum(PublicWork)/sum(totals.TotalPop),1) as PublicWork_pc, 
        round(sum(SelfEmployed)/sum(totals.TotalPop),1) as SelfEmployed_pc, 
        round(sum(FamilyWork)/sum(totals.TotalPop),1) as FamilyWork_pc, 
        round(sum(Unemployment)/sum(totals.TotalPop),1) as Unemployment_pc
        FROM
             (SELECT
                    Year, 
                    State, 
                    County, 
                    TotalPop,
                    Men,
                    Women,
                    Hispanic*TotalPop as Hispanic, 
                    White*TotalPop as White, 
                    Black*TotalPop as Black, 
                    Native*TotalPop as Native, 
                    Asian*TotalPop as Asian, 
                    Pacific*TotalPop as Pacific, 
                    Citizen,
                    Income*TotalPop as Income,
                    IncomeErr*TotalPop as IncomeErr,
                    IncomePerCap*TotalPop as IncomePerCap,
                    IncomePerCapErr*TotalPop as IncomePerCapErr,
                    Poverty*TotalPop as Poverty, 
                    ChildPoverty*TotalPop as ChildPoverty, 
                    Professional*TotalPop as Professional, 
                    Service*TotalPop as Service, 
                    Office*TotalPop as Office, 
                    Construction*TotalPop as Construction, 
                    Production*TotalPop as Production, 
                    Drive*TotalPop as Drive, 
                    Carpool*TotalPop as Carpool, 
                    Transit*TotalPop as Transit, 
                    Walk*TotalPop as Walk, 
                    OtherTransp*TotalPop as OtherTransp, 
                    WorkAtHome*TotalPop as WorkAtHome, 
                    MeanCommute*TotalPop as MeanCommute, 
                    Employed,
                    PrivateWork*TotalPop as PrivateWork, 
                    PublicWork*TotalPop as PublicWork, 
                    SelfEmployed*TotalPop as SelfEmployed, 
                    FamilyWork*TotalPop as FamilyWork, 
                    Unemployment*TotalPop as Unemployment 
             FROM CensusData) totals
      group by 1, 2, 3;
      """)

def main():
    initialize()
    conn = dbconnect()

    createAggregateTable(conn)
    aggregateData(conn)


if __name__ == "__main__":
    main()