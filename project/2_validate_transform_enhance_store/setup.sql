# get setup

create database ctran;
create user vancouver with password 'washington';
grant all privileges on ctran to vancouver;

# Usage: psql -U vancouver -d ctran -h0
