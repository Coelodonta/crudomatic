# crudomatic
Python 3 script to generate basic CRUD (Create Read Update Delete) stored procedures for relational databases

Outputs two files:
- "./crudomatic_procedures.sql" : SQL DDL to generate database stored procedures
- "./drop_crudomatic_procedures.sql" : SQL to drop the generated database stored procedures

Current version supports MySQL stored procedures for:
- Insert one row
- Insert one row using column defaults
- Select one row using primary key
- Select all rows
- Update one row
- Update one row using column defaults
- Delete one row using primary key

Requirements:
- Python3
- module mysql

# Usage:
./crudomatic.py <database> <user> <password> [RDBMS] [host]

# Road Map:
- Add PostgresQL support
- Add better command line support
- Add MS SQL support
- Add support for two table joins

