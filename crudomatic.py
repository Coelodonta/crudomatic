#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  crudomatic.py
#  
#  Copyright 2019 Coelodonta
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  
import mysql.connector

"""
Base class for RDBMS procedure creation syntax
"""
class RDBMSBaseSyntax:
	def generate(self,db,usr,pwd,svr):
		pass;

"""
Procedure creation syntax for MySQL
Used to generate two SQL files :
./crudomatic_procedures.sql - Create CRUD stored procedures
./drop_crudomatic_procedures.sql - Drop CRUD stored procedures
"""
class MySQLSyntax(RDBMSBaseSyntax):
	drops=None	
	out=None
	db=None
	cnt=0
		
	def generateSelectAll(self,cols,tableName):
		print("-- Select all rows from "+tableName,file=self.out)
		self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_SelectAll;\n")
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_SelectAll;\n")
		self.out.write("DELIMITER // ;\n")

		fields=""
		comma=""

		for col in cols:
			fields+=comma
			fields+="`"+col[0]+"` "
			comma=","

		self.out.write("CREATE PROCEDURE "+tableName+"_SelectAll()\nSQL SECURITY INVOKER\nBEGIN\n")
		self.out.write("\tSELECT "+fields+" FROM `"+tableName+"`;\n") 
		self.out.write("END//\n");
		self.out.write("DELIMITER ;\n")
		self.cnt+=1
		
	def generateSelectOne(self,cols,tableName):
		print("-- Select single row using Primary Key from "+tableName,file=self.out)
		self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_SelectOne;\n")
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_SelectOne;\n")
		self.out.write("DELIMITER // ;\n")
		
		fields=""
		args=""
		argscomma=""
		comma=""
		where=""
		ands=""
		
		for col in cols:
			fields+=comma
			fields+="`"+col[0]+"` "
			comma=","
			if col[3]=="PRI":
				args+=argscomma
				args+="IN arg_"+col[0]+" "+col[1]
				argscomma=","
				where+=ands+"`"+col[0]+"`=arg_"+col[0]
				ands=" AND " 	
			 
		self.out.write("CREATE PROCEDURE "+tableName+"_SelectOne("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
		self.out.write("\tSELECT "+fields+" FROM `"+tableName+"` WHERE "+where+";\n") 
		self.out.write("END//\n");
		self.out.write("DELIMITER ;\n")
		self.cnt+=1
		
	def generateDeleteOne(self,cols,tableName):
		print("-- Delete single row using Primary Key from "+tableName,file=self.out)
		self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_DeleteOne;\n")
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_DeleteOne;\n")
		self.out.write("DELIMITER // ;\n")

		args=""
		comma=""
		where=""
		ands=""

		for col in cols:
			if col[3]=="PRI":
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				comma=","
				where+=ands+"`"+col[0]+"`=arg_"+col[0]
				ands=" AND " 	
			 
		self.out.write("CREATE PROCEDURE "+tableName+"_DeleteOne("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
		self.out.write("\tDELETE FROM `"+tableName+"` WHERE "+where+";\n") 
		self.out.write("END//\n");
		self.out.write("DELIMITER ;\n")
		self.cnt+=1
		
	def generateInsert(self,cols,tableName):
		print("-- Insert row into "+tableName,file=self.out)
		self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_Insert;\n")
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_Insert;\n")
		self.out.write("DELIMITER // ;\n")
		
		fields=""
		args=""
		comma=""
		values=""
		
		for col in cols:
			if col[5]!="auto_increment":
				fields+=comma
				fields+="`"+col[0]+"` "
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				values+=comma
				values+="arg_"+col[0]
				comma=","
			 
		self.out.write("CREATE PROCEDURE "+tableName+"_Insert("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
		self.out.write("\tINSERT INTO `"+tableName+"` ("+fields+") VALUES ("+values+");\n") 
		self.out.write("END//\n");
		self.out.write("DELIMITER ;\n")
		self.cnt+=1

	def generateInsertWithDefaults(self,cols,tableName):
		print("-- Insert row using default values into "+tableName,file=self.out)
		self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_InsertUsingDefaults;\n")
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_InsertUsingDefaults;\n")
		self.out.write("DELIMITER // ;\n")
		
		fields=""
		args=""
		comma=""
		values=""
		
		for col in cols:
			if col[2]=="YES":
				# Skip nullable columns
				continue;
			if col[5]!="auto_increment":
				fields+=comma
				fields+="`"+col[0]+"` "
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				values+=comma
				values+="arg_"+col[0]
				comma=","
			 
		self.out.write("CREATE PROCEDURE "+tableName+"_InsertUsingDefaults("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
		self.out.write("\tINSERT INTO `"+tableName+"` ("+fields+") VALUES ("+values+");\n") 
		self.out.write("END//\n");
		self.out.write("DELIMITER ;\n")
		self.cnt+=1

		
	def generateUpdate(self,cols,tableName):
		print("-- Update row using primary key for "+tableName,file=self.out)
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_Update;\n")
		
		fields=""
		args=""
		comma=""
		fldscomma=""
		where=""
		ands=""
		
		for col in cols:
			if col[3]!="PRI":
				# Update if not part of  primary key
				fields+=fldscomma
				fldscomma=","
				fields+="`"+col[0]+"`=arg_"+col[0]
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				comma=","
			else:
				where+=ands
				where+="`"+col[0]+"`=arg_"+col[0]
				ands=" AND " 	
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				comma=","

		if len(fields)>0:		
			self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_Update;\n")
			self.out.write("DELIMITER // ;\n")
			self.out.write("CREATE PROCEDURE "+tableName+"_Update("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
			self.out.write("\tUPDATE `"+tableName+"` SET "+fields+" WHERE "+where+";\n") 
			self.out.write("END//\n");
			self.out.write("DELIMITER ;\n")
			self.cnt+=1
		else:
			self.out.write("-- **********************************\n")
			self.out.write("-- WARNING! NO PROCEDURE GENERATED:\n")
			self.out.write("-- "+tableName+"_Update\n")
			self.out.write("-- **********************************\n")
			

	def generateUpdateWithDefaults(self,cols,tableName):
		print("-- Update row using default values and primary key for "+tableName,file=self.out)
		self.drop.write("DROP PROCEDURE IF EXISTS "+tableName+"_UpdateUsingDefaults;\n")
		
		fields=""
		args=""
		comma=""
		fldcomma=""
		where=""
		ands=""
		
		for col in cols:
			if col[2]=="YES":
				# Skip nullable columns
				continue;
			if col[3]!="PRI":
				# Update if not part of  primary key
				fields+=fldcomma
				fields+="`"+col[0]+"`=arg_"+col[0]
				fldcomma=","
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				comma=","
			else:
				where+=ands
				where+="`"+col[0]+"`=arg_"+col[0]
				ands=" AND " 	
				args+=comma
				args+="IN arg_"+col[0]+" "+col[1]
				comma=","
		if len(fields)>0:
			self.out.write("DROP PROCEDURE IF EXISTS "+tableName+"_UpdateUsingDefaults;\n")
			self.out.write("DELIMITER // ;\n")
			self.out.write("CREATE PROCEDURE "+tableName+"_UpdateUsingDefaults("+args+")\nSQL SECURITY INVOKER\nBEGIN\n")
			self.out.write("\tUPDATE `"+tableName+"` SET "+fields+" WHERE "+where+";\n") 
			self.out.write("END//\n");
			self.out.write("DELIMITER ;\n")
			self.cnt+=1
		else:
			self.out.write("-- **********************************\n")
			self.out.write("-- WARNING! NO PROCEDURE GENERATED:\n")
			self.out.write("-- "+tableName+"_UpdateUsingDefaults\n")
			self.out.write("-- **********************************\n")

	def	processTable(self,tableName):
		print("-- ******************************",file=self.out)
		print("-- TABLE: "+tableName,file=self.out)
		print("-- ******************************",file=self.out)
		
		columnCursor=self.db.cursor()
		columnCursor.execute("DESCRIBE `"+tableName+"`")
		cols=columnCursor.fetchall()
		
		# cols is a list of tuples. Tuples have following format
		# (Field,Type,Nullable,Key,Default,Extra)
		self.generateSelectAll(cols,tableName)
		self.generateSelectOne(cols,tableName)
		self.generateDeleteOne(cols,tableName)
		self.generateInsert(cols,tableName)
		self.generateInsertWithDefaults(cols,tableName)
		self.generateUpdate(cols,tableName)
		self.generateUpdateWithDefaults(cols,tableName)
		print("-- END TABLE: "+tableName,file=self.out)
		
	def generate(self,db,usr,pwd,svr):
		self.out=open("./crudomatic_procedures.sql","w")
		self.out.write("-- DB: MySQL "+db+"\n")
		self.out.write("use "+db+";\n")

		self.drop=open("./drop_crudomatic_procedures.sql","w")
		self.drop.write("use "+db+";\n")


		self.db=mysql.connector.connect(host=svr,user=usr,
		passwd=pwd,database=db)
		
		tableCursor=self.db.cursor()
		tableCursor.execute("SHOW TABLES")
		tables=tableCursor.fetchall()
		for x in tables:
			tableName=x[0] # First element in tuple is table name
			print("Processing table "+tableName)
			self.processTable(tableName)
			
		self.out.write("-- *******************************\n");
		self.out.write("-- Run command below to verify success.\n");
		self.out.write("-- There should be "+str(self.cnt)+" procedures created.\n");
		self.out.write("-- If numbers don't match, please see warnings.\n");
		self.out.write("-- show procedure status where db='"+db+"';\n");
		
		self.out.close()
		self.drop.close()	
		self.db.close()
	
"""
Entry point to crudomatic
"""		
def generateCRUD(args):
	if len(args)<4 :
		print("Usage ./crudomatic.py <database> <user> <password> [RDBMS] [host]")
		print("or")
		print("python3 crudomatic.py <database> <user> <password>  [RDBMS] [host]")
		return 0
	
	if len(args)>4:
		RDBMS=args[4] # TO DO: Add PostgresQL support. Maybe MSSQL??
	else:
		RDBMS="MySQL" 

	if len(args)>5:
		host=args[5]
	else:
		host="localhost"
	
	if RDBMS=="MySQL" :
		gen=MySQLSyntax()
		gen.generate(args[1],args[2],args[3],host)
	else:
		print("Unknown RDBMS")

	return 0

if __name__ == '__main__':
	import sys
	sys.exit(generateCRUD(sys.argv))
