#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'

NO_OF_THREADS = 5

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    con = openconnection.cursor()
    con.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + ";")
    mVal = con.fetchone()[0]

    con.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + ";")
    mxVal = con.fetchone()[0]

    interval = float (mxVal-mVal) / NO_OF_THREADS

    con.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable + "\';")
    InputTblSchema = con.fetchall()

    for i in range(NO_OF_THREADS):
        tableName = "range_part" + str(i)
        con.execute("CREATE TABLE " + tableName + " (" + InputTblSchema[0][0] + " " + InputTblSchema[0][1] + ");")
        for each in range(1,len(InputTblSchema)):
            con.execute("ALTER TABLE " + tableName + " ADD COLUMN " + InputTblSchema[each][0] + " " + InputTblSchema[each][1] + ";")
    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        if i == 0:
            mnval = mVal
            mxval = mnval + interval
        else:
            mnval = mxval
            mxval = mxval + interval

        threads[i] = threading.Thread(target=Sorted,args=(InputTable,SortingColumnName,i,mnval,mxval,openconnection))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    con.execute("CREATE TABLE " + OutputTable + " (" + InputTblSchema[0][0] + " INTEGER);")
    for each in range(1,len(InputTblSchema)):
            con.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTblSchema[each][0] + " " + InputTblSchema[each][1] + ";")
    for i in range(NO_OF_THREADS):
        con.execute("INSERT INTO " + OutputTable + " SELECT * FROM range_part" + str(i) + ";")

    for i in range(NO_OF_THREADS):
        con.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")

    openconnection.commit()         

def Sorted(InputTable,SortingColumnName,i,mnval,mxval,openconnection):
    con = openconnection.cursor()
    tblName = "range_part" + str(i)
    if i == 0:
        t_query = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(mnval) + " AND " + SortingColumnName + " <= " + str(mxval) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        t_query = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(mnval) + " AND " + SortingColumnName + " <= " + str(mxval) + " ORDER BY " + SortingColumnName + " ASC;"      
    con.execute(t_query)
    return

    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    con = openconnection.cursor()
    con.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    mintbl1 = float(con.fetchone()[0])

    con.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    mintbl2 = float(con.fetchone()[0])

    con.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    maxtbl1 = float(con.fetchone()[0])

    con.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    maxtbl2 = float(con.fetchone()[0])

    maxtbl = max(maxtbl1,maxtbl2)
    mintbl = min(mintbl1,mintbl2)
    interval = (maxtbl - mintbl) / NO_OF_THREADS

    con.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable1 + "\';")
    InputTableSchema1 = con.fetchall()

    con.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';")
    InputTableSchema2 = con.fetchall()

    con.execute("CREATE TABLE " + OutputTable + " (" + InputTableSchema1[0][0] + " INTEGER);")

    for each in range(1,len(InputTableSchema1)):
        con.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";")

    for each in range(len(InputTableSchema2)):
        con.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";")    

    for i in range(NO_OF_THREADS):
        tblName = "inputtable1_" + str(i)
        if i == 0:
            mnval = mintbl
            mxval = mnval + interval
            t_query = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(mnval) + " AND " + Table1JoinColumn + " <= " + str(mxval)  + ";"
        else:
            mnval = mxval
            mxval = mnval + interval
            t_query = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(mnval) + " AND " + Table1JoinColumn + " <= " + str(mxval)  + ";"
        con.execute(t_query)
    
    for i in range(NO_OF_THREADS):
        tblName = "inputtable2_" + str(i)
        if i == 0:
            mnval = mintbl
            mxval = mnval + interval
            t_query = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(mnval) + " AND " + Table2JoinColumn + " <= " + str(mxval) + ";"
        else:
            mnval = mxval
            mxval = mnval + interval
            t_query = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(mnval) + " AND " + Table2JoinColumn + " <= " + str(mxval) + ";"
        con.execute(t_query)

    for i in range(NO_OF_THREADS):
        OutputTableRange = "outtable_range" + str(i)
        t_query = "CREATE TABLE " + OutputTableRange + " (" + InputTableSchema1[0][0] + " INTEGER);"
        con.execute(t_query)

        for each in range(1,len(InputTableSchema1)):
            con.execute("ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";")

        for each in range(len(InputTableSchema2)):
            con.execute("ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";")

    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        threads[i] = threading.Thread(target=Join,args=(Table1JoinColumn,Table2JoinColumn,openconnection,i))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    for i in range(NO_OF_THREADS):
        con.execute("INSERT INTO " + OutputTable + " SELECT * FROM outtable_range" + str(i) + ";")

    for i in range(NO_OF_THREADS):
        t_query = "DROP TABLE IF EXISTS inputtable1_" + str(i) + ";" 
        t_query1 = "DROP TABLE IF EXISTS inputtable2_" + str(i) + ";" 
        t_query2 = "DROP TABLE IF EXISTS outtable_range" + str(i) + ";" 
        con.execute(t_query)   
        con.execute(t_query1) 
        con.execute(t_query2) 

    openconnection.commit()

def Join(Table1JoinColumn,Table2JoinColumn,openconnection,i):
    con = openconnection.cursor()
    con.execute("""INSERT INTO outtable_range""" + str(i) + """ SELECT * FROM inputtable1_""" + str(i) + """ INNER JOIN inputtable2_""" + str(i) +""" ON inputtable1_""" + str(i) + """.""" + str(Table1JoinColumn).lower() + """ = inputtable2_""" + str(i) + """.""" + str(Table2JoinColumn).lower() + """;""")
    return 
