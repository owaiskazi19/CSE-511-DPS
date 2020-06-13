
#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from io import StringIO

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS " + ratingstablename + " (userid INTEGER, movieid INTEGER,rating DECIMAL, temp TEXT)")
    with open(ratingsfilepath, "r") as f:
        file = StringIO(f.read().replace("::", ","))
        cur.copy_from(file, ratingstablename, sep=",")
    cur.execute("ALTER TABLE " + ratingstablename +  " DROP COLUMN IF EXISTS temp")
    openconnection.commit()
    cur.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    part = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(0, numberofpartitions):
        mRange = i * part
        mxRange = mRange + part
        new_table = RANGE_TABLE_PREFIX + str(i)
        cur.execute("CREATE TABLE " + new_table + " (userid integer, movieid integer, rating float);")
        if i == 0:
            cur.execute("INSERT INTO " + new_table + " (userid, movieid, rating) select userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(mRange) + " and rating <= " + str(mxRange) + ";")
        else:
            cur.execute("INSERT INTO " + new_table + " (userid, movieid, rating) select userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(mRange) + " and rating <= " + str(mxRange) + ";")
    cur.close()
    con.commit()

    
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE rrobin_temporary AS SELECT *, row_number() OVER (ORDER BY (SELECT 100)) - 1 AS row_num FROM " + ratingstablename + "")
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    count = cur.fetchone()[0]
    cur.execute("CREATE TABLE IF NOT EXISTS rrobin_counter (counter INTEGER, num_of_part INTEGER)")
    cur.execute("TRUNCATE rrobin_counter")
    cur.execute("INSERT INTO rrobin_counter (counter, num_of_part) VALUES (" + str((count - 1) % numberofpartitions) + "," +str(numberofpartitions)+")")
    for i in range(0, numberofpartitions):
        query = "CREATE TABLE rrobin_part" + str(i) + " AS SELECT * FROM rrobin_temporary WHERE MOD(row_num, "+str(numberofpartitions)+") = " + str(i) + ""
        cur.execute(query)
    cur.execute("DROP TABLE rrobin_temporary")
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    tb_nm_prefix = "rrobin_part"
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM rrobin_counter")
    r_query = cur.fetchone()
    num = r_query[1]
    p = (r_query[0] + 1) % num
    cur.execute("INSERT INTO rrobin_part" + str(p) + " VALUES ("+ str(userid) +  "," + str(itemid) + "," +str(rating)+")")
    cur.execute("INSERT INTO " +str(ratingstablename)+ " VALUES (" + str(userid) +  "," + str(itemid) + "," +str(rating)+")")
    cur.execute("UPDATE rrobin_counter SET counter = " +str(p)+ " WHERE num_of_part = " + str(num) + "")
    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE = 'range_part'
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + RANGE_TABLE + "%';")
    count = cur.fetchone()[0]
    part = 5 / count
    i = int(rating / part)
    if rating % part == 0 and i != 0:
        i = i - 1
    new_table = RANGE_TABLE + str(i)
    cur.execute("INSERT INTO " + new_table + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))
    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except (psycopg2.DatabaseError, e):
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    except (IOError, e):
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
