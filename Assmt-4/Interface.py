#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    # if ratingMinValue < 0 or ratingMaxValue>5:
    #     print "Invalid input"
    #     exit()
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    all_table = cur.fetchall()
    f = open('RangeQueryOut.txt', 'w')
    for tab in all_table:
        if 'rangeratingspart' in tab[0] or 'roundrobinratingspart' in tab[0]:
            if tab[0]=='rangeratingspart0':
                cur.execute("SELECT * FROM "+tab[0]+" WHERE rating >= "+str(ratingMinValue)+" AND rating <= " + str(ratingMaxValue)+";")
                val = cur.fetchall()
                if len(val) != 0:
                        for v in val:
                            f.write("RangeRatingsPart" + str(tab[0][-1]) + ',' + str(v[0]) + ',' + str(v[1]) + ',' + str(v[2]) +'\n')
            else:
                cur.execute("SELECT * FROM "+tab[0]+" WHERE rating >= "+str(ratingMinValue)+" AND rating <= " + str(ratingMaxValue)+";")
                val = cur.fetchall()
                if len(val) != 0:
                    for v in val:
                        if "range" in tab[0]:
                            f.write("RangeRatingsPart" + str(tab[0][-1]) + ',' + str(v[0]) + ',' + str(v[1]) + ',' + str(v[2]) +'\n')
                        else:
                            f.write("RoundRobinRatingsPart" + str(tab[0][-1]) + ',' + str(v[0]) + ',' + str(v[1]) + ',' + str(v[2]) +'\n')
    f.close() 



def PointQuery(ratingsTableName, ratingValue, openconnection):
    # if ratingValue>5 or ratingValue<0:
    #     print "Invalid input"
    #     exit()
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    all_table = cur.fetchall()
    f = open('PointQueryOut.txt', 'w')
    for tab in all_table:
        if 'rangeratingspart' in tab[0] or 'roundrobinratingspart' in tab[0]:
                cur.execute("SELECT * FROM "+tab[0]+" WHERE rating = "+str(ratingValue)+";")
                val = cur.fetchall()
                if len(val) != 0:
                    for v in val:
                        if "range" in tab[0]:
                            f.write("RangeRatingsPart" + str(tab[0][-1]) + ',' + str(v[0]) + ',' + str(v[1]) + ',' + str(v[2]) +'\n')
                        else:
                            f.write("RoundRobinRatingsPart" + str(tab[0][-1]) + ',' + str(v[0]) + ',' + str(v[1]) + ',' + str(v[2]) +'\n')
    f.close()
   
def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
