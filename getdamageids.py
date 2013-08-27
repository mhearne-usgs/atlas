#!/usr/bin/env python

#stdlib
import sys

#third party imports
import MySQLdb as mysql

#local
from atlas2db import getDataBaseConnections

THRESH = 1000

CONFIGFILE = 'smconfig.ini'

DAMAGETABLES = {'pde':['damage',
                       'casualty'],
                'pdeisc':['damage',
                       'casualty'],
                'emdat':['fatalities','injured','affected','homeless','totalaffected','loss'],
                'htd':['tsudeaths','tsuinjuries','tsudamage','tsuhouses','eventdeaths',
                       'eventinjuries','eventdamage','eventhouses'],
                'noaa':['deaths','injuries','damage','dedamage','bdestroyed','bdamaged'],
                'utsu':['deaths','injuries','fireflag','damage']}

LOCHIERARCHY = ['atlas_event','other','centennial','pde','pdeisc','noaa']

def getHypocenter(cursor,eid):
    #now loop through contributing tables, looking for the best location/magnitude
    foundLocation = False
    idx = 0
    lat = None
    lon = None
    depth = None
    time = None
    for table in LOCHIERARCHY:
        query = 'SELECT lat,lon,depth,time FROM %s WHERE eid = %i' % (table,eid)
        try:
            cursor.execute(query)
        except:
            pass
        lrow = cursor.fetchone()
        if lrow is None:
            continue
        else:
            lat = lrow[0]
            lon = lrow[1]
            depth = lrow[2]
            time = lrow[3]
            break

    return (lat,lon,depth,time)

def getBigEvents(expfile):
    #read the output of the pagerlite process
    f = open(expfile,'rt')
    header = f.readline()
    bigevents = []
    for line in f.readlines():
        parts = line.split(',')
        eid = parts[0].replace('us','')
        exp5 = int(parts[10])
        hasExposure = exp5 >= THRESH
        bigevents.append(eid)
    f.close()
    return bigevents

if __name__ == '__main__':
    shakehome = sys.argv[1]
    csvfile = sys.argv[2]

    #get the list of events that have large exposures
    bigevents = getBigEvents(csvfile)
    
    dbdict = getDataBaseConnections(shakehome)
    atlas = dbdict['atlas']
    connection = mysql.connect(db=atlas['database'],user=atlas['user'],passwd=atlas['password'],host='127.0.0.1')
    cursor = connection.cursor()
    
    query1 = 'SELECT id FROM event WHERE time >= "1973-01-01" order by time'
    cursor.execute(query1)
    rows = cursor.fetchall()
    damevents = []
    for row in rows:
        eid = row[0]
        lat,lon,depth,time = getHypocenter(cursor,eid)
        eventcode = time.strftime('%Y%m%d%H%M%S')
        for table,trows in DAMAGETABLES.iteritems():
            nuggets = []
            query = 'SELECT count(*) FROM %s WHERE eid=%i AND (' % (table,eid)
            for trow in trows:
                nuggets.append('(%s IS NOT NULL AND %s > 0)' % (trow,trow))
            query += ' OR '.join(nuggets) + ')'
            cursor.execute(query)
            foundDamage = cursor.fetchone()[0]
            if foundDamage:
                print '%s: %s' % (eid,table)
                damevents.append(eventcode)
                continue

    cursor.close()
    connection.close()

    allevents = list(set(bigevents).union(set(damevents)))
    for event in allevents:
        print event
