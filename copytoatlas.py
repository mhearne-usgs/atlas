#!/usr/bin/env python

#stdlib
from xml.dom import minidom
import os.path
import sys
from datetime import datetime

#third party
import mysql.connector as mysql

#local
from atlas2db import getDamageEvents

MAGHIERARCHY = ['other','cmt','pde-Mw','pdeisc-Mw','centennial','pde','pdeisc']
LOCHIERARCHY = ['other','centennial','pde','pdeisc','noaa']
TIMEFMT = '%Y-%m-%d %H:%M:%S'
START = '1970-01-01'

def getMagnitude(eid):
    magnitude = None
    for table in MAGHIERARCHY:
        if table.find('-') > 0:
            parts = table.split('-')
            newtable = parts[0]
            magtype = parts[1]
            query = 'SELECT magnitude FROM %s WHERE eid=%i AND magtype="%s"' % (newtable,eid,magtype)
        else:
            query = 'SELECT magnitude from %s WHERE eid=%i' % (table,eid)
        try:
            cursor.execute(query)
        except:
            pass
        row = cursor.fetchone()
        if row is not None:
            magnitude = row[0]
            break
    return magnitude

def getLocation(eid):
    time = None
    lat = None
    lon = None
    depth = None
    for table in LOCHIERARCHY:
        query = 'SELECT time,lat,lon,depth from %s WHERE eid=%i' % (table,eid)
        try:
            cursor.execute(query)
        except:
            pass
        row = cursor.fetchone()
        if row is not None:
            time = row[0]
            lat = row[1]
            lon = row[2]
            depth = row[3]
            break
    return (time,lat,lon,depth)

if __name__ == '__main__':
    connection = mysql.connect(db='atlas',user='atlas',passwd='atlas')
    cursor = connection.cursor()

    eidlist = getDamageEvents(cursor)

    for eid in eidlist:
        query = 'SELECT id FROM atlas_event WHERE eid=%i AND TIME >= "%s"' % (eid,START)
        cursor.execute(query)
        row = cursor.fetchone()
        if row is not None:
            continue
        magnitude = getMagnitude(eid)
        time,lat,lon,depth = getLocation(eid)
        if magnitude is None or time is None:
            print 'NULL values for eid %i' % eid
            sys.exit(1)
        eventcode = time.strftime('%Y%m%d%H%M%S')
        created = datetime.now().strftime(TIMEFMT)
        inserttime = datetime.now().strftime(TIMEFMT)
        network = 'us'
        fmt = '(%i,"%s","%s",%.4f,%.4f,%.1f,%.1f,"%s","%s","%s")'
        tpl = (eid,eventcode,time,lat,lon,depth,magnitude,created,inserttime,network)
        try:
            insert_string = fmt % tpl
        except:
            pass
        query = 'INSERT INTO atlas_event (eid,eventcode,time,lat,lon,depth,magnitude,created,inserttime,network) VALUES %s' % insert_string
        cursor.execute(query)
        connection.commit()
        
    
    cursor.close()
    connection.close()
