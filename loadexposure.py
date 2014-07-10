#!/usr/bin/env python

#stdlib
from xml.dom import minidom
import os.path
import mysql.connector as mysql
import sys

if __name__ == '__main__':
    expofile = sys.argv[1]
    connection = mysql.connect(db='atlas',user='atlas',passwd='atlas',buffered=True)
    cursor = connection.cursor()
    root = minidom.parse(expofile)
    events = root.getElementsByTagName('event')
    for event in events:
        eventcode = event.getAttribute('code')[2:]
        query = 'SELECT id FROM atlas_event WHERE eventcode="%s"' % eventcode
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            print 'Could not find atlas event in database with event code of "%s".  Skipping.' % eventcode
            continue
        eid = row[0]
        countries = event.getElementsByTagName('exposure')
        for country in countries:
            ccode = country.getAttribute('ccode')
            exposures = [str(int(exp)) for exp in country.firstChild.data.split()]
            valuestr = ','.join(exposures)
            expstr = ','.join(['exp'+str(i) for i in range(1,11)])
            query = 'INSERT INTO atlas_exposure (event_id,ccode,%s) VALUES (%i,"%s",%s)' % (expstr,eid,ccode,valuestr)
            cursor.execute(query)
            connection.commit()
    cursor.close()
    connection.close()
