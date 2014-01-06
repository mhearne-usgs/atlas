#!/usr/bin/env python

#stdlib
from xml.dom import minidom
import os.path
import MySQLdb as mysql

if __name__ == '__main__':
    expofile = sys.argv[1]
    connection = mysql.connect(db='atlas',user='atlas',passwd='atlas')
    cursor = connection.cursor()
    root = minidom.parse(expofile)
    events = root.getElementsByTagName('event')
    for event in events:
        eventcode = event.getAttribute('code')
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
            exposures = [int(exp) for exp in country.firstChild.data.split()]
            for mmi in range(0,len(exposures)):
                exp = exposures[mmi]
                query = 'INSERT INTO atlas_exposure (event_id,ccode,exp%i) VALUES (%i,"%s",%i)' % (mmi+1,eid,ccode,exp)
                cursor.execute(query)
                connection.commit()
    cursor.close()
    connection.close()
