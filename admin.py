#!/usr/bin/env python

import mysql.connector as mysql
import argparse
import sys

TABLES = ['atlas_event',
          'centennial',
          'cmt',
          'dyfi',
          'htd',
          'isc',
          'noaa',
          'other',
          'pde',
          'pdecomcat3',
          'pdeisc',
          'utsu']   

def listEvents(eid):
    db = mysql.connect(host='localhost',db='atlas',user='atlas',passwd="atlas",buffered=True)
    cursor = db.cursor()
    query0 = 'SELECT time,lat,lon,magnitude FROM event WHERE id=%i' % eid
    cursor.execute(query0)
    row = cursor.fetchone()
    etime,elat,elon,emag = row
    print 'Event with ID %i: %s (%.4f,%.4f) M%.1f' % (eid,etime,elat,elon,emag)
    for table in TABLES:
        #print 'Checking table %s' % table
        query = 'SELECT time,lat,lon,magnitude,id FROM %s WHERE eid=%i' % (table,eid)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            ttime,tlat,tlon,tmag,tid = row
            print '\t%20s %6i: %s (%.4f,%.4f) M%.1f' % (table,tid,ttime,tlat,tlon,tmag)
    cursor.close()
    db.close()

def mergeEvents(eid1,eid2):
    db = mysql.connect(host='localhost',db='atlas',user='atlas',passwd="atlas",buffered=True)
    cursor = db.cursor()
    #find all events in TABLES linked to eid2
    for table in TABLES:
        query = 'SELECT id FROM %s WHERE eid=%i' % (table,eid2)
        cursor.execute(query)
        row = cursor.fetchone()
        if row is not None:
            tid = row[0]
            alterquery = 'UPDATE %s SET eid=%i WHERE id=%i' % (table,eid1,tid)
            print alterquery
            cursor.execute(alterquery)
            db.commit()
    #Now delete the event with the second event ID, since nothing is linked to it anymore
    deletequery = 'DELETE FROM event WHERE id=%i' % eid2
    print deletequery
    cursor.execute(deletequery)
    db.commit()
    cursor.close()
    db.close()

def deleteEvent(eid):
    db = mysql.connect(host='localhost',db='atlas',user='atlas',passwd="atlas",buffered=True)
    cursor = db.cursor()
    #remove all references to event from catalog tables
    for table in TABLES:
        query = 'SELECT id FROM %s WHERE eid=%i' % (table,eid)
        cursor.execute(query)
        row = cursor.fetchone()
        if row is not None:
            tid = row[0]
            alterquery = 'UPDATE %s SET eid=NULL WHERE id=%i' % (table,tid)
            print alterquery
            cursor.execute(alterquery)
            db.commit()
    deletequery = 'DELETE FROM event WHERE id=%i' % eid
    print deletequery
    cursor.execute(deletequery)
    db.commit()
    cursor.close()
    db.close()
    
def main(args):
    if args.list:
        eid = args.list
        listEvents(eid)
        sys.exit(0)        
    if args.merge:
        eid1,eid2 = args.merge
        mergeEvents(eid1,eid2)
        print
        listEvents(eid1)
    if args.delete:
        eid = args.delete
        deleteEvent(eid)
            
if __name__ == '__main__':
    desc = '''Perform Atlas DB administrative actions (check, merge).
    '''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-l','--list',metavar='ID',type=int,
                        help='List events from all tables associated with event table ID')
    parser.add_argument('-m','--merge',nargs=2,metavar=['ID1','ID2'],type=int,
                        help='Merge events from all tables associated with ID2 to ID1')
    parser.add_argument('-d','--delete',metavar='ID',type=int,
                        help='Delete event from event table and remove references to it from all tables')
    pargs = parser.parse_args()
    main(pargs)
    
