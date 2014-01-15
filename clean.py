#!/usr/bin/env python

#stdlib imports
import os.path
import argparse
import shutil
import sys

#third party
import MySQLdb as mysql

SHAKEDIR = '/home/shake/ShakeMap/'


def main(argparser,args):
    if not args.event and not args.all and not args.nuke:
        argparser.print_help()
        sys.exit(0)
    if (args.event and args.all):
        print 'You must choose to delete a list of events OR all events.'
        argparser.print_help()
        sys.exit(0)

    if args.all:
        print 'You have elected to delete all events from the database and the file system.'
        resp = raw_input('Are you sure you want to do this? Y/[n] ')
        if resp != 'Y':
            print 'Whew - we dodged the bullet.'
            sys.exit(0)
        
    datadir = os.path.join(SHAKEDIR,'data')
    pwfile = os.path.join(SHAKEDIR,'pw','passwords')
    if not os.path.isfile(pwfile):
        print 'Cannot find password file %s.' % pwfile
        sys.exit(0)
    database,user,password = parsePasswords(pwfile)
    if database is None:
        print 'Cannot find a shakemap database specified in %s.' % pwfile
        sys.exit(0)
    db = mysql.connect(passwd=password,db=database,user=user,host='127.0.0.1')
    cursor = db.cursor()
    if args.all:
        eventlist = getEventList(datadir)
    else:
        eventlist = args.event

    for event in eventlist:
        fsres = deleteFromFileSystem(datadir,event,nuke=args.nuke)
        dbres = deleteFromDatabase(db,cursor,event)
        print 'Event %s deleted from file system and database' % event
    
    cursor.close()
    db.close()

def deleteFromFileSystem(datadir,event,nuke=False):
    eventfolder = os.path.join(datadir,event)
    if not os.path.isdir(eventfolder):
        return False
    if nuke:
        shutil.rmtree(eventfolder)
        return True
    outputs = ['genex','output','richter','zoneconfig']
    for ofolder in outputs:
        outfolder = os.path.join(eventfolder,ofolder)
        if os.path.isdir(outfolder):
            shutil.rmtree(outfolder)
    return True

def deleteFromDatabase(db,cursor,event):
    tables = ['earthquake','shake_runs','shake_version']
    for table in tables:
        query = 'DELETE FROM %s WHERE evid="%s"' % (table,event)
        cursor.execute(query)
        db.commit()
    return True
    
def getEventList(datadir,cursor):
    eventlist = []
    folders = os.listdir(datadir)
    for tfol in folders:
        folder = os.path.join(datadir,tfol)
        inputfolder = os.path.join(folder,'input')
        if not os.path.isdir(inputfolder):
            continue
        eventlist.append(tfol)
    query = 'SELECT evid FROM earthquake'
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        if row[0] not in eventlist:
            eventlist.append(row[0])
    return eventlist

    
def parsePasswords(pwfile):
    database = None
    user = None
    password = None
    lines = open(pwfile,'rt').readlines()
    for line in lines:
        parts = line.split()
        tmp,database = parts[0].split('=')
        if database != 'shakemap':
            continue
        user = parts[1]
        password = parts[2]
        break
    return (database,user,password)
    
if __name__ == '__main__':
    desc = 'Delete event data from file system and database.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-event',help='Delete data for event',metavar='EVENT',nargs='+')
    parser.add_argument('-all',action='store_true',default=False,
                        help='Delete input data for ALL events')
    parser.add_argument('-nuke',action='store_true',default=False,
                        help='Delete ALL data (input and output) for ALL events')
    arguments = parser.parse_args()

    main(parser,arguments)
    
