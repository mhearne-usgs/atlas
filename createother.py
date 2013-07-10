#!/usr/bin/env python

import MySQLdb as mysql
import os.path
import argparse
import sys
from xml.dom.minidom import parse
import re
import glob
import warnings
import datetime
import time
import cStringIO
import traceback
import string
import math

CONFIGFILE = 'smconfig.ini'

def insertEvent(eventdict,connection,cursor):
    fmt = '''INSERT INTO other
    (time,lat,lon,depth,magnitude,eid,ambflag) VALUES
    ("%s",%.4f,%.4f,%.1f,%.1f,NULL,0)'''
    tpl = (eventdict['time'],eventdict['lat'],eventdict['lon'],
           eventdict['depth'],eventdict['mag'])
    query = fmt % tpl
    try:
        self.cursor.execute(query)
        self.connection.commit()
    except:
        return False
    return True

def parseEvent(eventfile):
    try:
        dom = parse(eventfile)
    except Exception,msg:
        print 'Failed to parse %s due to error "%s".  Skipping.' % (eventfile,msg)
        return None
    earthquake = dom.getElementsByTagName('earthquake')[0]
    eventdict = {}
    year = int(earthquake.getAttribute('year'))
    month = int(earthquake.getAttribute('month'))
    day = int(earthquake.getAttribute('day'))
    hour = int(earthquake.getAttribute('hour'))
    minute = int(earthquake.getAttribute('minute'))
    try:
        second = int(earthquake.getAttribute('second'))
    except ValueError,msg:
        second = 0
    eventdict['time'] = datetime.datetime(year,month,day,hour,minute,second)
    eventdict['lat'] = float(earthquake.getAttribute('lat'))
    eventdict['lon'] = float(earthquake.getAttribute('lon'))
    eventdict['depth'] = float(earthquake.getAttribute('depth'))
    eventdict['mag'] = float(earthquake.getAttribute('mag'))
    eventdict['timezone'] = earthquake.getAttribute('timezone')
    eventdict['locstring'] = earthquake.getAttribute('locstring')
    eventdict['type'] = earthquake.getAttribute('type')
    eventdict['eventcode'] = eventdict['time'].strftime('%Y%m%d%H%M%S')
    if earthquake.hasAttribute('created'):
        try:
            ctime = int(earthquake.getAttribute('created'))
        except:
            ctime = time.time()
    else:
        ctime = time.time()
    try:
        eventdict['created'] = datetime.datetime.utcfromtimestamp(ctime)
    except Exception,msg:
        eventdict['created'] = datetime.datetime.utcfromtimestamp(time.time())
    if earthquake.hasAttribute('network'):
        eventdict['network'] = earthquake.getAttribute('network')
    else:
        eventdict['network'] = 'us'
    dom.unlink()
    return eventdict

def connect(configfile):
    # host = igskcicgwsgm046.cr.usgs.gov
    # db = atlas
    # user = atlas
    # password = atlas
    config = ConfigParser.ConfigParser()
    config.readfp(open(configfile,'rt'))
    host = config.get('DATABASE','host')
    db = config.get('DATABASE','db')
    user = config.get('DATABASE','user')
    password = config.get('DATABASE','password')
    connection = mysql.connect(passwd=password,db=db,user=user,host=host)
    cursor = connection.cursor()
    return (connection,cursor)

if __name__ == '__main__':
    usage = 'Create PAGER-Cat other table from folder(s) of reviewed and approved ShakeMap data.'
    parser = argparse.ArgumentParser(description='Create PAGER-Cat other table.',usage=usage)
    parser.add_argument('folders', metavar='FOLDERS', type=int, nargs='+',
                        help='a list of folders containing ShakeMap data')
    parser.add_argument('--config', metavar='CONFIG',dest='configfile', nargs=1,
                        help='override the default config file')

    args = parser.parse_args()

    homedir = os.path.abspath(sys.path[0]) #where is this script?
    if args.configfile is not None:
        configfile = os.path.join(homedir,args.configfile)
    else:
        configfile = os.path.join(homedir,CONFIGFILE)
    connection,cursor = connect(configfile)
        
    #create the other table
    tabledef = {'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','eid':'int',
                   'ambflag':'int','lat':'double','lon':'double','depth':'float',
                   'magnitude':'float','time':'datetime'}

    print 'Creating %s...' % tablename
    query = 'CREATE TABLE %s (' % tablename
    for cname,cvalue in tabledef.iteritems():
        query = query + ' %s %s,' % (cname,cvalue)
        query = query[0:-1] + ')'
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception,msg:
            pass
        
    folders = sys.argv[1:]
    for folder in folders:
        eventfile = os.path.join(folder,'input','event.xml')
        if not os.path.isfile(eventfile):
            continue
        edict = parseEvent(eventfile)
        if edict is None:
            print 'Error parsing %s' % eventfile
            continue
        success = insertEvent(edict,connection,cursor)
        if not success:
            print 'Error inserting %s' % edict['eventcode']
            sys.exit(1)

    cursor.close()
    connection.close()
