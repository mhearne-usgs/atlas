#!/usr/bin/env python

#stdlib
import os.path
import sys
import optparse
import ConfigParser
import datetime
import zipfile
import shutil

#local
from losspager.io import shapefile
from losspager.map.poly import PagerPolygon
from losspager.map import geoserve
from losspager.map import country
from losspager.io import esri

#third party
import MySQLdb as mysql
import numpy

CONFIGFILE = 'smconfig.ini'

DAMAGETABLES = {'pde':['damage',
                       'casualty'],
                'emdat':['fatalities','injured','affected','homeless','totalaffected','loss'],
                'htd':['tsudeaths','tsuinjuries','tsudamage','tsuhouses','eventdeaths',
                       'eventinjuries','eventdamage','eventhouses'],
                'noaa':['deaths','injuries','damage','dedamage','bdestroyed','bdamaged'],
                'other':['shakingDeaths','landslideDeaths','otherDeaths','missing',
                         'undiffDeaths','totalDeaths','injuries','homeless'],
                'utsu':['deaths','injuries','fireflag','damage']}

MAGHIERARCHY = ['other','cmt','pde-Mw','centennial','pde']
LOCHIERARCHY = ['other','centennial','pde','noaa']
PDEMAG = {'magnitude':'magtype','magc1':'magc1type','magc2':'magc2type'}

EVENTXML = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE shakemap-data [
<!ELEMENT shakemap-data (earthquake,stationlist)>
<!ATTLIST earthquake
id            ID      #REQUIRED
lat           CDATA   #REQUIRED
lon           CDATA   #REQUIRED
mag           CDATA   #REQUIRED
year          CDATA   #REQUIRED
month         CDATA   #REQUIRED
day           CDATA   #REQUIRED
hour          CDATA   #REQUIRED
minute        CDATA   #REQUIRED
second        CDATA   #REQUIRED
timezone      CDATA   #REQUIRED
depth         CDATA   #REQUIRED
type          CDATA   #REQUIRED
locstring     CDATA   #REQUIRED
pga           CDATA   #REQUIRED
pgv           CDATA   #REQUIRED
sp03          CDATA   #REQUIRED
sp10          CDATA   #REQUIRED
sp30          CDATA   #REQUIRED
created       CDATA   #REQUIRED
>
]>
<shakemap-data code_version="3.2.1 GSM" map_version="4">
<earthquake id="[ID]" lat="[LAT]" lon="[LON]" mag="[MAG]" year="[YEAR]" month="[MONTH]" day="[DAY]" hour="[HOUR]" minute="[MINUTE]" timezone="GMT" depth="[DEPTH]" locstring="[LOCATION]" />
</shakemap-data>
'''

def createShakeInput(eventlist,options):
    folders = []
    zipname = 'events'
    startTime = datetime.datetime(1900,1,1)
    endTime = datetime.datetime.utcnow()
    if options.startTime is not None:
        startTime = datetime.datetime.strptime(options.startTime,'%Y-%m-%d')
    if options.endTime is not None:
        endTime = datetime.datetime.strptime(options.endTime,'%Y-%m-%d')
    if options.countryCode is not None:
        zipname += '_'+options.countryCode.lower()
    if options.withDamage:
        zipname += '_'+'damage'
    zipname += '_'+startTime.strftime('%Y%m%d')
    zipname += '_'+endTime.strftime('%Y%m%d')
    zipname += '.zip'
    shakezip = zipfile.ZipFile(zipname,'w',zipfile.ZIP_DEFLATED)
    for event in eventlist:
        eventid = event['time'].strftime('%Y%m%d%H%M%S')
        folder = os.path.join(os.getcwd(),eventid)
        if not os.path.isdir(folder):
            os.makedirs(folder)
        inputfolder = os.path.join(folder,'input')
        if not os.path.isdir(inputfolder):
            os.mkdir(inputfolder)
        eventxml = EVENTXML
        eventxml = eventxml.replace('[ID]',eventid)
        eventxml = eventxml.replace('[LAT]','%.4f' % event['lat'])
        eventxml = eventxml.replace('[LON]','%.4f' % event['lon'])
        eventxml = eventxml.replace('[MAG]','%.1f' % event['magnitude'])
        eventxml = eventxml.replace('[YEAR]','%s' % str(event['time'].year))
        eventxml = eventxml.replace('[MONTH]','%s' % str(event['time'].month))
        eventxml = eventxml.replace('[DAY]','%s' % str(event['time'].day))
        eventxml = eventxml.replace('[HOUR]','%s' % str(event['time'].hour))
        eventxml = eventxml.replace('[MINUTE]','%s' % str(event['time'].minute))
        eventxml = eventxml.replace('[SECOND]','%s' % str(event['time'].second))
        eventxml = eventxml.replace('[DEPTH]','%.1f' % event['depth'])
        eventxml = eventxml.replace('[LOCATION]','%s' % event['loc'])
        eventfile = os.path.join(inputfolder,'event.xml')
        f = open(eventfile,'wt')
        f.write(eventxml)
        f.close()
        shakezip.write(eventfile,os.path.join(eventid,'input','event.xml'))
        folders.append(folder)

    for folder in folders:
        shutil.rmtree(folder)

    shakezip.close()
    return shakezip
    
    

def getLocation(cursor,eid):
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

def getMagnitude(cursor,eid):
    foundMagnitude = False
    idx = 0
    magnitude = None
    for table in MAGHIERARCHY:
        if table.find('Mw') > -1: #we're looking at PDE
            parts = table.split('-')
            table = parts[0]
            magtype = parts[1]
            query = 'SELECT magnitude FROM pde WHERE (magtype = "Mw" or magc1type = "Mw" or magc2type = "Mw") and eid=%i' % eid
            cursor.execute(query)
            lrow = cursor.fetchone()
            if lrow is None:
                continue
            else:
                magnitude = lrow[0]
                break
        else:
            query = 'SELECT magnitude from %s WHERE eid = %i' % (table,eid)
            cursor.execute(query)
            lrow = cursor.fetchone()
            if lrow is None:
                continue
            else:
                magnitude = lrow[0]
                break

    return magnitude
    
def getEvents(cursor,options,config):
    gs = geoserve.GeoServe()
                             
    if options.countryCode is not None:
        shpfile = config.get('FILES','country')
        shape = shapefile.PagerShapeFile(shpfile)
        countryshape = shape.getShapesByAttr('ISO2',options.countryCode)[0]
        pp = PagerPolygon(countryshape['x'],countryshape['y'])
        cgrid = esri.EsriGrid(config.get('FILES','isogrid'))
        numccode = country.getCountryCode(options.countryCode)['number']
    
    #add time to this query later
    startTime = datetime.datetime(1900,1,1)
    endTime = datetime.datetime.utcnow()
    if options.startTime is not None:
        startTime = datetime.datetime.strptime(options.startTime,'%Y-%m-%d')
    if options.endTime is not None:
        endTime = datetime.datetime.strptime(options.endTime,'%Y-%m-%d')
    query = 'SELECT id,lat,lon,depth,time,magnitude,ccode FROM event WHERE time >= "%s" and time <= "%s"' % (startTime,endTime)
    cursor.execute(query)
    rows = cursor.fetchall()
    eventlist = []
    idx = 0
    for row in rows:
        if (idx % 100) == 0:
            print 'Checking event %i of %i' % (idx,len(rows))
        eid = row[0]
        if options.withDamage:
            foundDamage = False
            for table,trows in DAMAGETABLES.iteritems():
                nuggets = []
                query = 'SELECT count(*) FROM %s WHERE eid=%i AND (' % (table,eid)
                for trow in trows:
                    nuggets.append('(%s IS NOT NULL AND %s > 0)' % (trow,trow))
                query += ' OR '.join(nuggets) + ')'
                cursor.execute(query)
                foundDamage = cursor.fetchone()[0]
                if foundDamage:
                    break
            if not foundDamage:
                idx += 1
                continue

        lat,lon,depth,time = getLocation(cursor,eid)
        if lat is None:
            print 'No location data found in any of %s.  Using event table.' % (str(LOCHIERARCHY))
            lat = row[1]
            lon = row[2]
            depth = row[3]
            time = row[4]

        location = gs.getRegionName(lat,lon)
            
        magnitude = getMagnitude(cursor,eid)
        if magnitude is None:
            magnitude = row[5]

        if options.countryCode is not None:
            if pp.boundingBoxContainsPoint(lon,lat):
                cgrid.load((lon-0.5,lon+0.5,lat-0.5,lat+0.5))
                isocode = int(cgrid.getValue(lat,lon))
                if isocode == numccode:
                    print 'Found event %s (%.4f,%.4f) in %s bounding box' % (time,lat,lon,options.countryCode)
                    eventlist.append({'lat':lat,'lon':lon,'depth':depth,'time':time,'magnitude':magnitude,'loc':location})
        else:
            eventlist.append({'lat':lat,'lon':lon,'depth':depth,'time':time,'magnitude':magnitude,'loc':location})
        idx += 1

    return eventlist

def main(options,config):
    config.readfp(open(configfile))
    sections = config.sections()

    if 'DATABASE' not in sections:
        print 'Missing section "DATABASE" in config file.'
        parser.print_help()
        sys.exit(1)

    reqfields = ['host','db','user','password']
    params = config.options('DATABASE')
    #does the list of options include every required field above?
    if not set(reqfields) <= set(params):
        print 'Missing one or more of the following options in config file: %s' % str(reqfields)
        parser.print_help()
        sys.exit(1)

    host = config.get('DATABASE','host')
    db = config.get('DATABASE','db')
    user = config.get('DATABASE','user')
    password = config.get('DATABASE','password')
    connection = mysql.connect(host=host,user=user,db=db,passwd=password)
    cursor = connection.cursor()

    eventlist = getEvents(cursor,options,config)
    shakezip = createShakeInput(eventlist,options)
    cursor.close()
    connection.close()

def generateConfig(configfile):
    f = open(configfile,'wt')
    config = ConfigParser.RawConfigParser()
    sections = {'DATABASE':['host','db','user','password'],
                'FILES':['country']}
    for section,fields in sections.iteritems():
        config.add_section(section)
        for field in fields:
            resp = ''
            while not len(resp.strip()):
                resp = raw_input('Enter a valid value for option "%s" under section "%s": ')
            config.set(section,field,resp.strip())
            
    config.write(f)
    f.close()
    
if __name__ == '__main__':
    usage = usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-c", "--country", dest="countryCode",
                      help="Extract events from within country COUNTRY", metavar="COUNTRY")
    parser.add_option("-s", "--starttime", dest="startTime",
                      help="Extract events from STARTTIME until present ", metavar="STARTTIME")
    parser.add_option("-e", "--endtime", dest="endTime",
                      help="Extract events from ENDTIME until present ", metavar="ENDTIME")
    parser.add_option("-d", "--damage",
                  action="store_true", dest="withDamage", default=False,
                  help="Only extract events with some kind of damage")
    parser.add_option("-g", "--genconfig",
                  action="store_true", dest="genConfig", default=False,
                  help="Generate the config file")
    
    (options, args) = parser.parse_args()

    homedir = os.path.abspath(sys.path[0]) #where is this script?
    configfile = os.path.join(homedir,CONFIGFILE)

    if options.genConfig:
        generateConfig(configfile)
        sys.exit(0)
    
    if not os.path.isfile(configfile):
        print 'Missing required config file %s' % configfile
        print 'Run with -g option to generate it.'
        parser.print_help()
        sys.exit(1)
    
    config = ConfigParser.ConfigParser()
    main(options,config)    
    
    
    
