#!/usr/bin/env python

#stdlib imports
import os.path
from optparse import OptionParser
import sys
import datetime
import time
import urllib2
import json
import string

#third party
import MySQLdb as mysql

#local
from atlas2db import getDataBaseConnections


DEFAULT_RUN = """
/opt/local/ShakeMap/bin/grind -event EVENTCODE -qtm -xml -lonspan 4.0 -psa 
/opt/local/ShakeMap/bin/mapping -event EVENTCODE -timestamp -itopo -gsm -pgminten
/opt/local/ShakeMap/bin/plotregr -event EVENTCODE -lab_dev 6 -psa
/opt/local/ShakeMap/bin/genex -event EVENTCODE -zip -metadata -shape shape -shape hazus
"""

MAGHIERARCHY = ['atlas_event','other','cmt','pde-Mw','pdeisc-Mw','centennial','pde','pdeisc']
LOCHIERARCHY = ['atlas_event','other','centennial','pde','pdeisc','noaa']

class DataBaseSucker(object):
    connection = None
    cursor = None

    def getLocation(self,lat,lon):
        MAX_DIST = 300
        urlt = 'http://igskcicgvmbkora.cr.usgs.gov:8080/gs_dad/get_gs_info?latitude=%.4f&longitude=%.4f&utc=%s'
        tstamp = datetime.datetime.utcnow().strftime('%m/%d/%Y:%H:%M:%S')
        url = urlt % (lat,lon,tstamp)
        locstr = '%.4f,%.4f' % (lat,lon)
        try:
            fh = urllib2.urlopen(url)
            data = fh.read()
            fh.close()
            jdict = json.loads(data)
            if jdict['cities'][0]['distance'] <= MAX_DIST:
                dist = jdict['cities'][0]['distance']
                direc = jdict['cities'][0]['direction']
                cname = jdict['cities'][0]['name']
                locstr = '%i km %s of %s' % (dist,direc,cname)
            else:
                try:
                    locstr = jdict['fe']['longName']
                except:
                    try:
                        dist = jdict['cities'][0]['distance']
                        direc = jdict['cities'][0]['direction']
                        cname = jdict['cities'][0]['name']
                        locstr = sprintf('%i km %s of %s',dist,direc,cname)
                    except:
                        pass
        except:
            pass
        return locstr

    def __init__(self,dbdict):
        atlas = dbdict['atlas']
        shakemap = dbdict['shakemap']
        self.connection = mysql.connect(db=atlas['database'],user=atlas['user'],passwd=atlas['password'],host='127.0.0.1')
        self.cursor = self.connection.cursor()

    def listEvents(self):
        query = 'SELECT time from event'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            print row[0].strftime('%Y%m%d%H%M%S')

    def getHypocenter(self,eid):
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
                self.cursor.execute(query)
            except:
                pass
            lrow = self.cursor.fetchone()
            if lrow is None:
                continue
            else:
                lat = lrow[0]
                lon = lrow[1]
                depth = lrow[2]
                time = lrow[3]
                break

        return (lat,lon,depth,time)

            
    def getMagnitude(self,eid):
        foundMagnitude = False
        idx = 0
        magnitude = None
        for table in MAGHIERARCHY:
            if table.find('Mw') > -1: #we're looking at PDE
                parts = table.split('-')
                table = parts[0]
                magtype = parts[1]
                query = 'SELECT magnitude FROM %s WHERE (magtype = "Mw" or magc1type = "Mw" or magc2type = "Mw") and eid=%i' % (table,eid)
                self.cursor.execute(query)
                lrow = self.cursor.fetchone()
                if lrow is None:
                    continue
                else:
                    magnitude = lrow[0]
                    break
            else:
                query = 'SELECT magnitude from %s WHERE eid = %i' % (table,eid)
                self.cursor.execute(query)
                lrow = self.cursor.fetchone()
                if lrow is None:
                    continue
                else:
                    magnitude = lrow[0]
                    break

        return magnitude
            
    def writeEvents(self,atlasdir,options,startDate,endDate):
        #query = 'SELECT id FROM event order by time'
        query = 'SELECT id,code,lat,lon,depth,magnitude,time FROM event WHERE time > "%s" AND time < "%s" order by time' % (startDate,endDate)
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            eventdict = {}
            eid = row[0]
            lat,lon,depth,time = self.getHypocenter(eid)
            if lat is None:
                lat = row[2]
            if lon is None:
                lon = row[3]
            if depth is None:
                depth = row[4]
            magnitude = self.getMagnitude(eid)
            try:
                eventcode = time.strftime('%Y%m%d%H%M%S')
            except:
                pass
            
            eventdict['lat'] = lat
            eventdict['lon'] = lon
            eventdict['depth'] = depth
            eventdict['time'] = time
            eventdict['mag'] = magnitude
            eventdict['eventcode'] = eventcode

            print 'Writing event data for %s' % eventcode
            query2 = 'SELECT id,timezone,locstring,created,type,network FROM atlas_event WHERE eid=%i' % eid
            nrows = self.cursor.execute(query2)
            #this section handles events that are NOT in the atlas_event table
            if not nrows:
                eventdict['timezone'] = 'GMT'
                eventdict['locstring'] = self.getLocation(lat,lon)
                eventdict['created'] = datetime.datetime.now()
                eventdict['type'] = ''
                eventdict['network'] = 'us'
                inputfolder = os.path.join(atlasdir,eventcode,'input')
                if not os.path.isdir(inputfolder):
                    os.makedirs(inputfolder)
                self.writeEventFile(inputfolder,eventdict)
                runfile = os.path.join(os.path.join(atlasdir,eventcode,'RUN_%s' % eventcode))
                f = open(runfile,'wt')
                f.write(DEFAULT_RUN.strip().replace('EVENTCODE',eventcode))
                f.close()
                statusfile = os.path.join(atlasdir,eventcode,'status.txt')
                f = open(statusfile,'wt')
                f.write('Status: Automatic\n')
                f.close()
                continue
            #this section handles events that ARE in the atlas_event table
            for row in self.cursor.fetchall():
                eventid = row[0]
                eventdict['timezone'] = row[1]
                #filter out non-ascii characters in the location string
                locstring = filter(lambda x: x in string.printable, row[2])
                eventdict['locstring'] = locstring
                eventdict['created'] = row[3]
                eventdict['type'] = row[4]
                eventdict['network'] = row[5]
                inputfolder = os.path.join(atlasdir,eventcode,'input')
                configfolder = os.path.join(atlasdir,eventcode,'config')
                try:
                    if not os.path.isdir(inputfolder):
                        os.makedirs(inputfolder)
                    if not os.path.isdir(configfolder):
                        os.makedirs(configfolder)
                except:
                    print 'Unable to create input or config folder %s.  Stopping.' % (inputfolder,configfolder)
                    self.close()
                    sys.exit(0)
                self.writeEventFile(inputfolder,eventdict)
                self.writeStatus(os.path.join(atlasdir,eventcode),eventid)
                if not options.noData:
                    self.writeStationList(eventid,inputfolder)
                if not options.noFault:
                    self.writeFaultFile(eventid,inputfolder)
                if not options.noSource:
                    self.writeSource(eventid,inputfolder)
                if not options.noConfig:
                    self.writeConfig(eventid,configfolder)
                if not options.noRun:
                    self.writeRun(eventid,os.path.join(atlasdir,eventcode))
                
    def writeStatus(self,atlasdir,eventid):
        statusfile = os.path.join(atlasdir,'status.txt')
        query = 'SELECT statuskey,statusvalue FROM atlas_status WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if not len(rows):
            return
        f = open(statusfile,'wt')
        for row in rows:
            f.write('%s: %s\n'% (row[0],row[1]))
        f.close()
        
    def writeRun(self,eventid,eventfolder):
        query = 'SELECT filename,content FROM atlas_run_file WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        runfile = os.path.join(eventfolder,row[0])
        runcontent = row[1]
        f = open(runfile,'wt')
        f.write(runcontent)
        f.close()
        

    def writeConfig(self,eventid,configfolder):
        query = 'SELECT id,filename FROM atlas_config WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            configid = row[0]
            filename = row[1]
            configfile = os.path.join(configfolder,filename)
            f = open(configfile,'wt')
            query = 'SELECT configparam,configvalue FROM atlas_config_param WHERE config_id=%i' % configid
            self.cursor.execute(query)
            for trow in self.cursor.fetchall():
                param = trow[0]
                value = trow[1]
                f.write('%s : %s\n' % (param,value))
            f.close()

    def writeSource(self,eventid,inputfolder):
        query = 'SELECT sourcekey,sourcevalue FROM atlas_source WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        filename = os.path.join(inputfolder,'source.txt')
        f = open(filename,'wt')
        for row in rows:
            key = row[0]
            value = row[1]
            f.write('%s = %s\n' % (key,value))
        f.close()
            

    def writeFaultFile(self,eventid,inputfolder):
        query = 'SELECT id,filename,firstline FROM atlas_fault_file WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row is None:
            return
        fileid = row[0]
        filename = row[1]
        firstline = row[2]
        faultfile = os.path.join(inputfolder,filename)
        f = open(faultfile,'wt')
        try:
            f.write('#%s\n' % firstline)
        except Exception,msg:
            pass
        query = 'SELECT seqno,lat,lon,depth FROM atlas_fault WHERE faultfile_id=%i' % fileid
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        rows = sorted(rows,key=lambda row:row[0]) #sort by seqno
        for row in rows:
            if row[3] is None:
                depth = 0.0
            else:
                depth = row[3]
            try:
                f.write('%.4f %.4f %.4f\n' % (row[1],row[2],depth))
            except:
                pass
        f.close()

    def writeEventFile(self,inputfolder,eventdict):
        eventfile = os.path.join(inputfolder,'event.xml')
        year = eventdict['time'].year
        month = eventdict['time'].month
        day = eventdict['time'].day
        hour = eventdict['time'].hour
        minute = eventdict['time'].minute
        second = eventdict['time'].second
        created = time.mktime(eventdict['created'].timetuple())
        try:
            otime = time.mktime(eventdict['time'].timetuple())
        except:
            dt = datetime.datetime(1970,1,1,0,0,0) - eventdict['time']
            otime = -1*(dt.days*86400 + dt.seconds)
        f = open(eventfile,'wt')
        f.write('<?xml version="1.0" encoding="US-ASCII" standalone="yes"?>\n')
        fmt = '''<earthquake id="%s" lat="%.4f" lon="%.4f" mag="%.1f" year="%4i" month="%02i" day="%02i" hour="%02i" minute="%02i" second="%02i" timezone="%s" depth="%.1f" locstring="%s" created="%i" otime="%i" type="%s" network="%s"/>\n'''
        ecode = eventdict['eventcode']
        lat = eventdict['lat']
        lon = eventdict['lon']
        mag = eventdict['mag']
        tzone = eventdict['timezone']
        depth = eventdict['depth']
        etype = eventdict['type']
        net = eventdict['network']
        locstring = eventdict['locstring']
        tpl = (ecode,lat,lon,mag,year,month,day,hour,minute,second,tzone,depth,locstring,created,otime,etype,net)
        f.write(fmt % tpl)
        f.close()

    def writeStationList(self,eventid,inputfolder):
        query = 'SELECT id,filename,created FROM atlas_station_file WHERE event_id=%i' % eventid
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            fileid = row[0]
            fname = row[1]
            created = time.mktime(row[2].timetuple())
            stationfile = os.path.join(inputfolder,fname)
            f = open(stationfile,'wt')
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
            f.write('<stationlist created="%i">\n' % created)
            fmt = 'SELECT id,code,name,insttype,lat,lon,source,netid,commtype,intensity FROM atlas_station WHERE stationfile_id=%i'
            self.cursor.execute(fmt % fileid)
            for trow in self.cursor.fetchall():
                stationid = trow[0]
                code = trow[1]
                name = trow[2]
                insttype = trow[3]
                lat = trow[4]
                lon = trow[5]
                source = trow[6]
                netid = trow[7]
                commtype = trow[8]
                intensity = trow[9]
                fmt = '<station code="%s" name="%s" insttype="%s" lat="%.3f" lon="%.3f" source="%s" netid="%s" commtype="%s" intensity="%.1f">\n'
                f.write(fmt % (code,name,insttype,lat,lon,source,netid,commtype,intensity))
                query = 'SELECT id,name FROM atlas_component WHERE station_id=%i' % stationid
                self.cursor.execute(query)
                for srow in self.cursor.fetchall():
                    compid = srow[0]
                    cname = srow[1]
                    f.write('<comp name="%s">\n' % name)
                    query = 'SELECT componentkey,componentvalue FROM atlas_component_param WHERE component_id=%i' % compid
                    self.cursor.execute(query)
                    for urow in self.cursor.fetchall():
                        key = urow[0]
                        value = urow[1]
                        if value is None:
                            value = float('nan')
                        try:
                            f.write('<%s value="%.4f"/>\n' % (key,value))
                        except:
                            pass
                    f.write('</comp>\n')
                f.write('</station>\n')
            f.write('</stationlist>\n')
            f.close()
            
            
            
    def close(self):
        self.cursor.close()
        self.connection.close()
        

if __name__ == '__main__':
    usage = "usage: %prog [options] atlasdir [eventcode1 eventcode2]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--noconfig",
                      action="store_true", dest="noConfig", default=False,
                      help="Do not write config files")
    parser.add_option("-d", "--nodata",
                      action="store_true", dest="noData", default=False,
                      help="Do not write station data (strong motion, observed intensity, etc.) files")
    parser.add_option("-f", "--nofault",
                      action="store_true", dest="noFault", default=False,
                      help="Do not write fault files")
    parser.add_option("-s", "--start-date", dest="startDate",
                      help="Choose a start date for processing (YYYY-MM-DD)", metavar="STARTDATE")
    parser.add_option("-e", "--end-date", dest="endDate",
                      help="Choose an end date for processing (YYYY-MM-DD)", metavar="ENDDATE")
    parser.add_option("-r", "--norun",
                      action="store_true", dest="noRun", default=False,
                      help="Do not write run files")
    parser.add_option("-n", "--nosource",
                      action="store_true", dest="noSource", default=False,
                      help="Do not write source.txt file")
    parser.add_option("-a", "--shakehome", dest="shakehome",
                      help="Inform the program about the root directory for the ShakeMap installation", metavar="SHAKEHOME")
    parser.add_option("-l", "--listevents",
                      action="store_true", dest="listEvents", default=False,
                      help="Print a list of event ids contained in the database")
    (options, args) = parser.parse_args()

    if options.shakehome is None:
        print 'Must specify the --shakehome parameter.'
        parser.print_usage()
        sys.exit(0)

    events = []
    if len(args) > 1:
        events = args[1:]
        
    dbdict = getDataBaseConnections(options.shakehome)
    sucker = DataBaseSucker(dbdict)

    if options.listEvents:
        sucker.listEvents()
        sys.exit(0)

    startDate = datetime.datetime(1900,1,1)
    if options.startDate is not None:
        startDate = datetime.datetime.strptime(options.startDate,'%Y-%m-%d')

    endDate = datetime.datetime(3000,1,1)
    if options.endDate is not None:
        endDate = datetime.datetime.strptime(options.endDate,'%Y-%m-%d')
        
    atlasdir = args[0]
    sucker.writeEvents(atlasdir,options,startDate,endDate)
    sucker.close()
