#!/usr/bin/env python

import MySQLdb as mysql
import os.path
from optparse import OptionParser
import sys
import datetime
import time
from atlas2db import getDataBaseConnections

DEFAULT_RUN = """
/opt/local/ShakeMap/bin/../bin/grind -event EVENTCODE -qtm -xml -lonspan 4.0 -psa 
/opt/local/ShakeMap/bin/../bin/mapping -event EVENTCODE -timestamp -itopo -gsm -pgminten
/opt/local/ShakeMap/bin/../bin/plotregr -event EVENTCODE -lab_dev 6 -psa
/opt/local/ShakeMap/bin/../bin/genex -event EVENTCODE -zip -metadata -shape shape -shape hazus
"""

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
                dist = jdict['cities']['distance']
                direc = jdict['cities']['direction']
                cname = jdict['cities']['name']
                locstr = '%i km %s of %s' % (dist,direc,cname)
            else:
                try:
                    locstr = jdict['fe']['longName']
                except:
                    try:
                        dist = jdict['cities'][0]['distance']
                        direc = jdict['cities'][0]['direction']
                        cname = jdict['cities']['name']
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

    def writeEvents(self,atlasdir,options):
        query = 'SELECT id,code,lat,lon,depth,magnitude,time FROM event order by time'
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            eid = row[0]
            eventcode = row[1]
            print 'Writing event data for %s' % eventcode
            query2 = 'SELECT id,eventcode,lat,lon,depth,magnitude,time,timezone,locstring,created,type,network,inserttime FROM atlas_event WHERE eid=%i' % eid
            nrows = self.cursor.execute(query2)
            if not nrows:
                row = list(row)
                row.append('GMT')
                row.append(self.getLocation(row[2],row[3]))
                row.append(datetime.datetime.now())
                row.append('')
                row.append('us')
                row.append(time.time())
                inputfolder = os.path.join(atlasdir,eventcode,'input')
                if not os.path.isdir(inputfolder):
                    os.makedirs(inputfolder)
                self.writeEventFile(inputfolder,row)
                runfile = os.path.join(os.path.join(atlasdir,eventcode,'RUN_%s' % eventcode))
                f = open(runfile,'wt')
                f.write(DEFAULT_RUN.strip().replace('EVENTCODE',eventcode))
                f.close()
                statusfile = os.path.join(atlasdir,eventcode,'status.txt')
                f = open(statusfile,'wt')
                f.write('Status: Automatic\n')
                f.close()
                continue
            for row in self.cursor.fetchall():
                eventid = row[0]
                eventcode = row[1]
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
                self.writeEventFile(inputfolder,row)
                self.writeStatus(atlasdir,eventid)
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
        runfile = row[0]
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
                f.write('%s = %s\n' % (param,value))
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

    def writeEventFile(self,inputfolder,row):
        eventfile = os.path.join(inputfolder,'event.xml')
        year = row[6].year
        month = row[6].month
        day = row[6].day
        hour = row[6].hour
        minute = row[6].minute
        second = row[6].second
        created = time.mktime(row[9].timetuple())
        try:
            otime = time.mktime(row[6].timetuple())
        except:
            dt = datetime.datetime(1970,1,1,0,0,0) - row[6]
            otime = -1*(dt.days*86400 + dt.seconds)
        f = open(eventfile,'wt')
        f.write('<?xml version="1.0" encoding="US-ASCII" standalone="yes"?>\n')
        fmt = '''<earthquake id="%s" lat="%.4f" lon="%.4f" mag="%.1f"
        year="%4i" month="%2i" day="%2i" hour="%2i" minute="%2i" second="%2i" timezone="%s"
        depth="%.1f" locstring="%s" created="%i" otime="%i" type="%s" network="%s"/>\n'''
        tpl = (row[1],row[2],row[3],row[5],year,month,day,hour,minute,second,row[7],row[4],row[8],created,otime,row[10],row[11])
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
    parser.add_option("-r", "--norun",
                      action="store_true", dest="noRun", default=False,
                      help="Do not write run files")
    parser.add_option("-s", "--nosource",
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
    
    atlasdir = args[0]
    sucker.writeEvents(atlasdir,options)
    sucker.close()
