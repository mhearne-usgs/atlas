#!/usr/bin/env python

import MySQLdb as mysql
import os.path
from optparse import OptionParser
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

#class
class DataBasePusher(object):
    connection = None
    cursor = None
    shake_connection = None
    shake_cursor = None
    def __init__(self,dbdict):
        atlas = dbdict['atlas']
        shakemap = dbdict['shakemap']
        self.connection = mysql.connect(db=atlas['database'],user=atlas['user'],passwd=atlas['password'])
        self.cursor = self.connection.cursor()
        # self.shake_connection = mysql.connect(db=shakemap['database'],user=shakemap['user'],passwd=shakemap['password'])
        # self.shake_cursor = self.shake_connection.cursor()

    def createDB(self):
        warnings.filterwarnings("ignore", "Unknown table *")
        tables = {
            'atlas_event':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','eventcode':'varchar(30)',
                'eid':'int','ambflag':'int','lat':'double','lon':'double','depth':'float',
                'magnitude':'float','time':'datetime','timezone':'varchar(10)',
                'locstring':'varchar(128)','created':'datetime','type':'varchar(64)',
                'network':'varchar(10)','inserttime':'datetime'},
            'atlas_status':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY',
                'event_id':'int',
                'statuskey':'varchar(64)',
                'statusvalue':'varchar(128)'},
            'atlas_station_file':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int','filename':'varchar(64)',
                'created':'datetime'},
            'atlas_station':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','stationfile_id':'int','code':'varchar(30)',
                'name':'varchar(128)','insttype':'varchar(30)','lat':'double','lon':'double',
                'source':'varchar(128)','netid':'varchar(30)','commtype':'varchar(30)','intensity':'float'},
            'atlas_component':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','station_id':'int','name':'varchar(128)'},
            'atlas_component_param':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','component_id':'int',
                'componentkey':'varchar(30)','componentvalue':'float',
                'flag':'varchar(16)'},
            'atlas_fault_file':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int',
                'filename':'varchar(64)','firstline':'varchar(512)'},
            'atlas_fault':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','faultfile_id':'int','seqno':'int',
                'segment':'int','lat':'double','lon':'double','depth':'float'},
            'atlas_config':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int','filename':'varchar(64)'},
            'atlas_config_param':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','config_id':'int',
                'configparam':'varchar(30)','configvalue':'varchar(128)'},
            'atlas_prog_flags':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int',
                'program':'varchar(30)','flags':'varchar(128)'},
            'atlas_config_known':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','program':'varchar(30)',
                'version':'varchar(10)','revision':'varchar(10)',
                'attributes':'varchar(1024)'},
            'atlas_source':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int',
                'sourcekey':'varchar(30)','sourcevalue':'varchar(50)'},
            'atlas_run_file':{
                'id':'INT NOT NULL AUTO_INCREMENT PRIMARY KEY','event_id':'int',
                'filename':'varchar(64)','content':'varchar(512)'}
                }

        #drop any of the tables in this database that may already exist and contain data
        for table in tables.keys():
            print 'Dropping %s...' % table
            query = 'DROP TABLE IF EXISTS %s' % table
            self.cursor.execute(query)
            self.connection.commit()

        for tablename,tabledef in tables.iteritems():
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

    def deleteChildren(self,topid,children):
        """
        Recursively find and delete child rows from tables.
        """
        child = children[0]
        table = child[0]
        idfield = child[1]
        #we're still somewhere in the middle of the hierarchy
        if len(children) > 1:
            query = 'SELECT id FROM %s WHERE %s=%i' % (table,idfield,topid)
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                newtopid = row[0]
                self.deleteChildren(newtopid,children[1:])

        if table == 'atlas_station_file':
            pass
        query = 'DELETE FROM %s WHERE %s=%i' % (table,idfield,topid)
        self.cursor.execute(query)
        self.connection.commit()

    def deleteEvent(self,eventcode):
        print 'Deleting event %s' % eventcode
        query = 'SELECT id FROM atlas_event WHERE eventcode="%s"' % eventcode
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row is not None:
            eventid = row[0]
        else:
            return

        stations = [('atlas_station_file','event_id'),
                    ('atlas_station','stationfile_id'),
                    ('atlas_component','station_id'),
                    ('atlas_component_param','component_id')]
        self.deleteChildren(eventid,stations)

        configs = [('atlas_config','event_id'),
                    ('atlas_config_param','config_id')]
        self.deleteChildren(eventid,configs)
        
        faults = [('atlas_fault_file','event_id'),
                    ('atlas_fault','faultfile_id')]
        self.deleteChildren(eventid,faults)

        sources = [('atlas_source','event_id')]
        self.deleteChildren(eventid,sources)

        flags = [('atlas_prog_flags','event_id')]
        self.deleteChildren(eventid,flags)

        query = 'DELETE FROM atlas_event WHERE id=%i' % eventid
        self.cursor.execute(query)
        self.connection.commit()
        

    def parseStatus(self,statusfile):
        status = {}
        if not os.path.isfile(statusfile):
            status = {'status':'Automatic','reviewer':'unknown'}
            return status
            
        f = open(statusfile,'rt')
        for line in f.readlines():
            parts = line.split(':')
            key = parts[0].strip().lower()
            value = parts[1].strip().lower()
            status[key] = value
        f.close()
        return status
        
    def pushEvent(self,eventfile,configfiles,stationfiles,sourcefile,
                  faultfile,runfile,statusfile,shakehome,doUpdate,version,revision,
                  dbdict):
        try:
            eventdict = self.parseEventFile(eventfile)
            if eventdict is None:
                return
            if eventdict['locstring'].lower().find('scenario') > -1:
                print 'Event %s appears to be a scenario.  Skipping.' % eventdict['eventcode']
                return
            if doUpdate:
                self.deleteEvent(eventdict['eventcode'])
            #read in the event.xml file and push to DB
            eventid = self.pushEventFile(eventdict)
            #read in the status file and push to DB
            statusdict = self.parseStatus(statusfile)
            self.pushStatus(statusdict,eventid)
            #read in config files and push to DB
            for configfile in configfiles:
                conflist = self.parseConfigFile(configfile)
                self.pushConfig(eventid,conflist,configfile,version,revision)
            #read in data files and push to DB
            for stationfile in stationfiles:
                stationlist,created = self.parseStationFile(stationfile)
                self.pushStations(eventid,stationlist,stationfile,created)
            #read in the command line flags and push to DB
            #flagdict = self.getProgramFlags(eventdict['eventcode'])
            #self.pushFlags(eventid,flagdict)
            if os.path.isfile(faultfile):
                (firstline,segments) = self.parseFaultFile(faultfile)
                self.pushFault(eventid,segments,firstline,faultfile)
            if os.path.isfile(sourcefile):
                sourcedict = self.parseSourceFile(sourcefile)
                self.pushSource(eventid,sourcedict)
            if runfile is not None:
                runcontent = open(runfile,'rt').read()
            else:
                runcontent,runfile = self.makeRunFile(shakehome,eventdict)
            self.pushRun(eventid,runfile,runcontent)
        except Exception,msg:
            f = cStringIO.StringIO()
            traceback.print_exc(file=f)
            mytrace = f.getvalue()
            print mytrace
            sys.exit(1)

    def makeRunFile(self,shakehome,eventdict):
        eventcode = eventdict['eventcode']
        mag = eventdict['mag']
        if shakehome[-1] in ['\\','/']:
            shakehome = shakehome[0:-1] + os.path.sep
        else:
            shakehome = shakehome + os.path.sep
        template = ['<shakehome>grind -event <eventid> -lonspan <lonspan> <psa> -qtm -xml -nativesc -nooutlier',
                    '<shakehome>mapping -event <eventid> -timestamp -itopo -gsm -pgminten',
                    '<shakehome>plotregr -event <eventid> <psa> -lab_dev 6',
                    '<shakehome>genex -event <eventid> -zip -metadata -shape shape -shape hazus']
        if mag < 5:
            lonspan = '3'
        if mag >= 5 and mag < 6:
            lonspan = '4'
        else:
            lonspan = '6'
        if mag >= 4.8:
            psa = '-psa'
        else:
            psa = ''
        runcontent = ''
        for line in template:
            line = line.replace('<lonspan>',lonspan)
            line = line.replace('<psa>',psa)
            line = line.replace('<eventid>',eventcode)
            line = line.replace('<shakehome>',shakehome)
            runcontent = runcontent + line + '\n'
        runfile = 'RUN_'+eventcode
        return runcontent,runfile
            
    def pushRun(self,eventid,runfile,runcontent):
        query = 'INSERT INTO atlas_run_file (event_id,filename,content) VALUES (%i,"%s","%s")' % (eventid,runfile,runcontent)
        self.cursor.execute(query)
        self.connection.commit()
        
    def pushStatus(self,statusdict,eventid):
        for key,value in statusdict.iteritems():
            fmt = 'INSERT INTO atlas_status (event_id,statuskey,statusvalue) VALUES (%i,"%s","%s")'
            query = fmt % (eventid,key,value)
            self.cursor.execute(query)
            self.connection.commit()
        
    def pushEventFile(self,eventdict):
        fmt = '''INSERT INTO atlas_event
        (eventcode,lat,lon,depth,magnitude,
        time,timezone,locstring,created,type,network,inserttime,eid,ambflag) VALUES
        ("%s",%f,%f,%f,%f,"%s","%s","%s","%s","%s","%s","%s",NULL,0)'''
        tnow = datetime.datetime.now()
        tpl = (eventdict['eventcode'],eventdict['lat'],eventdict['lon'],
               eventdict['depth'],eventdict['mag'],eventdict['time'],
               eventdict['timezone'],eventdict['locstring'],eventdict['created'],
               eventdict['type'],eventdict['network'],tnow)
        query = fmt % tpl
        self.cursor.execute(query)
        self.connection.commit()
        query = 'SELECT id FROM atlas_event WHERE eventcode="%s"' % eventdict['eventcode']
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def pushConfig(self,eventid,conflist,confname,version,revision):
        fpath,fname = os.path.split(confname)
        fmt = 'INSERT INTO atlas_config (event_id,filename) VALUES (%i,"%s")'
        tpl = (eventid,fname)
        self.cursor.execute(fmt % tpl)
        self.connection.commit()
        query = 'SELECT id FROM atlas_config WHERE filename="%s" AND event_id=%i' % (fname,eventid)
        self.cursor.execute(query)
        configid = self.cursor.fetchone()[0]
        for confpair in conflist:
            param = confpair[0]
            value = confpair[1]
            query = 'INSERT INTO atlas_config_param (config_id,configparam,configvalue) VALUES (%i,"%s","%s")' % (configid,param,value)
            try:
                self.cursor.execute(query)
                self.connection.commit()
            except Exception,msg:
                pass
            

    def pushSource(self,eventid,sourcedict):
        fmt = 'INSERT INTO atlas_source (event_id,sourcekey,sourcevalue) VALUES (%i,"%s","%s")'
        for key,value in sourcedict.iteritems():
            tpl = (eventid,key,value)
            self.cursor.execute(fmt % tpl)
            self.connection.commit()
            

    def pushStations(self,eventid,stationlist,stationfile,created):
        fpath,filename = os.path.split(stationfile)
        createdstr = created.strftime('%Y-%m-%d %H:%M:%S')
        query = 'INSERT INTO atlas_station_file (event_id,filename,created) VALUES (%i,"%s","%s")' % (eventid,filename,createdstr)
        self.cursor.execute(query)
        self.connection.commit()
        query = 'SELECT id FROM atlas_station_file WHERE filename="%s" and event_id=%i' % (filename,eventid)
        self.cursor.execute(query)
        stationfileid = self.cursor.fetchone()[0]
        fmt1 = '''INSERT INTO atlas_station (stationfile_id,code,name,insttype,lat,lon,source,netid,commtype,intensity) VALUES (%i,"%s","%s","%s",%f,%f,"%s","%s","%s",%f)'''
        for station in stationlist:
            tpl = (stationfileid,station['code'],station['name'],
                   station['insttype'],station['lat'],station['lon'],
                   station['source'],station['netid'],station['commtype'],
                   station['intensity'])
            try:
                self.cursor.execute(fmt1 % tpl)
                self.connection.commit()
            except Exception,msg:
                pass
            fmt2 = 'SELECT id FROM atlas_station WHERE stationfile_id=%i AND code="%s" AND name="%s" AND lat=%.6f AND lon=%.6f'
            query = fmt2 % (stationfileid,station['code'],station['name'],station['lat'],station['lon'])
            self.cursor.execute(query)
            try:
                stationid = self.cursor.fetchone()[0]
            except Exception,msg:
                pass
            for comp in station['components']:
                name = comp['name']
                query = 'INSERT INTO atlas_component (station_id,name) VALUES (%i,"%s")' % (stationid,name)
                try:
                    self.cursor.execute(query)
                    self.connection.commit()
                except Exception,msg:
                    pass
                query = 'SELECT id FROM atlas_component WHERE station_id=%i AND name="%s"' % (stationid,name)
                try:
                    self.cursor.execute(query)
                except Exception,msg:
                    pass
                componentid = self.cursor.fetchone()[0]
                for key,compdict in comp['attributes'].iteritems():
                    value = compdict['value']
                    if compdict.has_key('flag'):
                        flag = compdict['flag']
                        query = 'INSERT INTO atlas_component_param (component_id,componentkey,flag) VALUES (%i,"%s","%s")' % (componentid,key,flag)
                        self.cursor.execute(query)
                        self.connection.commit()
                    if math.isnan(value):
                        fmt3 = 'INSERT INTO atlas_component_param (component_id,componentkey,componentvalue) VALUES (%i,"%s",%s)'
                        value = 'NULL'
                    else:
                        fmt3 = 'INSERT INTO atlas_component_param (component_id,componentkey,componentvalue) VALUES (%i,"%s",%f)'
                    tpl = (componentid,str(key),value)
                    try:
                        self.cursor.execute(fmt3 % tpl)
                    except Exception,msg:
                        pass
                    self.connection.commit()
                    

    def pushFault(self,eventid,segments,firstline,faultfile):
        fpath,filename = os.path.split(faultfile)
        fmt = 'INSERT INTO atlas_fault_file (event_id,filename,firstline) VALUES (%i,"%s","%s")'
        tpl = (eventid,filename,firstline)
        query = fmt % tpl
        self.cursor.execute(query)
        self.connection.commit()
        query = 'SELECT id FROM atlas_fault_file WHERE event_id=%i AND filename="%s"' % (eventid,filename)
        self.cursor.execute(query)
        faultfileid = self.cursor.fetchone()[0]
        hasDepth = False
        if len(segments[0][0]) == 2:
            fmt = 'INSERT INTO atlas_fault (faultfile_id,seqno,segment,lat,lon,depth) VALUES (%i,%i,%i,%f,%f,%s)'
        else:
            fmt = 'INSERT INTO atlas_fault (faultfile_id,seqno,segment,lat,lon,depth) VALUES (%i,%i,%i,%f,%f,%f)'
            hasDepth = True
        seqno = 0
        for i in range(0,len(segments)):
            segment = segments[i]
            for point in segment:
                lat = point[0]
                lon = point[1]
                if hasDepth:
                    depth = point[2]
                else:
                    depth = 'NULL'
                tpl = (faultfileid,seqno,i,lat,lon,depth)
                self.cursor.execute(fmt % tpl)
                self.connection.commit()
                seqno += 1

    def pushFlags(self,eventid,flagdict):
        fmt = 'INSERT INTO atlas_prog_flags (event_id,program,flags) VALUES (%i,"%s","%s")'
        for key,value in flagdict.iteritems():
            tpl = (eventid,key,value)
            self.cursor.execute(fmt % tpl)
            self.connection.commit()
    
    def close(self):
        self.cursor.close()
        self.connection.close()
        # self.shake_cursor.close()
        # self.shake_connection.close()

    def parseEventFile(self,eventfile):
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

    def parseSourceFile(self,sourcefile):
        f = open(sourcefile,'rt')
        sourcedict = {}
        for line in f.readlines():
            if line.strip().startswith('#'):
                continue
            if line.find('=') > -1:
                parts = line.split('=')
                sourcedict[parts[0].strip()] = parts[1].strip()
        return sourcedict

    def parseConfigFile(self,confname):
        #The output is NOT a dictionary because the config parameters are not guaranteed to be unique
        f = open(confname,'rt')
        conflist = []
        for line in f.readlines():
            if line.strip().startswith('#'):
                continue
            if line.find(':') > -1:
                parts = line.split(':')
                key = parts[0].strip()
                value = parts[1].strip().replace('"',"'")
                conflist.append((key,value))
        f.close()
        return conflist

    def parseStationFile(self,stationfile):
        dom = parse(stationfile)
        stationlist = dom.getElementsByTagName('stationlist')
        try:
            created = datetime.datetime.utcfromtimestamp(int(stationlist[0].getAttribute('created')))
        except:
            created = datetime.datetime.now()
        stations = []
        stationels = stationlist[0].getElementsByTagName('station')
        for statel in stationels:
            complist = statel.getElementsByTagName('comp')
            comps = []
            for comp in complist:
                compdict = {'name':comp.getAttribute('name')}
                attributes = {}
                for node in comp.childNodes:
                    if node.nodeType != node.ELEMENT_NODE:
                        continue
                    try:
                        attributes[node.nodeName] = {'value':float(node.getAttribute('value'))}
                    except:
                        continue
                    if node.hasAttribute('flag'):
                            attributes[node.nodeName]['flag'] = node.getAttribute('flag')

                compdict['attributes'] = attributes.copy()
                comps.append(compdict.copy())
            station = {}
            station['code'] = statel.getAttribute('code')
            #The lambda here filters out non-ascii characters
            station['name'] = filter(lambda x: x in string.printable,statel.getAttribute('name'))
            station['name'] = station['name'].replace('"','&quot;')
            station['insttype'] = statel.getAttribute('insttype')
            
            try:
                station['lat'] = float(statel.getAttribute('lat'))
                station['lon'] = float(statel.getAttribute('lon'))
            except ValueError:
                fmt = 'Station file %s has a non-float value "%s" or "%s" for lat or lon.'
                print fmt % (stationfile,statel.getAttribute('lat'),statel.getAttribute('lon'))
                station['lat'] = 0.0
                station['lon'] = 0.0
                
            station['source'] = statel.getAttribute('source')
            station['netid'] = statel.getAttribute('netid')
            station['commtype'] = statel.getAttribute('commtype')
            if statel.hasAttribute('intensity') and len(statel.getAttribute('intensity')):
                try:
                    station['intensity'] = float(statel.getAttribute('intensity'))
                except ValueError:
                    fmt = 'Station file %s has non-floating point attribute value "%s" for intensity.'
                    print fmt % (stationfile,statel.getAttribute('intensity'))
                    station['intensity'] = 0.0
            else:
                station['intensity'] = 0.0
            station['components'] = comps[:]
            stations.append(station.copy())
        dom.unlink()
        return (stations,created)

    def parseFaultFile(self,faultfile):
        points = []
        segments = []
        f = open(faultfile,'rt')
        firstline = f.readline()
        firstline = filter(lambda x: x in string.printable,firstline)
        if not firstline.strip().startswith('#'):
            f.seek(0,0)
            firstline = ''
        for line in f.readlines():
            line = filter(lambda x: x in string.printable,line)
            if line.strip().startswith('#'):
                continue
            if line.strip().startswith('>'):
                if len(points):
                    segments.append(points)
                    points = []
                    continue
            parts = line.split()
            if len(parts) == 3:
                points.append((float(parts[0]),float(parts[1]),float(parts[2])))
            elif len(parts) == 2:
                points.append((float(parts[0]),float(parts[1])))
            else:
                continue
        f.close()
        segments.append(points)
        return (firstline,segments)

    def getProgramFlags(self,eventid):
        fmt = 'select s.* from shake_runs s where s.evid="%s" and not exists (select * from shake_runs where evid=s.evid and s.version<version)'
        self.shake_cursor.execute(fmt % eventid)
        rows = self.shake_cursor.fetchall()
        flagdict = {}
        for row in rows:
            if not len(row[4].strip()): #no flags for this program, so we don't care
                continue
            flagdict[row[1]] = row[4]
        return flagdict

def getDataBaseConnections(shakehome):
    pwfile = os.path.join(shakehome,'pw','passwords')
    if not os.path.isfile(pwfile):
        raise Exception,'Missing password file %s' % pwfile
    f = open(pwfile,'rt')
    dbdict = {}
    for line in f.readlines():
        if line.startswith('database'):
            parts = line.split('=')
            parts = parts[1].split()
            db = parts[0]
            user = parts[1]
            passwd = parts[2]
            dbdict[db] = {'user':user,'password':passwd,'database':db}
    f.close()
    return dbdict

    

            
if __name__ == '__main__':
    usage = """usage: %prog [options] atlasdir [eventcode1 eventcode2]
    Typical usages:
    (creating database, loading in all events):
    %prog -c -v3.5 -r1.0 -s /opt/local/ShakeMap /opt/local/ShakeMap/data
    (not creating database, updating just a couple of events in the database):
    %prog -v3.5 -r1.0 -s /opt/local/ShakeMap/ /opt/local/ShakeMap/data 197501010101 197601010101
    """
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--createdb",
                      action="store_true", dest="createDatabase", default=False,
                      help="Delete all atlas data in pagercat database(!)")
    parser.add_option("-v", "--version", dest="version",
                      help="Inform the program about the version of ShakeMap that created the data", metavar="VERSION")
    parser.add_option("-r", "--revision", dest="revision",
                      help="Provide ShakeMap revision number",metavar="REVISION")
    parser.add_option("-s", "--shakehome", dest="shakehome",
                      help="Inform the program about the root directory for the ShakeMap installation", metavar="SHAKEHOME")
    (options, args) = parser.parse_args()

    if options.shakehome is None:
        print 'Must specify the --shakehome parameter.'
        parser.print_usage()
        sys.exit(0)

    dbdict = getDataBaseConnections(options.shakehome)

    if options.createDatabase:
        yesno = raw_input('>>Are you sure you want to drop all tables? (y/[n]) ')
        if yesno.lower() != 'y':
            print 'You have opted not to drop all tables.  Exiting.'
            sys.exit(0)
        pusher = DataBasePusher(dbdict)
        pusher.createDB()
        pusher.close()
        #if no other options are set, exit
        if not options.version and not options.revision:
            sys.exit(0)

    if len(args) < 1:
        print 'You must specify the atlas directory.'
        parser.print_help()
        sys.exit(0)

    atlasdir = args[0]
    
    if not options.version or not options.revision:
        print 'Version and revision numbers are required.'
        parser.print_usage()
        sys.exit(0)

    if options.version == '3.2':
        idlength = 12
    else:
        idlength = 12

    if not os.path.isdir(atlasdir):
        print 'Could not find Atlas directory "%s".' % atlasdir
        parser.print_usage()
        sys.exit(0)

    pusher = DataBasePusher(dbdict)
    
    doUpdate = False
    if len(args) > 1:
        allfolders = args[1:]
        doUpdate = True
    else:
        allfolders = os.listdir(atlasdir)

    pat = '\\d{%i}' % idlength #event folders should be a sequence of digits
    for f in allfolders:
        folder = os.path.join(atlasdir,f)
        if not os.path.isdir(folder) or not re.match(pat,f):
            continue

        eventfile = os.path.join(folder,'input','event.xml')
        if not os.path.isfile(eventfile):
            print 'No event.xml file for %s.  Skipping.' % f
            continue

        configfiles = glob.glob(folder+'/config/*.conf')
        stationfiles = glob.glob(folder+'/input/*_dat.xml')
        faultfile = glob.glob(folder+'/input/*_fault.txt')
        runfiles = glob.glob(folder+'/run_*.txt')
        if not len(runfiles):
            runfiles = glob.glob(folder+'/RUN_*.txt')
        if len(runfiles):
            runfile = runfiles[0]
        else:
            runfile = None
            
        if len(faultfile) > 0:
            if len(faultfile) > 1:
                p,fname = os.path.split(faultfile[0])
                print 'Found multiple fault files for event %s.  Taking %s' % (f,fname)
            faultfile = faultfile[0]
        else:
            faultfile = ''
        
        sourcefile = os.path.join(folder,'input','source.txt')
        statusfile = os.path.join(folder,'status.txt')
        version = options.version
        revision = options.revision
        print 'Saving event %s' % f
        pusher.pushEvent(eventfile,configfiles,stationfiles,sourcefile,faultfile,runfile,
                         statusfile,options.shakehome,doUpdate,version,revision,
                         dbdict['shakemap'])
        
        
    pusher.close()
            
        



