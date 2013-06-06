#!/usr/bin/env python

#stdlib imports
from xml.dom import minidom
import os.path
import sys
import tarfile
import copy
import datetime
import math
import zipfile
import glob

def parseEvent(eventxml):
    event = {}
    dom = minidom.parse(eventxml)
    eqtag = dom.getElementsByTagName('earthquake')[0]
    event['id'] = eqtag.getAttribute('id')
    event['mag'] = float(eqtag.getAttribute('mag'))
    year = int(eqtag.getAttribute('year'))
    month = int(eqtag.getAttribute('month'))
    day = int(eqtag.getAttribute('day'))
    hour = int(eqtag.getAttribute('hour'))
    minute = int(eqtag.getAttribute('minute'))
    second = int(math.floor(float(eqtag.getAttribute('second'))))
    event['time'] = datetime.datetime(year,month,day,hour,minute,second)
    dom.unlink()
    return event

def parseInfo(infoxml):
    event = {'hasbias':False,'hasfault':False}
    dom = minidom.parse(infoxml)
    tags = dom.getElementsByTagName('tag')
    for tag in tags:
        name = tag.getAttribute('name')
        if name == 'bias' or name == 'mi_bias':
            value = tag.getAttribute('value')
            bias_values = [float(b) for b in value.split()]
            if max(bias_values) > 0.0:
                event['hasbias'] = True
        if name == 'faultfiles':
            value = tag.getAttribute('value')
            if len(value.strip()):
                event['hasfault'] = True
    dom.unlink()
    return event

def parseFolder(rootfolder,startdate,enddate):
    eventfolders = os.listdir(rootfolder)
    events = []
    for efolder in eventfolders:
        eventfolder = os.path.join(rootfolder,efolder)
        eventxml = os.path.join(eventfolder,'input','event.xml')
        if not os.path.isfile(eventxml):
            continue
        event1 = parseEvent(eventxml)
        if event1['time'] < startdate or event1['time'] > enddate:
            continue
        infoxml = os.path.join(eventfolder,'output','info.xml')
        if not os.path.isfile(infoxml):
            continue
        event = parseInfo(infoxml)
        event['folder'] = eventfolder
        events.append(copy.deepcopy(event))
    return events

def getFiles(folder):
    allfiles = []
    eventfile = os.path.join(folder,'input','event.xml')
    allfiles.append(eventfile)
    if not os.path.isfile(eventfile):
        print 'No event.xml file for %s.  Skipping.' % f
        return []

    configfiles = glob.glob(folder+'/config/*.conf')
    allfiles += configfiles
    stationfiles = glob.glob(folder+'/input/*_dat.xml')
    allfiles += stationfiles
    faultfile = glob.glob(folder+'/input/*_fault.txt')
    
    runfiles = glob.glob(folder+'/run_*.txt')
    if not len(runfiles):
        runfiles = glob.glob(folder+'/RUN_*.txt')
    allfiles += runfiles

    if len(faultfile) > 0:
        if len(faultfile) > 1:
            p,fname = os.path.split(faultfile[0])
            print 'Found multiple fault files for event %s.  Taking %s' % (f,fname)
        faultfile = faultfile[0]
        allfiles.append(faultfile)

    sourcefile = os.path.join(folder,'input','source.txt')
    if os.path.isfile(sourcefile):
        allfiles.append(sourcefile)
    statusfile = os.path.join(folder,'status.txt')
    if os.path.isfile(statusfile):
        allfiles.append(statusfile)
    else:
        f = open(statusfile,'wt')
        f.write('Status: Constrained\n')
        f.write('Reviewer: unknown\n')
        f.close()
        allfiles.append(statusfile)
    return allfiles
    
if __name__ == '__main__':
    #constrained events are defined as those that have finite fault data or have a non-zero bias correction.
    rootfolder = sys.argv[1]
    startdatestr = sys.argv[2]
    enddatestr = sys.argv[3]

    startdate = datetime.datetime.strptime(startdatestr,'%Y-%m-%d')
    enddate = datetime.datetime.strptime(enddatestr,'%Y-%m-%d')
    
    if os.path.isdir(rootfolder):
        events = parseFolder(rootfolder,startdate,enddate)
    else:
        print 'Root folder "%s" is not a directory'
        sys.exit(1)

    myzip = zipfile.ZipFile('constrained.zip','w',zipfile.ZIP_DEFLATED)
    for event in events:
        print event['folder']
        if event['hasbias'] or event['hasfault']:
            filenames = getFiles(event['folder'])
            p1,f1 = os.path.split(event['folder'])
            for filename in filenames:
                try:
                    p2,f2 = filename.split(f1)
                except:
                    pass
                arcname = f1+f2
                myzip.write(filename,arcname)
                
        
    myzip.close()
            
    
