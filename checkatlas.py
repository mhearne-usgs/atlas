#!/usr/bin/env python

#stdlib
import os.path
import sys
import ConfigParser
import argparse
from xml.dom import minidom
import csv
import glob

#third party
from neicio import esri
import mysql.connector as mysql
import numpy as np
from neicmap import country

CONFIGFILE = 'smconfig.ini'

def getConnection():
    homedir = os.path.abspath(sys.path[0]) #where is this script?
    configfile = os.path.join(homedir,CONFIGFILE)
    if not os.path.isfile(configfile):
        print 'Could not find config file %s on the system.' % configfile
        sys.exit(1)

    config = ConfigParser.ConfigParser()
    config.readfp(open(configfile,'rt'))
    host = '127.0.0.1'
    if config.has_option('DATABASE','host'):
        host = config.get('DATABASE','host')
    db = config.get('DATABASE','db')
    user = config.get('DATABASE','user')
    password = config.get('DATABASE','password')

    connection = mysql.connect(db=db,user=user,passwd=password,host=host)
    cursor = connection.cursor()
    return (connection,cursor)

def checkEvent(eventxmlfile):
    root = minidom.parse(eventxmlfile)
    event = root.getElementsByTagName('earthquake')[0]
    hasmech = False
    hasnewmech = True
    if event.hasAttribute('type'):
        hasmech = True
        mechtype = event.getAttribute('type')
        if mechtype == 'NS':
            hasnewmech = False
    root.unlink()
    return (hasmech,hasnewmech)

def checkFault(eventcode,faultfile,faultdict):
    faultHasDepths = True
    faultHasReference = False
    faultNamedCorrectly = False
    faultClosed = False
    eventshort = eventcode[0:12]
    if eventshort not in faultdict.keys():
        raise LookupError,'Missing event %s in references database' % eventcode
    lines = open(faultfile,'rt').readlines()
    faultname,faultref = faultdict[eventshort]
    if lines[0].strip().replace('#','') == faultref.strip():
        faultHasReference = True
    faultpath,faultfile = os.path.split(faultfile)
    if faultfile == faultname:
        faultNamedCorrectly = True
    firstline = None
    for line in lines:
        if line.strip().startswith('#') or line.strip().startswith('>'):
            continue
        if firstline is None:
            firstline = [float(p) for p in line.split()]
            if len(firstline) < 3:
                faultHasDepths = False
            thisline = firstline[:]
            continue
        thisline = [float(p) for p in line.split()]
        if len(thisline) < 3:
            faultHasDepths = False
    if faultHasDepths:
        if thisline[0] == firstline[0] and thisline[1] == firstline[1] and thisline[2] == firstline[2]:
            faultClosed = True

    return (faultHasDepths,faultHasReference,faultNamedCorrectly,faultClosed)

def getFaultDict(faultref):
    faultfile = open(faultref,'rt')
    csvreader = csv.reader(faultfile)
    eventdict = {}
    for row in csvreader:
        try:
            int(row[0])
        except:
            continue
        shortref = row[1]+'_fault.txt'
        longref = row[2]
        eventid = row[0]
        eventdict[eventid] = (shortref,longref)
    faultfile.close()
    return eventdict

def checkConstrained(eventfolder):
    statusfile = os.path.join(eventfolder,'status.txt')
    if not os.path.isfile(statusfile):
        return False
    lines = open(statusfile,'rt').readlines()
    for line in lines:
        if line.find('status') > -1:
            parts = line.split(':')
            if parts[1].strip().lower() == 'constrained':
                return True
    return False

def printEventErrors(eventcode,folder,eventxml,faultdict):
    hasFault = False
    hasMultiFault = True
    hasMechanism = False
    faultHasDepths = False
    faultHasReference = False
    faultNamedCorrectly = False
    faultClosed = False
    isConstrained = False
    hasNewMechanism = False #does this event have NM for mechanism, if it has a mechanism?
    hasMechanism,hasNewMechanism = checkEvent(eventxml)
    isConstrained = checkConstrained(folder)
    isMissing = False
    faultfiles = glob.glob(os.path.join(folder,'input','*_fault.txt'))
    if len(faultfiles):
        hasFault = True
        if len(faultfiles) == 1:
            hasMultiFault = False
            try:
                faultHasDepths,faultHasReference,faultNamedCorrectly,faultClosed = checkFault(eventcode,faultfiles[0],faultdict)
            except LookupError,excobj:
                if not isConstrained:
                    isMissing = True

    fault1bad = hasFault and hasMultiFault
    fault2bad = hasFault and not faultNamedCorrectly
    fault3bad = hasFault and not faultClosed
    fault4bad = hasFault and not faultHasReference
    fault5bad = hasFault and not faultHasDepths
    mechbad = hasMechanism and not hasNewMechanism
    nuggets = []
    if fault1bad:
        nuggets.append('Multiple fault files')
    if fault2bad:
        nuggets.append('Misnamed fault file')
    if fault3bad:
        nuggets.append('Fault not closed')
    if fault4bad:
        nuggets.append('Fault missing reference')
    if fault5bad:
        nuggets.append('Fault missing depths')
    if mechbad:
        nuggets.append('Event missing mechanism')

    if isConstrained:
        if fault1bad or fault3bad or fault5bad:
            print 'Event %s (constrained) issues: %s' % (eventcode,' | '.join(nuggets))
    else:
        if fault1bad or fault2bad or fault3bad or fault4bad or fault5bad or mechbad:
            print 'Event %s (reviewed) issues: %s' % (eventcode,' | '.join(nuggets))
    
        

def main(args):
    if not os.path.isdir(args.datadir):
        print 'Could not find data directory %s on the system.' % args.datadir
        sys.exit(1)

    if not os.path.isfile(args.faultref):
        print 'Could not find fault reference file %s on the system.' % args.faultref
        sys.exit(1)
    
    folders = os.listdir(args.datadir)

    faultdict = getFaultDict(args.faultref)
    
    for eventcode in sorted(faultdict.keys()):
        folders = glob.glob(os.path.join(args.datadir,eventcode+'*'))
        if len(folders) > 1:
            print 'Ambiguous match for event code %s:' % eventcode
            for fol in folders:
                print '\t%s' % fol
        if not len(folders):
            print 'No fault match for event code %s' % eventcode
            continue
        folder = folders[0]
        eventxml = os.path.join(folder,'input','event.xml')
        # if not os.path.isfile(eventxml):
        #     continue
        ffile,longref = faultdict[eventcode]
        faultfile = os.path.join(folder,'input',ffile)
        if not os.path.isfile(faultfile):
            print 'Fault file %s is missing.' % faultfile
            continue
        faultHasDepths,faultHasReference,faultNamedCorrectly,faultClosed = checkFault(eventcode,faultfile,faultdict)
        if args.checkDepths and not faultHasDepths:
            print 'Event %s is missing depths' % eventcode
        if args.checkReference and not faultHasReference:
            print 'Event %s has the wrong reference' % eventcode
            if args.fixEvents:
                fixReference(faultfile,longref)
        if args.checkName and not faultNamedCorrectly:
            print 'Event %s has the wrong fault file name' % eventcode
        if args.checkClosed and not faultClosed:
            print 'Event %s fault file is not closed' % eventcode
            if args.fixEvents:
                fixClosed(faultfile)
        
        

        
                        
        
    #Wait a minute - what do I need database info for anyway?  Keep it here just in case I think of a reason.
    # connection,cursor = getConnection()
    # cursor.close()
    # connection.close() 

if __name__ == '__main__':
    description = 'Perform basic QA/QC on folder of ShakeMap Atlas data.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('datadir', metavar='DATADIR', 
                        help='a folder of ShakeMap data')
    parser.add_argument('faultref', metavar='FAULTREF', 
                        help='Specify a CSV file containing fault reference information.')
    parser.add_argument('-d', dest='checkDepths',action='store_true')
    parser.add_argument('-r', dest='checkReference',action='store_true')
    parser.add_argument('-n', dest='checkName',action='store_true')
    parser.add_argument('-c', dest='checkClosed',action='store_true')
    parser.add_argument('-f',dest='fixEvents',action='store_true',
                        help='Fix fault files where discrepancies are found.')

    args = parser.parse_args()

    main(args)

    
