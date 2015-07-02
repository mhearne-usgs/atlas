#!/usr/bin/env python

#stdlib imports
import os.path
from xml.dom import minidom
import datetime
import sys

TIMEFMT = '%Y-%m-%d %H:%M:%S'

if __name__ == '__main__':
    filelist = sys.argv[1:] #presumed to be in the proper hierarchical order
    eventdict = {}
    for f in filelist:
        dom = minidom.parse(f)
        eventlist = dom.getElementsByTagName('event')
        for event in eventlist:
            time = datetime.datetime.strptime(event.getAttribute('time'),TIMEFMT)
            eventcode = event.getAttribute('code')[2:]
            if eventdict.has_key(eventcode):
                continue
            lat = float(event.getAttribute('lat'))
            lon = float(event.getAttribute('lon'))
            depth = float(event.getAttribute('depth'))
            mag = float(event.getAttribute('mag'))
            expodict = {}
            expolist = event.getElementsByTagName('exposure')
            for expo in expolist:
                ccode = expo.getAttribute('ccode')
                exposure = [int(e) for e in expo.firstChild.data.split()]
                expodict[ccode] = exposure
            eventdict[eventcode] = {'time':time,'lat':lat,'lon':lon,'depth':depth,'mag':mag,'exposure':expodict.copy()}
            
        
        dom.unlink()
    outfile = 'merged_exporesults.xml'
    f = open(outfile,'wt')
    for eventcode,eventdict in eventdict.iteritems():
        f.write('\t<event time="%s" code="us%s" lat="%

    f.close()
