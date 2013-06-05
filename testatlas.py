#!/usr/bin/env python

from atlas2db import *

# stationlist = parseStationFile('obs_dat.xml')
# for station in stationlist:
#     print station

# conflist = parseConfigFile('grind.conf')
# for config in conflist:
#     print '%s = %s' % (config[0],config[1])

dbdict = {'atlas':{'database':'atlas','user':'atlas','password':'atlas'},
          'shakemap':{'database':'shakemap','user':'shake','password':'atlas'}
          }
pusher = DataBasePusher(dbdict)
segments = pusher.parseFaultFile('managua_fault.txt')
for segment in segments:
    for point in segment:
        print point
