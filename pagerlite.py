#!/usr/bin/env python

#local imports
from losspager.io import shake
from losspager.io import esri
from losspager.exposure import exposure
from losspager.map import country,region

#stdlib imports
import sys
import os.path
import datetime
import gc
from optparse import OptionParser

#third party imports
import numpy

TIMEFMT = '%Y-%m-%d %H:%M:%S'

def getExposure(shakefile,popfile,isofile):
    if not os.path.isfile(shakefile):
            return (None,None,'No such file %s' % shakefile)
    try:
        expobj = exposure.Exposure(shakefile,popfile,isofile)
    except Exception,msg:
        print 'Error running event %s - "%s"' % (folder,msg)
        return (None,None,msg)
    expresults = expobj.calcBasicExposure(mmiranges)
    shakeobj = shake.ShakeGrid(shakefile)
    shakedict = shakeobj.getAttributes()

    #try freeing memory by allocating big objects to None
    shakeobj = None
    expobj = None
    return (expresults,shakedict,'')

if __name__ == '__main__':
    usage = """usage: %prog [options] atlasdir|eventdir datadir
    When -s option is used, first argument is assumed to be a
    folder containing a single event."""
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--single",
                      action="store_true", dest="singleEvent", default=False,
                      help="Run a single event")

    (options, args) = parser.parse_args()
        
    if len(args) < 2:
        parser.print_help()
        sys.exit(1)

    
    atlasdir = args[0]
    datadir = args[1]
    popfile = os.path.join(datadir,'lspop2010.flt')
    isofile = os.path.join(datadir,'isogrid.bil')
    mmiranges = numpy.array([[  0.5,   1.5],
                             [  1.5,   2.5],
                             [  2.5,   3.5],
                             [  3.5,   4.5],
                             [  4.5,   5.5],
                             [  5.5,   6.5],
                             [  6.5,   7.5],
                             [  7.5,   8.5],
                             [  8.5,   9.5],
                             [  9.5,  10.5]])

    if options.singleEvent:
        shakefile = os.path.join(atlasdir,'output','grid.xml')
        expresults,shakedict,msg = getExposure(shakefile,popfile,isofile)
        if expresults is None:
            print 'Error running event %s: "%s".' % msg
            sys.exit(1)
        ncols = shakedict['grid_specification']['nlon']
        nrows = shakedict['grid_specification']['nlat']
        npixels = nrows*ncols
        eventcode = shakedict['shakemap_grid']['shakemap_originator']+shakedict['shakemap_grid']['event_id']
        etime = shakedict['event']['event_timestamp']
        lat = shakedict['event']['lat']
        lon = shakedict['event']['lon']
        depth = shakedict['event']['depth']
        mag = shakedict['event']['magnitude']

        #figure out which country we're in
        try:
            isogrid.load(bounds=(lon-0.5,lon+0.5,lat-0.5,lat+0.5))
            numcode = int(isogrid.getValue(lat,lon))
            cdict = country.getCountryCode(numcode)
            ccode = cdict['alpha2']
            pregion = region.getPagerRegionByCountry(ccode)
        except:
            pregion = 0
            ccode = '0'

        exp1 = expresults[0]
        exp2 = expresults[1]
        exp3 = expresults[2]
        exp4 = expresults[3]
        exp5 = expresults[4]
        exp6 = expresults[5]
        exp7 = expresults[6]
        exp8 = expresults[7]
        exp9 = expresults[8]
        exp10 = expresults[9]
        fmt = '%s,%s,%.4f,%.4f,%.1f,%.1f,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%s,%i'
        tstr = etime.strftime(TIMEFMT)
        tpl = (eventcode,tstr,lat,lon,depth,mag,exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10,ccode,pregion)
        print fmt % tpl
        sys.exit(0)
    
    f = open('expresults.csv','wt')
    f.write('eventcode,time,lat,lon,depth,mag,exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10,ccode,PRegion\n')
    icount = 0
    start = datetime.datetime.now()

    isogrid = esri.EsriGrid(isofile)
        
    oldelapsed = 0
    for folder in os.listdir(atlasdir):
        fullfolder = os.path.join(atlasdir,folder)
        shakefile = os.path.join(fullfolder,'output','grid.xml')
        expresults,shakedict = getExposure(shakefile,popfile,isofile)
        
        ncols = shakedict['grid_specification']['nlon']
        nrows = shakedict['grid_specification']['nlat']
        npixels = nrows*ncols
        eventcode = shakedict['shakemap_grid']['shakemap_originator']+shakedict['shakemap_grid']['event_id']
        etime = shakedict['event']['event_timestamp']
        lat = shakedict['event']['lat']
        lon = shakedict['event']['lon']
        depth = shakedict['event']['depth']
        mag = shakedict['event']['magnitude']

        #figure out which country we're in
        try:
            isogrid.load(bounds=(lon-0.5,lon+0.5,lat-0.5,lat+0.5))
            numcode = int(isogrid.getValue(lat,lon))
            cdict = country.getCountryCode(numcode)
            ccode = cdict['alpha2']
            pregion = region.getPagerRegionByCountry(ccode)
        except:
            pregion = 0
        

        exp1 = expresults[0]
        exp2 = expresults[1]
        exp3 = expresults[2]
        exp4 = expresults[3]
        exp5 = expresults[4]
        exp6 = expresults[5]
        exp7 = expresults[6]
        exp8 = expresults[7]
        exp9 = expresults[8]
        exp10 = expresults[9]
        fmt = '%s,%s,%.4f,%.4f,%.1f,%.1f,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%s,%i\n'
        tstr = etime.strftime(TIMEFMT)
        tpl = (eventcode,tstr,lat,lon,depth,mag,exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10,ccode,pregion)
        f.write(fmt % tpl)
        icount += 1
        elapsed = (datetime.datetime.now() - start).seconds
        delapsed = (elapsed - oldelapsed)/npixels
        oldelapsed = elapsed
        print 'Results for %-5i events - %-5i seconds elapsed, %.2e pixels' % (icount,elapsed,npixels)
        sys.stdout.flush()
        
    f.close()

