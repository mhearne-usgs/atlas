#!/usr/bin/env python

#stdlib imports
import sys
import os.path
import datetime
import gc
from optparse import OptionParser
import glob
import re

#third party imports
import numpy
from neicio import shake
from neicio import esri
from neicmap import country
from neicutil.text import *
from neicpager.exposure import Exposure
from neicpager.population import PopulationGrowth
from neicpager.nationpop import readNationPop

TIMEFMT = '%Y-%m-%d %H:%M:%S'

def renderExposure(expresults,eqdict,format='screen'):
    etime = eqdict['time'].strftime(TIMEFMT)
    code = eqdict['code']
    lat = eqdict['lat']
    lon = eqdict['lon']
    depth = eqdict['depth']
    mag = eqdict['mag']
    if format == 'screen':
        print '%s: %s %.4f,%.4f %.1f km M%.1f' % (code,etime,lat,lon,depth,mag)
        for ccode,cexp in expresults.iteritems():
            exposure = [exp['exposure'] for exp in cexp]
            print '%s exposure:' % ccode
            for i in range(0,len(exposure)):
                print '\tMMI %i - %s' % (i+1,commify(exposure[i]))
        return
                                         
    print '<event time="%s" code="%s" lat="%.4f" lon="%.4f" depth="%.1f" mag="%.1f">' % (etime,code,lat,lon,depth,mag)
    for ccode,cexp in expresults.iteritems():
        exposure = [float(exp['exposure']) for exp in cexp]
        expstr = ' '.join(['%.0f' % exp for exp in exposure])
        print '\t<exposure ccode="%s">' % ccode
        print '\t\t%s' % expstr
        print '\t</exposure>'
    print '</event>'
    

def getClosestPop(eyear,datafolder):
    popfiles = glob.glob(os.path.join(datafolder,'*pop*.flt'))
    imin = -1
    mindiff = 99999999
    for i in range(0,len(popfiles)):
        pfile = popfiles[i]
        dyear = int(re.search('\d{4}',pfile).group())
        if abs(dyear-eyear) < mindiff:
            mindiff = abs(dyear-eyear)
            imin = i

    return popfiles[imin]
        
def getExposure(shakefile,popfile,isofile,growthfile,multiCountry=False):
    if not os.path.isfile(shakefile):
            return (None,None,'No such file %s' % shakefile)
    try:
        ratedict = readNationPop(growthfile)
        poppath,popfname = os.path.split(popfile)
        m = re.search('[0-9]{4}',popfname)
        baseyear = int(m.group())
        expobj = Exposure(shakefile,popfile,isofile,ratedict,baseyear)
    except Exception,msg:
        print 'Error running event "%s"' % (msg)
        return (None,None,msg)

    if multiCountry:
        expresults = expobj.getCountryExposure(mmiranges)
    else:
        expresults = expobj.getAggregateExposure(mmiranges)

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
    isofile = os.path.join(datadir,'isogrid.bil')
    growthfile = os.path.join(datadir,'WPP2012_POP_F02_POPULATION_GROWTH_RATE.XLS')
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
        shakemap = shake.ShakeGrid(shakefile)
        shakedict = shakemap.getAttributes()
        #figure out which population data file to use...
        popfile = getClosestPop(shakedict['event']['event_timestamp'].year,datadir)
        expresults,shakedict,msg = getExposure(shakefile,popfile,isofile,growthfile,multiCountry=True)
        if expresults is None:
            print 'Error running event %s: "%s".' % msg
            sys.exit(1)

        eventcode = shakedict['shakemap_grid']['shakemap_originator']+shakedict['shakemap_grid']['event_id']
        etime = shakedict['event']['event_timestamp']
        lat = shakedict['event']['lat']
        lon = shakedict['event']['lon']
        depth = shakedict['event']['depth']
        mag = shakedict['event']['magnitude']
        eqdict = {'code':eventcode,'time':etime,'lat':lat,'lon':lon,'depth':depth,'mag':mag}
        renderExposure(expresults,eqdict,format='screen')
        sys.exit(0)
    
    isogrid = esri.EsriGrid(isofile)
    print '<expresults>'
    ic = 0
    folders = os.listdir(atlasdir)
    for folder in folders:
        fullfolder = os.path.join(atlasdir,folder)
        sys.stderr.write('Processing %s (%i of %i)\n' % (folder,ic,len(folders)))
        ic += 1
        shakefile = os.path.join(fullfolder,'output','grid.xml')
        if not os.path.isfile(shakefile):
            sys.stderr.write('No grid.xml file found for %s\n' % folder)
            continue

        shakemap = shake.ShakeGrid(shakefile)
        shakedict = shakemap.getAttributes()
        eventcode = shakedict['shakemap_grid']['shakemap_originator']+shakedict['shakemap_grid']['event_id']
        etime = shakedict['event']['event_timestamp']
        popfile = getClosestPop(etime.year,datadir)
        expresults,shakedict,msg = getExposure(shakefile,popfile,isofile,growthfile,multiCountry=True)
        if expresults is None:
            sys.stdout.write('Error running event %s: "%s".\n' % (folder,msg))
            continue
        lat = shakedict['event']['lat']
        lon = shakedict['event']['lon']
        depth = shakedict['event']['depth']
        mag = shakedict['event']['magnitude']
        eqdict = {'code':eventcode,'time':etime,'lat':lat,'lon':lon,'depth':depth,'mag':mag}
        renderExposure(expresults,eqdict,format='xml')
        sys.stdout.flush()
    print '</expresults>'


