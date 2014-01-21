#!/usr/bin/env python

#stdlib imports
import os.path
import sys
import argparse

#third party
import MySQLdb as mysql
from neicio.tag import Tag
from neicmap import poly
import numpy as np

#local
from atlas2db import getDataBaseConnections


MAGHIERARCHY = ['atlas_event','other','cmt','pde-Mw','pdeisc-Mw','centennial','pde','pdeisc']
LOCHIERARCHY = ['atlas_event','other','centennial','pde','pdeisc','noaa']
DAMAGETABLES = {'pde':['damage',
                       'casualty'],
                'pdeisc':['damage',
                       'casualty'],
                'emdat':['fatalities','injured','affected','homeless','totalaffected','loss'],
                'htd':['tsudeaths','tsuinjuries','tsudamage','tsuhouses','eventdeaths',
                       'eventinjuries','eventdamage','eventhouses'],
                'noaa':['deaths','injuries','damage','dedamage','bdestroyed','bdamaged'],
                'utsu':['deaths','injuries','fireflag','damage']}

LOSSHIERARCHY = {'totalDeaths':[('pde','totalDeaths'),
                                ('pdeisc','totalDeaths'),
                                ('noaa','deaths'),
                                ('htd','eventdeaths'),
                                ('emdat','fatalities')],
                 'shakingDeaths':[('pde','shakingDeaths'),
                                  ('pdeisc','shakingDeaths'),
                                  ('htd','eventdeaths-tsudeaths')],
                 'dollars':[('munich','directLoss'),
                            ('emdat','loss'),
                            ('pde','econLoss'),
                            ('noaa','damage'),
                            ('htd','eventdamage')],
                 'injuries':[('pde','injured'),
                             ('pdeisc','injured'),
                             ('noaa','injuries'),
                             ('htd','eventinjuries'),
                             ('emdat','injured')]}


def readShakeZone(zonefile,zone='CRATON'):
    """
    Read ShakeMap ZoneConfig polygon file.
    @param zonefile: ShakeMap ZoneConfig polygon file.
    @keyword zone: Key code of zone to read in - defaults to 'CRATON'.
    @return: Tuple of x,y vertices delimiting stable continental region polygons.  Polygons are NaN-delimited.
    """
    x = []
    y = []
    missing = -999
    f = open(zonefile,'rt')
    tlines = f.readlines()
    f.close()
    markOn = False
    for tline in tlines:
        if tline.find(zone) > -1 and not tline.startswith('#'):
            markOn = True
            continue
        if markOn:
            if tline.strip() != '':
                #parse the coordinates from the line
                parts = tline.strip().split()
                x.append(float(parts[1]))
                y.append(float(parts[0]))
            else:
                x.append(missing)
                y.append(missing)
                markOn = False
    xr = np.array(x)
    yr = np.array(y)
    i = (xr == missing).nonzero()
    if i[0][0] == len(xr)-1: #if missing is only at the end of the list of points
        xr = xr[0:-1]
        yr = yr[0:-1]
    else:
        xr[i] = np.NaN
        yr[i] = np.NaN

    return (xr,yr)

def getOrigin(eid,cursor,loctable):
    locquery = 'SELECT time,lat,lon,depth FROM %s WHERE eid=%i' % (loctable,eid)
    cursor.execute(locquery)
    locrow = cursor.fetchone()
    if locrow is None:
        return None
    if locrow[3] is None:
        return None
    origindict = {}
    origindict['time'] = locrow[0]
    origindict['lat'] = locrow[1]
    origindict['lon'] = locrow[2]
    origindict['depth'] = locrow[3]
    origindict['source'] = loctable
    return origindict

def getMagnitude(eid,cursor,magtable):
    magnitude = None
    magfields = [('magnitude','magtype'),('magc1','magc1type'),('magc2','magc2type')]
    if magtable.find('Mw') > -1: #we're looking at PDE
        parts = magtable.split('-')
        magtable = parts[0]
        magtype = parts[1]
        for magtuple in magfields:
            magfield = magtuple[0]
            magftype = magtuple[1]
            query = 'SELECT %s FROM %s WHERE %s = "Mw" and eid=%i' % (magfield,magtable,magftype,eid)
            cursor.execute(query)
            lrow = cursor.fetchone()
            if lrow is None:
                continue
            else:
                magnitude = lrow[0]
                break
    else:
        query = 'SELECT magnitude from %s WHERE eid = %i' % (magtable,eid)
        cursor.execute(query)
        lrow = cursor.fetchone()
        if lrow is not None:
            magnitude = lrow[0]

    if magnitude is None:
        return None
    magdict = {}
    magdict['magnitude'] = magnitude
    magdict['source'] = magtable
    return magdict

def checkDamage(cursor,eid):
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
            continue
    return foundDamage

def checkAtlas(cursor,eid):
    hasAtlas = False
    query = 'SELECT id FROM atlas_event WHERE eid=%i' % eid
    cursor.execute(query)
    row = cursor.fetchone()
    if row is not None:
        hasAtlas = True
    return hasAtlas

def checkMagnitude(lat,lon,mag,pp):
    if mag >= 5.5:
        return True
    if mag >= 4.5 and pp.containsPoint(lon,lat):
        return True
    return False       

def main(argparser,args):
    zonefile = os.path.join(args.shakehome,'config','zone_config.conf')
    px,py = readShakeZone(zonefile)
    pp = poly.PagerPolygon(px,py)
    dbdict = getDataBaseConnections(args.shakehome)['atlas']
    db = mysql.connect(host='127.0.0.1',db=dbdict['database'],user=dbdict['user'],passwd=dbdict['password'])
    cursor = db.cursor()
    eventquery = 'SELECT a.id,a.ccode,a.lat,a.lon,a.magnitude,b.id FROM event a, atlas_event b WHERE a.id = b.eid'
    cursor.execute(eventquery)
    eventrows = cursor.fetchall()
    ic = 0
    f = open('expocat.xml','wt')
    for eventrow in eventrows:
        eid = eventrow[0]
        ccode = eventrow[1]
        lat = eventrow[2]
        lon = eventrow[3]
        mag = eventrow[4]
        atlasid = eventrow[5]
        hasDamage = checkDamage(cursor,eid)
        hasAtlas = checkAtlas(cursor,eid)
        meetsMagnitude = checkMagnitude(lat,lon,mag,pp)
        if not hasDamage and not hasAtlas and not meetsMagnitude:
            continue
        print 'Extracting %s (%i of %i)' % (eid,ic,len(eventrows))
        ic += 1
        eventdict = {}
        eventdict['ccode'] = ccode
        for loctable in LOCHIERARCHY:
            origindict = getOrigin(eid,cursor,loctable)
            if origindict is None:
                continue
            if not eventdict.has_key('origins'):
                eventcode = origindict['time'].strftime('%Y%m%d%H%M%S')
                eventdict['code'] = eventcode
                origindict['preferred'] = True
            else:
                origindict['preferred'] = False
            if eventdict.has_key('origins'):
                eventdict['origins'].append(origindict.copy())
            else:
                eventdict['origins'] = [origindict.copy()]
        for magtable in MAGHIERARCHY:
            magdict = getMagnitude(eid,cursor,magtable)
            if magdict is None:
                continue
            if not eventdict.has_key('magnitudes'):
                magdict['preferred'] = True
            else:
                magdict['preferred'] = False
            if eventdict.has_key('magnitudes'):
                eventdict['magnitudes'].append(magdict.copy())
            else:
                eventdict['magnitudes'] = [magdict.copy()]

        for lossfield in LOSSHIERARCHY.keys():
            for tabletuple in LOSSHIERARCHY[lossfield]:
                table,column = tabletuple
                lossdict = getLoss(table,column,cursor,eid)
                if not eventdict.has_key(lossfield+'s'):
                    lossdict['preferred'] = True
                else:
                    lossdict['preferred'] = False
                if eventdict.has_key(lossfield+'s'):
                    eventdict[lossfield+'s'].append(lossdict)
                else:
                    eventdict[lossfield+'s'] = [lossdict]

        fmt = 'SELECT ccode,exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10 FROM atlas_exposure WHERE event_id=%i'
        query = fmt % atlasid
        cursor.execute(query)
        exprows = cursor.fetchall()
        exposurelist = []
        for exprow in exprows:
            ccode = exprow[0]
            exposures = list(exprow[1:])
            exposurelist.append({'ccode':ccode,'exposure':exposures})
        eventdict['exposures'] = exposurelist
        
        f.write('\t<event eventcode="%s" ccode="%s">\n' % (eventdict['code'],eventdict['ccode']))
        for origin in eventdict['origins']:
            timestr = origin['time'].strftime('%Y-%m-%d %H:%M:%S')
            lat = origin['lat']
            lon = origin['lon']
            depth = origin['depth']
            source = origin['source']
            preferred = origin['preferred']
            fmt = '\t\t<origin time="%s" lat="%.4f" lon="%4f" depth="%.1f" source="%s" preferred="%s"/>\n'
            f.write(fmt % (timestr,lat,lon,depth,source,preferred))
        for mag in eventdict['magnitudes']:
            value = mag['magnitude']
            source = mag['source']
            preferred = mag['preferred']
            fmt = '\t\t<magnitude value="%.1f" source="%s" preferred="%s"/>\n'
            f.write(fmt % (value,source,preferred))
        for exp in eventdict['exposures']:
            ccode = exp['ccode']
            exposure = exp['exposure']
            fmt = '\t\t<exposure ccode="%s">%s</exposure>\n'
            f.write(fmt % (ccode,' '.join(exposure)))
            
        f.write('\t</event>\n')
    f.close()
    cursor.close()
    db.close()

if __name__ == '__main__':
    description='Export Exposure Catalog information from PAGER-Cat database into XML format'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('shakehome', help='Specify location of ShakeMap home directory')
    arguments = parser.parse_args()
    main(parser,arguments)    