#!/usr/bin/env python

#stdlib imports
import os.path
import sys
import argparse

#third party
import mysql.connector as mysql
from neicio.tag import Tag
from neicmap import poly
import numpy as np

#local
from atlas2db import getDataBaseConnections


MAGHIERARCHY = ['atlas_event',
                'cmt',
                'pde-Mw',
                'pdecomcat2-Mw',
                'pdecomcat3-Mw',
                'pdecomcat4-Mw',
                'pdeisc-Mw',
                'centennial',
                'pde',
                'pdecomcat',
                'pdecomcat2',
                'pdecomcat3',
                'pdecomcat4',
                'pdeisc']
LOCHIERARCHY = ['atlas_event',
                'centennial',
                'pde',
                'pdecomcat',
                'pdecomcat2',
                'pdecomcat3',
                'pdecomcat4',
                'pdeisc',
                'noaa']
DAMAGETABLES = {'pde':['damage',
                       'casualty'],
                'pdecomcat2':['damage',
                              'casualty'],
                'pdecomcat3':['damage',
                              'casualty'],              
                'pdecomcat4':['damage',
                              'casualty'],
                'pdeisc':['damage',
                       'casualty'],
                'emdat':['fatalities','injured','affected','homeless','totalaffected','loss'],
                'htd':['tsudeaths','tsuinjuries','tsudamage','tsuhouses','eventdeaths',
                       'eventinjuries','eventdamage','eventhouses'],
                'noaa':['deaths','injuries','damage','dedamage','bdestroyed','bdamaged'],
                'other':['injured','homeless','landslideDeaths','buildingsDamaged','undiffDeaths',
                         'otherDeaths','missing','totalDeaths','tsunamiDeaths','shakingDeaths','buildingsDestroyed'],
                'utsu':['deaths','injuries','fireflag','damage']}

LOSSHIERARCHY = {'totalDeaths':[('other','totalDeaths'),
                                ('pde','totalDeaths'),
                                ('pdecomcat2','totalDeaths'),
                                ('pdecomcat3','totalDeaths'),
                                ('pdecomcat4','totalDeaths'),
                                ('pdeisc','totalDeaths'),
                                ('noaa','deaths'),
                                ('htd','eventdeaths'),
                                ('emdat','fatalities')],
                 'shakingDeaths':[('other','shakingDeaths'),
                                  ('pde','shakingDeaths'),
                                  ('pdecomcat2','shakingDeaths'),
                                  ('pdecomcat3','shakingDeaths'),
                                  ('pdecomcat4','shakingDeaths'),
                                  ('pdeisc','shakingDeaths'),
                                  ('htd','eventdeaths-tsudeaths')],
                 'dollars':[('munich','directLoss'),
                            ('emdat','loss'),
                            ('pde','econLoss'),
                            ('pdecomcat2','econLoss'),
                            ('pdecomcat3','econLoss'),
                            ('pdecomcat4','econLoss'),
                            ('noaa','damage'),
                            ('htd','eventdamage')],
                 'injured':[('other','injured'),
                            ('pde','injured'),
                            ('pdecomcat2','injured'),
                            ('pdecomcat3','injured'),
                            ('pdecomcat4','injured'),
                            ('pdeisc','injured'),
                            ('noaa','injuries'),
                            ('htd','eventinjuries'),
                            ('emdat','injured')],
                 'displaced':[('other','homeless'),
                              ('emdat','homeless'),
                              ('pde','homeless'),
                              ('pdecomcat2','homeless'),
                              ('pdecomcat3','homeless'),
                              ('pdecomcat4','homeless'),
                              ('pdeisc','homeless'),],
                 'tsunamiDeaths':[('other','tsunamiDeaths'),
                                  ('htd','tsudeaths'),
                                  ('pde','tsunamiDeaths'),
                                  ('pdecomcat2','tsunamiDeaths'),
                                  ('pdecomcat3','tsunamiDeaths'),
                                  ('pdecomcat4','tsunamiDeaths'),
                                  ('pdeisc','tsunamiDeaths')],
                 'tsunamiInjured':[('htd','tsuinjuries')],
                 'missing':[('other','missing'),
                            ('pde','missing'),
                            ('pdecomcat2','missing'),
                            ('pdecomcat3','missing'),
                            ('pdecomcat4','missing'),
                            ('pdeisc','missing')],
                 'buildingsDamaged':[('other','buildingsDamaged'),
                                     ('pde','buildDamaged'),
                                     ('pdecomcat2','buildDamaged'),
                                     ('pdecomcat3','buildDamaged'),
                                     ('pdecomcat4','buildDamaged'),
                                     ('pdeisc','buildDamaged'),
                                     ('htd','eventdamage'),
                                     ('noaa','bdamaged')],
                 'buildingsDestroyed':[('other','buildingsDestroyed'),
                                       ('pde','buildDestroyed'),
                                       ('pdecomcat2','buildDestroyed'),
                                       ('pdecomcat3','buildDestroyed'),
                                       ('pdecomcat4','buildDestroyed'),
                                       ('pdeisc','buildDestroyed'),
                                       ('htd','eventhouses'),
                                       ('noaa','bdestroyed')],
                 'buildingsDamagedOrDestroyed':[('pde','totDamagedBuild'),
                                                ('pdecomcat2','totDamagedBuild'),
                                                ('pdecomcat3','totDamagedBuild'),
                                                ('pdecomcat4','totDamagedBuild'),
                                                ('pdeisc','totDamagedBuild')],
                'tsunamiBuildingsDamaged':[('htd','tsudamage')],
                'tsunamiBuildingsDestroyed':[('htd','tsuhouses')],}

EFFECTS = {'tsunami':[('pde','tsunami'),
                      ('pdecomcat2','tsunami'),
                      ('pdecomcat3','tsunami'),
                      ('pdecomcat4','tsunami'),
                      ('pdeisc','tsunami'),
                      ('htd','maxheight'),
                      ('emdat','tsunami'),
                      ('utsu','tsunami')],
           'fire':[('pde','fire'),
                   ('pdecomcat2','fire'),
                   ('pdecomcat3','fire'),
                   ('pdecomcat4','fire'),
                   ('pdeisc','fire'),
                   ('emdat','fire'),
                   ('utsu','fireflag')],
           'liquefaction':[('pde','liquefaction'),
                           ('pdecomcat2','liquefaction'),
                           ('pdecomcat3','liquefaction'),
                           ('pdecomcat4','liquefaction'),
                           ('pdeisc','liquefaction')],
           'landslide':[('pde','landslide'),
                        ('pdecomcat2','landslide'),
                        ('pdecomcat3','landslide'),
                        ('pdecomcat4','landslide'),
                        ('pdeisc','landslide'),
                        ('emdat','landslide')],
           'damage':[('pde','damage'),
                     ('pdecomcat2','damage'),
                     ('pdecomcat3','damage'),
                     ('pdecomcat4','damage'),
                     ('pdeisc','damage')],
           'casualty':[('pde','casualty'),
                       ('pdecomcat2','casualty'),
                       ('pdecomcat3','casualty'),
                       ('pdecomcat4','casualty'),
                       ('pdeisc','casualty')]}

SOURCES = {'atlas_event':'Various',
           'centennial':'Centennial',
           'cmt':'GCMT',
           'htd':'NOAA HTD',
           'isc':'ISC-GEM',
           'noaa':'NOAA SED',
           'pde':'PDE',
           'pdecomcat':'PDE',
           'pdecomcat2':'PDE',
           'pdecomcat3':'PDE',
           'pdecomcat4':'PDE',
           'pdeisc':'PDE',
           'utsu':'Utsu',
           'emdat':'EM-DAT'}
           
                   
START = '1960-01-01';

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
    try:
        if locrow[3] is None:
            return None
    except:
        pass
    origindict = {}
    origindict['time'] = locrow[0]
    origindict['lat'] = locrow[1]
    origindict['lon'] = locrow[2]
    origindict['depth'] = locrow[3]
    origindict['source'] = SOURCES[loctable]
    return origindict

def getLoss(table,column,cursor,eid,type='loss'):
    query = 'SELECT %s FROM %s WHERE eid=%i' % (column,table,eid)
    cursor.execute(query)
    row = cursor.fetchone()
    if row is None:
        return None
    if row[0] is None:
        loss = 0
    else:
        loss = row[0]
    if table == 'other':
        query1 = 'SELECT source FROM other where eid=%i' % eid
        cursor.execute(query1)
        source = cursor.fetchone()[0]
    else:
        source = SOURCES[table]
    if type == 'loss':
        lossdict = {'loss':loss,'source':source}
    else:
        lossdict = {'effect':loss,'source':source}
    return lossdict
    

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
    magdict['source'] = SOURCES[magtable]
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
    aid = row[0]
    query2 = 'SELECT statuskey,statusvalue FROM atlas_status WHERE event_id=%i' % aid
    cursor.execute(query2)
    rows = cursor.fetchall()
    for row in rows:
        key = row[0]
        value = row[1]
        if key == 'status' and value != 'automatic':
            hasAtlas = True
            break
    return hasAtlas

def checkMagnitude(lat,lon,mag,pp):
    if mag >= 5.5:
        return True
    if mag >= 4.5 and pp.containsPoint(lon,lat):
        return True
    return False       

def checkExposure(cursor,eid):
    #get atlas_event id
    query1 = 'SELECT id FROM atlas_event WHERE eid=%i' % eid
    cursor.execute(query1)
    row = cursor.fetchone()
    aid = row[0]
    #get all the rows for this event in the exposure table, regardless of country
    query2 = 'SELECT exp5,exp6,exp7,exp8,exp9,exp10 FROM atlas_exposure WHERE event_id=%i' % aid
    cursor.execute(query2)
    rows = cursor.fetchall()
    expsum = 0
    for row in rows:
        expsum += sum(row)
    if expsum > 1000:
        return True
    return False

def checkReviewed(eid,cursor):
    query1 = 'SELECT id FROM atlas_event WHERE eid=%i' % eid
    cursor.execute(query1)
    row = cursor.fetchone()
    aid = row[0]
    query2 = 'SELECT statusvalue FROM atlas_status WHERE event_id=%i AND statuskey="status"' % aid
    cursor.execute(query2)
    row = cursor.fetchone()
    status = row[0]
    if status not in ['approved','reviewed']:
        return False
    return True

def main(argparser,args):
    zonefile = os.path.join(args.shakehome,'config','zone_config.conf')
    px,py = readShakeZone(zonefile)
    pp = poly.PagerPolygon(px,py)
    dbdict = getDataBaseConnections(args.shakehome)['atlas']
    db = mysql.connect(host='127.0.0.1',db=dbdict['database'],user=dbdict['user'],passwd=dbdict['password'],buffered=True)
    cursor = db.cursor()
    eventquery = 'SELECT a.id,a.ccode,a.lat,a.lon,a.magnitude,b.id,a.time FROM event a, atlas_event b WHERE a.id = b.eid and a.time > "%s" ORDER by a.time' % START
    cursor.execute(eventquery)
    eventrows = cursor.fetchall()
    ic = 0
    f = open('expocat.xml','wt')
    f.write('<expocat>\n')
    for eventrow in eventrows:
        eid = eventrow[0]
        ccode = eventrow[1]
        lat = eventrow[2]
        lon = eventrow[3]
        mag = eventrow[4]
        atlasid = eventrow[5]
        etime = eventrow[6]
        hasDamage = checkDamage(cursor,eid)
        hasAtlas = checkAtlas(cursor,eid)
        meetsMagnitude = checkMagnitude(lat,lon,mag,pp)
        meetsExposure = checkExposure(cursor,eid)
        if hasAtlas or hasDamage or meetsExposure:
            pass
        else:
            print 'Skipping event %s M%.1f' % (etime,mag)
            continue
        # if not hasDamage and not hasAtlas and not meetsMagnitude and not meetsExposure:
        #     continue
        print 'Extracting %s (%i of %i)' % (eid,ic,len(eventrows))
        ic += 1
        eventdict = {}
        eventdict['ccode'] = ccode
        for loctable in LOCHIERARCHY:
            if loctable == 'atlas_event':
                isReviewed = checkReviewed(eid,cursor)
                if not isReviewed:
                    continue
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
        if not eventdict.has_key('origins'):
            continue
        for magtable in MAGHIERARCHY:
            if magtable == 'atlas_event':
                isReviewed = checkReviewed(eid,cursor)
                if not isReviewed:
                    continue
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
                if lossdict is None:
                    continue
                if not eventdict.has_key(lossfield):
                    lossdict['preferred'] = True
                else:
                    lossdict['preferred'] = False
                if eventdict.has_key(lossfield):
                    eventdict[lossfield].append(lossdict)
                else:
                    eventdict[lossfield] = [lossdict]

        for effectfield in EFFECTS.keys():
            for tabletuple in EFFECTS[effectfield]:
                table,column = tabletuple
                effectdict = getLoss(table,column,cursor,eid,type='effect')
                if effectdict is None:
                    continue
                if not eventdict.has_key(effectfield):
                    effectdict['preferred'] = True
                else:
                    effectdict['preferred'] = False
                if eventdict.has_key(effectfield):
                    eventdict[effectfield].append(effectdict)
                else:
                    eventdict[effectfield] = [effectdict]

        # for table in DAMAGETABLES:
        #     damfields = DAMAGETABLES[table]
        #     for field in damfields:
        #         damdict

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
            expstr = [str(exp) for exp in exposure]
            fmt = '\t\t<exposure ccode="%s">%s</exposure>\n'
            f.write(fmt % (ccode,' '.join(expstr)))

        for lossfield in LOSSHIERARCHY.keys():
            losskey = lossfield
            if not eventdict.has_key(losskey):
                continue
            fmt = '\t\t<impact type="%s" source="%s" value="%i" preferred="%s"/>\n'
            for lossdict in eventdict[losskey]:
                try:
                    f.write(fmt % (losskey,lossdict['source'],lossdict['loss'],lossdict['preferred']))
                except:
                    pass
                
        #write out the effects (landslide, fire, etc.)
        for effectfield in EFFECTS.keys():
            effectkey = effectfield
            if not eventdict.has_key(effectkey):
                continue
            fmt = '\t\t<effect type="%s" source="%s" value="%i" preferred="%s"/>\n'
            for effectdict in eventdict[effectkey]:
                f.write(fmt % (effectkey,effectdict['source'],effectdict['effect'],effectdict['preferred']))
                
        f.write('\t</event>\n')
    f.write('</expocat>\n')
    f.close()
    cursor.close()
    db.close()

if __name__ == '__main__':
    description='Export Exposure Catalog information from PAGER-Cat database into XML format'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('shakehome', help='Specify location of ShakeMap home directory')
    arguments = parser.parse_args()
    main(parser,arguments)    
