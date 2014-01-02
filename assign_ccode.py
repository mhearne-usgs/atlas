#!/usr/bin/env python

#stdlib
import os.path
import sys

#third party
from pagerio import esri
import MySQLdb as mysql
import numpy as np
from pagermap import country

if __name__ == '__main__':
    ccodegrid = sys.argv[1]
    connection = mysql.connect(db='atlas',user='atlas',passwd='atlas')
    cursor = connection.cursor()
    query = 'SELECT id,lat,lon FROM event WHERE ccode is NULL'
    cursor.execute(query)
    rows = cursor.fetchall()
    lat = []
    lon = []
    eid = []
    for row in rows:
        eid.append(row[0])
        lat.append(row[1])
        lon.append(row[2])
    eid = np.array(eid)
    lat = np.array(lat)
    lon = np.array(lon)
    #group everything into 10 chunks
    ymin = lat.min()
    ymax = lat.max()
    xmin = lon.min()
    xmax = lon.max()

    ydim = (ymax-ymin)/10.0
    xdim = (xmax-xmin)/10.0

    #instantiate ccode grid
    isogrid = esri.EsriGrid(ccodegrid)
    
    left = xmin
    top = ymax
    for i in range(0,10):
        left = xmin
        bottom = top - ydim
        for j in range(0,10):
            right = left + xdim
            print 'Processing grid chunk %i, %i' % (i,j)
            isogrid.load(bounds=(left,right,bottom,top))
            idxlat = (lat >= bottom) & (lat < top)
            idxlon = (lon >= left) & (lon < right)
            idx = np.intersect1d(idxlat.nonzero()[0],idxlon.nonzero()[0])
            gridxmin,gridxmax,gridymin,gridymax = isogrid.getRange()
            if len(idx):
                boxlat = lat[idx]
                boxlon = lon[idx]
                boxeid = eid[idx]
                outsidex = ((boxlon < gridxmin) | (boxlon > gridxmax)).nonzero()[0]
                outsidey = ((boxlat < gridymin) | (boxlat > gridymax)).nonzero()[0]
                outside = np.union1d(outsidex,outsidey)
                boxlon[outside] = np.mean([gridxmin,gridxmax])
                boxlat[outside] = np.mean([gridymin,gridymax])
                ccodes = isogrid.getValue(boxlat,boxlon)
                ccodes[outside] = 0
                for idx1 in range(0,len(ccodes)):
                    numcode = int(ccodes[idx1])
                    eid1 = boxeid[idx1]
                    ccode = country.getCountryCode(numcode)['alpha2']
                    query = 'UPDATE event set ccode="%s" WHERE id=%i' % (ccode,eid1)
                    cursor.execute(query)
                    connection.commit()
            left += xdim
        top -= ydim
    
    cursor.close()
    connection.close()

