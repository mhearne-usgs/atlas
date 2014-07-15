#!/usr/bin/env python

import zipfile
import os.path
import sys

if __name__ == '__main__':
    rootfolder = sys.argv[1]
    if rootfolder.endswith(os.sep):
        rootfolder = rootfolder[0:-1]
    p,f = os.path.split(rootfolder)
    zipname = '%s_grids.zip' % (f)
    myzip = zipfile.ZipFile(zipname,'w',zipfile.ZIP_DEFLATED)
    folders = os.listdir(rootfolder)
    ic = 1
    for tfolder in folders:
        print 'Reading %s (%i of %i)' % (tfolder,ic,len(folders))
        folder = os.path.join(rootfolder,tfolder)
        gridfile = os.path.join(folder,'output','grid.xml')
        errorfile = os.path.join(folder,'output','uncertainty.xml')
        if not os.path.isfile(gridfile):
            continue
        gridarcname = '%s_grid.xml' % tfolder
        errorarcname = '%s_error.xml' % tfolder
        myzip.write(gridfile,gridarcname)
        myzip.write(errorfile,errorarcname)
        ic += 1
    myzip.close()
