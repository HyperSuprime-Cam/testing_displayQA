#!/usr/bin/env python
# Original filename: bin/pipeQa.py
#
# Author: 
# Email: 
# Date: Mon 2011-04-11 13:11:01
# 
# Summary: 
# 

import sys
import re
import optparse
import os
import datetime

import numpy

import lsst.testing.displayQA         as dispQA
import lsst.testing.displayQA.figures as qaFig

#############################################################
#
# Main body of code
#
#############################################################

# This tutorial assumes you've read through dispQaTutorial1.py !

def main():


    # create a TestSet
    ts = dispQA.TestSet(group="tutorial", label="clickplot-howto", wwwCache=True)

    # Add some metadata
    ts.addMetadata("time-run", datetime.datetime.now().strftime("%a %Y-%m-%d %H:%M:%S"))


    # make some fake data, two clusters of stars in a 1024x1024 image
    nx, ny = 1024, 1024
    n = [100, 50]
    size = [200, 125]
    xcen = [300, 700]
    ycen = [300, 600]
    magPeak  = [24.0, 20.0]
    dmag = [2.0, 1.0]

    x = numpy.array([])
    y = numpy.array([])
    mag = []

    for i in range(len(n)):
        r      = size[i]*numpy.random.uniform(0.0, 1.0, n[i])**2
        theta  = numpy.random.uniform(0.0, 2.0*numpy.pi, n[i])
        
        x = numpy.append(x, xcen[i] - r*numpy.cos(theta))
        y = numpy.append(y, ycen[i] - r*numpy.sin(theta))
        mag.append(numpy.random.normal(magPeak[i], dmag[i], n[i]))
        

                  

    ############
    # make plots 

    # We'll make an plot showing x,y positions of the stars
    # We'll make histograms of each cluster, and add map 'areas' to the x,y plot
    #     ... clicking in the 'area' boxes will show the histogram for the cluster we clicked on

    qafigXy = qaFig.QaFigure() # this will be the x,y plot

    
    for i in range(len(n)):

        # make the histogram plot
        qafigHist = qaFig.QaFigure()
        fig = qafigHist.fig
        ax = fig.add_subplot(111)
        ax.hist(mag[i])

        # add a map area around the cluster of points

        # create a unique id 'areaLabel' for this area
        # areaLabel must be passed to:
        #  -- any Test you want displayed when this area is active (it can be a prefix/suffix etc)
        #  -- and any QaFigure which is to be displayed when this map area is clicked (prefix/suffix ok)
        areaLabel = "cluster%04d" % (i)
        x0, y0, x1, y1 = xcen[i]-size[i], ycen[i]-size[i], xcen[i]+size[i], ycen[i]+size[i]
        area      = [x0, y0, x1, y1]    # llc, urc of clickable region
        areaInfo  = "n=%d"%(n[i])       # text to be displayed on mouse-over

        # add the map area to the x,y figure
        qafigXy.addMapArea(areaLabel, area, areaInfo)


        # add the figure - notice areaLabel is added as a part of the filename
        ts.addFigure(qafigHist, "hist.png", "Histogram of stars in cluster %d" % (i), areaLabel=areaLabel)

        # add a test - notice the 'areaLabel' is part of the Test label
        ts.addTest("count", n[i], [0, None], "Verify > 0 stars in cluster.", areaLabel=areaLabel)


    # now that we have the areas defined, make the x,y figure
    fig = qafigXy.fig
    ax = fig.add_subplot(111)
    ax.plot(x, y, "r.")
    ax.set_xlim([0, nx])
    ax.set_ylim([0, ny])

    navMap = True # figure appears in right panel, map areas are links (not just mouse-over tooltips)
    ts.addFigure(qafigXy, "stars%d.png" % (i), "Stars in cluster %d" % (i), navMap=navMap)


    # make a histogram of *all* stars
    # - this will be displayed if no 'area' region is active, or if you click 'all'
    #  --> you can skip this figure ... then no image is displayed unless a map region is selected
    qafigHistAll = qaFig.QaFigure()
    fig = qafigHistAll.fig
    ax = fig.add_subplot(111)
    ax.hist(numpy.concatenate(mag))
    ts.addFigure(qafigHistAll, "hist.png", "Histogram of all stars", areaLabel="all")




if __name__ == '__main__':
    main()
    
