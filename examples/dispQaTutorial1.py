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

def main():

    ############################
    # create a TestSet
    
    # assign it to a 'group', and give it a 'label'
    # - 'groups' are used to categorize tests
    #   --> eg. LSST visits; biases,flats,arcs; or in this case, 'tutorial'
    #   --> the groups used are linked from the display home page.
    # - Each TestSet is labeled (and listed) according to script name, by default,
    #   --> 'label="mylabel"' appends .mylabel to a TestSet label
    #   --> This allows multiple TestSets to be created in a single script.
    ts = dispQA.TestSet(group="tutorial", label="basic-howto", wwwCache=True)


    #############################
    # Adding metadata

    # - These values will be posted in the upper right of the display
    date = datetime.datetime.now().strftime("%a %Y-%m-%d %H:%M:%S")
    ts.addMetadata("time-run", date)
    ts.addMetadata("my-important-param", "important-value")


    #############################
    # Adding Tests


    #########
    # test 1
    # create some values we'd like to test, along with the range we consider acceptable
    val1label        = "v1-label"    # a label to refer to this test/value (no spaces or underscores)
    importantVal1    = 1.0           # the value to test
    acceptableRange1 = [0.0, 2.0]    # the range (inclusive) considered a 'pass'
    comment1         = "A very important value!"  # additional info about this value

    # make the Test, and add it to the TestSet
    test1 = dispQA.Test(val1label, importantVal1, acceptableRange1, comment1)
    ts.addTest(test1)

    #########
    # test 2
    # - be a bit more concise this time
    importantVal2 = 5.0
    acceptableRange2 = [12.0, None]  # 'None' means unlimited
    ts.addTest(dispQA.Test("v2-label", importantVal2, acceptableRange2, "Fairly important stuff"))

    #########
    # test 3
    # - note that addTest() actually accepts test parameters directly
    ts.addTest("v3-label", 99.0, [98.0, 100], "the great one!")
    


    ##############################
    # Adding an matplotlib Figures

    ts = dispQA.TestSet(group="tutorial", label="basic-howto")

    # Get a matplotlib 'Figure' object with QaFigure()
    # - this just handles instantiation, and canvas creation
    # - you could create a regular Figure in matplotlib
    qafig = qaFig.QaFigure()
    fig = qafig.fig
    ax = fig.add_subplot(111)
    n = 100
    x = 2.0*numpy.pi*numpy.arange(n)/n
    y = numpy.sin(x)
    ax.plot(x, y, "r-")

    figName = "sineWave.png"  # no path here ... the TestSet knows where to write it
    caption = "A sine wave from 0 to 2*pi."
    ts.addFigure(fig, figName, caption)





if __name__ == '__main__':
    main()
    
