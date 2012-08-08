import os, sys, re, glob

import numpy
import numpy.ma as numpyMa

import matplotlib
#matplotlib.use('TkAgg')

import matplotlib.figure as figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigCanvas
from matplotlib.font_manager import FontProperties
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
from matplotlib.patches import Ellipse
from matplotlib import cm
from matplotlib import colors


class QaFigure(object):
    """A wrapper for a matplotlib figure, and a Baseclass for more complicated QA figures. """

    count = 0
    
    def __init__(self, size=(4.0, 4.0), dpi=100): # (512, 512), DPI=100):
        """
        @param size  Figure size in inches
        @param dpi   Dots per inch to use.
        """

        self.fig         = figure.Figure(figsize=size)
        self.fig.set_dpi(dpi)
        self.canvas      = FigCanvas(self.fig)
        self.map         = {}
        #self.fig.set_size_inches(size[0] / DPI, size[1] / DPI)
        self.mapAreas    = []
        self.mapTransformed = True

        QaFigure.count += 1
        
    def reset(self):
        self.fig.clf()

    def validate(self):
        pass
    
    def makeFigure(self):
        # override
        pass

    def getFigure(self):
        return self.fig


    def savefig(self, path, **kwargs):
        """Save figure."""
        olderr = numpy.seterr(all="warn")
        self.fig.savefig(path, dpi=self.fig.get_dpi(), **kwargs)
        numpy.seterr(**olderr)

    def savemap(self, path):
        """Save internal map area data to .map file. """
        
        if self.mapTransformed:
            mapList = self.getMapInfo()
        else:
            mapList = self.getTransformedMap()
            
        fp = open(path, 'w')

        if len(mapList) > 0:
            # don't include overplotted map areas
            n = 100
            xpmax, ypmax = self.fig.transFigure.transform((1.0, 1.0))
            haveLookup = numpy.zeros([n, n])
            for array in mapList:

                label, x0, y0, x1, y1, info = array
                ix = int(n*0.5*(x0 + x1)/xpmax)
                iy = int(n*0.5*(y0 + y1)/ypmax)
                ixOk = ix >= 0 and ix < n
                iyOk = iy >= 0 and iy < n
                if ixOk and iyOk and haveLookup[ix,iy] == 0:
                    fp.write("%s %d %d %d %d %s\n" % (label, x0, y0, x1, y1, info))
                    haveLookup[ix,iy] = 1
                    
        fp.close()


    def getTransformedMap(self):
        """Take plot coordinates for map areas and convert to figure coordinates."""

        xpmax, ypmax = self.fig.transFigure.transform((1.0, 1.0))
        
        mapAreasNew = []
        for ma in self.mapAreas:
            label, x0, y0, x1, y1, info, axes = ma
            left, bottom = axes.transData.transform((x0, y0))
            right, top = axes.transData.transform((x1, y1))
            mapAreasNew.append([label, left, ypmax-top, right, ypmax-bottom, info])
            
        return mapAreasNew
            

    def addMapArea(self, label, area, info, axes=None):
        """Add a map area to this figure.

        @param label    an areaLabel to associate the area to other QaFigures or Tests.
        @param area     [x0, y0, x1, y1]  llc, urc corners of the area in plot coordinates.
        @param info     string to show no mouseover (no whitespace)
        @param axes     axes to use for map transformation
        """
        
        if axes is None:
            axes = self.fig.gca()
        x0, y0, x1, y1 = area
        self.mapAreas.append([label, x0, y0, x1, y1, info, axes])
        self.mapTransformed = False

    
    def getMapInfo(self):
        return self.mapAreas


