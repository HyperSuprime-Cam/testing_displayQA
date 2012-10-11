import sys
import traceback
import os
import re
import glob
import inspect
import stat
import shutil
import eups
import sqlite as sqlite
#import apsw
import errno
import cPickle as pickle
import shelve

import LogConverter as logConv

import numpy
BAD_VALUE = -99

useApsw = False


def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


class TestFailError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)
    
    
class Test(object):
    """A class to verify some condition is met.
    """

    def __init__(self, label, value, limits, comment, areaLabel=None):
        """
        @param label      A name for this test
        @param value      Value to be tested
        @param limits     A list [min, max] specifying range of acceptable values (inclusive).
        @param comment    A comment with extra info about the test
        @param areaLabel  [optional] Label associating this test with a mapped area in a figure.
        """
        
        self.label = label
        if not areaLabel is None:
            self.label += " -*- "+areaLabel

        self.limits = limits
        if value is None or numpy.isnan(value):
            self.value = BAD_VALUE
            if self.evaluate() != False:
                # -99 is actually within the window of good values; keep as NaN for now
                self.value = value
        else:
            self.value = value
            
        self.comment = comment

    def __str__(self):
        return self.label+" "+str(self.evaluate())+" value="+str(self.value)+" limits="+str(self.limits)

    def evaluate(self):
        """Verify that our value is within our limits."""
        
        # grab a traceback for failed tests
        if (not self.limits[0] is None) and (not self.limits[1] is None):
            if (self.value < self.limits[0] or self.value > self.limits[1]):
                return False
            else:
                return True
        elif (self.limits[0] is None):
            if self.value > self.limits[1]:
                return False
            else:
                return True
        elif (self.limits[1] is None):
            if self.value < self.limits[0]:
                return False
            else:
                return True
        else:
            return True


class TestSet(object):
    """A container for Test objects and associated matplotlib figures."""
    
    def __init__(self, label=None, group="", clean=False, useCache=False, alias=None, wwwCache=False):
        """
        @param label  A name for this testSet
        @param group  A category this testSet belongs to
        """

        self.conn = None
        
        missing = []
        for env in ["WWW_ROOT", "WWW_RERUN"]:
            if not os.environ.has_key(env):
                missing.append(env)
        if len(missing) > 0:
            raise Exception("Must set environment variable:\n", "\n".join(missing))


        self.useCache = useCache
        self.wwwCache = wwwCache

        wwwRootDir = os.environ['WWW_ROOT']
        qaRerun = os.environ['WWW_RERUN']
        self.wwwBase = os.path.join(wwwRootDir, qaRerun)
        testfileName = inspect.stack()[-1][1]
        if alias is None:
            self.testfileBase = re.sub(".py", "", os.path.split(testfileName)[1])
        else:
            self.testfileBase = alias
            
        prefix = "test_"+group+"_"
        self.testDir = prefix+self.testfileBase
        if not label is None:
            self.testDir += "."+label
        self.wwwDir = os.path.join(self.wwwBase, self.testDir)

        if clean and os.path.exists(self.wwwDir):
            shutil.rmtree(self.wwwDir)

        if not os.path.exists(self.wwwDir):
            try:
                os.mkdir(self.wwwDir)
                os.chmod(self.wwwDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            except os.error, e:  # as exc: # Python >2.5
                if e.errno != errno.EEXIST:
                    raise


        # connect to the db and create the tables
        self.connect()
        #self.dbFile = os.path.join(self.wwwDir, "db.sqlite3")
        #self.conn = sqlite.connect(self.dbFile)
        #self.curs = self.conn.cursor()
        self.summTable, self.figTable, self.metaTable, self.eupsTable = \
                        "summary", "figure", "metadata", "eups"
        self.tables = {
            self.summTable : ["label text unique", "value double",
                              "lowerlimit double", "upperlimit double", "comment text",
                              "backtrace text"],
            self.figTable  : ["filename text", "caption text"],
            self.metaTable : ["key text", "value text"],
            }

        self.stdKeys = ["id integer primary key autoincrement", "entrytime timestamp DEFAULT (strftime('%s','now'))"]
        for k, v in self.tables.items():
            keys = self.stdKeys + v
            cmd = "create table if not exists " + k + " ("+",".join(keys)+")"
            self.curs.execute(cmd)

        self.conn.commit()
        self.close()

        
        self.tests = []

        # create the cache table
        if self.wwwCache:
            self.countsTable = "counts"
            self.failuresTable = "failures"
            self.allFigTable = "allfigures"
            self.cacheTables = {
                self.countsTable : ["test text", "ntest integer", "npass integer", "dataset text",
                                   "oldest timestamp", "newest timestamp", "extras text"],
                self.failuresTable : ["testandlabel text unique", "value double",
                                      "lowerlimit double", "upperlimit double", "comment text"],
                self.allFigTable : ["path text", "caption text"],
                }
            
            self.countKeys = self.cacheTables[self.countsTable]
            self.failureKeys = self.cacheTables[self.failuresTable]
            
            for k,v in self.cacheTables.items():
                keys = self.stdKeys + v
                cmd = "create table if not exists " + k + " ("+",".join(keys)+")"
                curs = self.cacheConnect()
                curs.execute(cmd)
                self.cacheClose()


    def connect(self):
        self.dbFile = os.path.join(self.wwwDir, "db.sqlite3")
        if useApsw:
            self.conn = apsw.Connetion(self.dbFile)
        else:
            self.conn = sqlite.connect(self.dbFile)
        self.curs = self.conn.cursor()
        return self.curs

    def close(self):
        if not self.conn is None:
            self.conn.close()
        

    def cacheConnect(self):
        self.cacheDbFile = os.path.join(self.wwwBase, "db.sqlite3")
        if useApsw:
            self.cacheConn = apsw.Connection(self.cacheDbFile)
        else: 
            self.cacheConn = sqlite.connect(self.cacheDbFile)
        self.cacheCurs = self.cacheConn.cursor()
        return self.cacheCurs

    def cacheClose(self):
        if not self.cacheConn is None:
            self.cacheConn.close()
            

        
    def __del__(self):
        if not self.conn is None:
            self.conn.close()


    #########################################
    # routines to handle caching data
    #########################################
    def setUseCache(self, useCache):
        self.useCache = useCache

    def pickle(self, label, data):
        if self.useCache:
            filename = os.path.join(self.wwwDir, label+".pickle")
            fp = open(filename, 'w')
            pickle.dump(data, fp)
            fp.close()

    def unpickle(self, label, default=None):
        data = default
        if self.useCache:
            filename = os.path.join(self.wwwDir, label+".pickle")
            if os.path.exists(filename):
                fp = open(filename)
                data = pickle.load(fp)
                fp.close()
        return data


    def shelve(self, label, dataDict, useCache=None):
        if useCache is None:
            useCache = self.useCache
        if useCache:
            filename = os.path.join(self.wwwDir, label+".shelve")
            shelf = shelve.open(filename)
            for k,v in dataDict.items():
                shelf[k] = v
            shelf.close()
            
    def unshelve(self, label):
        data = {}
        if self.useCache:
            filename = os.path.join(self.wwwDir, label+".shelve")
            try:
                shelf = shelve.open(filename)
                for k,v in shelf.items():
                    data[k] = v
                shelf.close()
            except:
                pass
        return data

            

    def _verifyTest(self, value, lo, hi):

        if not value is None:
            value = float(value)
        if not lo is None:
            lo = float(lo)
        if not hi is None:
            hi = float(hi)
    
        cmp = 0   #true;  # default true (ie. no limits were set)
        if ((not lo is None) and (not hi is None)):
            if (value < lo):
                cmp = -1
            elif (value > hi):
                cmp = 1
        elif ((not lo is None) and (hi is None) and (value < lo)):
            cmp = -1
        elif ((lo is None) and (not hi is None) and (value > hi)):
            cmp = 1
        return cmp


    def _readCounts(self):
        sql = "select label,entrytime,value,lowerlimit,upperlimit from summary"
        self.connect()

        if useApsw:
            results = self.curs.execute(sql)
        else:
            self.curs.execute(sql)
            results = self.curs.fetchall()
        self.close()

        # key: [regex, displaylabel, units, values]
        extras = {
            'fwhm' : [".*fwhm.*",                    "fwhm",            "[&Prime;] (FWHM)",           []],
            'r50'  : [".*median astrometry error.*", "r<sub>50</sub>",  "[&Prime;] (Ast.error)", []],
            'std'  : [".*stdev psf_vs_cat.*",        "&sigma;<sub>phot</sub>", "[mag] (psf-cat)",  []],
            'comp' : [".*photometric depth.*",       "&omega;<sub>50</sub>", "[mag] (Completeness)", []],
            "nccd" : [".*nCcd.*",                    "n<sub>CCD</sub>",      "(num. CCDs proc.)",     []],
            "nstar": [".*nDet.*",                    "n<sub>*</sub>",        "(num. Detections)",  []],
            #'zero' : [".*median zeropoint.*",        "ZP",               "[mag] (Zeropoint)",        []],
            }
        
        # count the passed tests
        npass = 0
        ntest = 0
        oldest = 1e12
        newest = 0
        for r in results:
            ntest += 1
            label = r[0]
            entrytime = r[1]
            vlu = r[2:]
            cmp = self._verifyTest(*vlu)
            if cmp == 0:
                npass += 1
            if entrytime < oldest:
                oldest = entrytime
            if entrytime > newest:
                newest = entrytime

            for k, v in extras.items():
                reg, displabel, units, values = v
                if re.search(reg, label) and not re.search("^99\.", str(vlu[0])):
                    extras[k][3].append(vlu[0])

        # encode any extras
        extraStr = ""
        extraValues = []
        for k,v in extras.items():
            if len(v[3]) > 0:
                extraValues.append("%s:%.2f:%.2f:%s" % (v[1], numpy.mean(v[3]), numpy.std(v[3]), v[2]))
        extraStr = ",".join(extraValues)
        
        # get the dataset from the metadata
        sql = "select key,value from metadata"
        self.connect()
        if useApsw:
            metaresults = self.curs.execute(sql)
        else:
            self.curs.execute(sql)
            metaresults = self.curs.fetchall()
            
        self.close()
        
        dataset = "unknown"
        for m in metaresults:
            k, v = m
            if k == 'dataset':
                dataset = v
        
        return ntest, npass, dataset, oldest, newest, extraStr


    def _writeCounts(self, ntest, npass, dataset="unknown", oldest=None, newest=None, extras=""):
        """Cache summary info for this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        curs = self.cacheConnect()
        keys = [x.split()[0] for x in self.countKeys]
        replacements = dict( zip(keys, [self.testDir, ntest, npass, dataset, oldest, newest, extras]))
        self._insertOrUpdate(self.countsTable, replacements, ['test'], cache=True)
        self.cacheClose()


    def _writeFailure(self, label, value, lo, hi, overwrite=True):
        """Cache failure info for this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        #curs = self.cacheConnect()
        keys = [x.split()[0] for x in self.failureKeys]
        testandlabel = self.testDir + "QQQ" + str(label)
        replacements = dict( zip(keys, [testandlabel, value, lo, hi]))
        if overwrite:
            self._insertOrUpdate(self.failuresTable, replacements, ['testandlabel'], cache=True)
        else:
            self._pureInsert(self.failuresTable, replacements, ['testandlabel'], cache=True)
        #self.cacheClose()

        
    def _insertOrUpdate(self, table, replacements, selectKeys, cache=False):
        """Insert entries into a database table, overwrite if they already exist."""
        
        # there must be a better sql way to do this ... but my sql-foo is weak
        # we want to overwrite entries if they exist, or insert them if they don't
        
        # delete the rows which match the selectKeys
        if True:
            where = []
            for key in selectKeys:
                if isinstance(replacements[key], str):
                    where.append(key + "='" + replacements[key] + "'")
                else:
                    where.append(key + "=" + str(replacements[key]))
            where = " where " + " and ".join(where)

            cmd = "delete from " + table + " " + where


            if not cache:
                self.connect()

            if not cache:
                self.curs.execute(cmd)
            else:
                self.cacheCurs.execute(cmd)


            # insert the new data
            keys = []
            values = []
            for k,v in replacements.items():
                keys.append(k)
                values.append(v)
            values = tuple(values)
            inlist = " (id, entrytime,"+ ",".join(keys) + ") "
            qmark = " (NULL, strftime('%s', 'now')," + ",".join("?"*len(values)) + ")"
            cmd = "insert into "+table+inlist + " values " + qmark
        else:
            # insert the new data
            keys = []
            values = []
            for k,v in replacements.items():
                keys.append(k)
                values.append(v)
            values = tuple(values)
            inlist = " ("+ ",".join(keys) + ") "
            qmark = " ("+ ",".join("?"*len(values)) + ")"
            cmd = "replace into "+table+inlist + " values " + qmark
            
            #print cmd, values
            if not cache:
                self.connect()
        if not cache:
            self.curs.execute(cmd, values)
            if not useApsw:
                self.conn.commit()
        else:
            self.cacheCurs.execute(cmd, values)
            if not useApsw:
                self.cacheConn.commit()

        if not cache:
            self.close()
            

    def _pureInsert(self, table, replacements, selectKeys, cache=False):
        """Insert entries into a database table, overwrite if they already exist."""
        
        # insert the new data
        keys = []
        values = []
        for k,v in replacements.items():
            keys.append(k)
            values.append(v)
        values = tuple(values)
        inlist = " (id, entrytime,"+ ",".join(keys) + ") "
        qmark = " (NULL, strftime('%s', 'now')," + ",".join("?"*len(values)) + ")"
        cmd = "insert into "+table+inlist + " values " + qmark

        if not cache:
            self.curs.execute(cmd, values)
            if not useApsw:
                self.conn.commit()
        else:
            self.cacheCurs.execute(cmd, values)
            if not useApsw:
                self.cacheConn.commit()


    def addTests(self, testList):

        for test in testList:
            self.addTest(test)
            

    def updateFailures(self, overwrite=True):

        if self.wwwCache:

            # load the summary
            sql = "select label,entrytime,value,lowerlimit,upperlimit from summary"
            self.connect()
            if useApsw:
                results = self.curs.execute(sql)
            else:
                self.curs.execute(sql)
                results = self.curs.fetchall()
            self.close()
            
            # write failures
            failSet = []
            curs = self.cacheConnect()            
            for r in results:
                label, etime, value, lo, hi = r
                if re.search("-*-", label):
                    labelsplit = label.split("-*-")
                    tag = labelsplit[1].strip()
                    failSet.append(tag)
                    cmp = self._verifyTest(value, lo, hi)
                    if cmp:
                        self._writeFailure(str(label), value, lo, hi, overwrite)
            self.cacheClose()

            failSet = set(failSet)
            
            # load the figures
            sql = "select filename from figure"
            self.connect()
            if useApsw:
                figures = self.curs.execute(sql)
            else:
                self.curs.execute(sql)
                figures = self.curs.fetchall()
            self.close()

            # write allfigtable
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            curs = self.cacheConnect()
            for f in figures:
                filename, = f
                filebase = re.sub(".png", "", filename)
                if re.search("[^-]-[^-]", filebase):
                    fileqqq = re.sub("([^-])-([^-])", r"\1QQQ\2", filebase)
                    tag = fileqqq.split("QQQ")[-1]
                    tag = tag.strip()
                    if tag in failSet:
                        #print filebase
                        path = str(os.path.join(self.wwwDir, filename))
                        replacements = dict( zip(keys, [path, ""]))
                        if overwrite:
                            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
                        else:
                            self._pureInsert(self.allFigTable, replacements, ['path'], cache=True)

            self.cacheClose()
    

    def updateCounts(self, dataset=None, increment=[0,0]):

        if self.wwwCache:
            ntest, npass = increment
            ntestOrig, npassOrig, datasetOrig, oldest, newest, extras = self._readCounts()

            ntest = int(ntestOrig) + ntest
            npass = int(npassOrig) + npass
            if dataset is None:
                dataset = datasetOrig

            self._writeCounts(ntest, npass, dataset, oldest, newest, extras)

            # return the new settings
            return ntest, npass, dataset, oldest, newest, extras

        
    def addTest(self, *args, **kwargs):
        """Add a test to this testing suite.

        @param *args  Either a Test object, or the arguments to create one
        """

        if len(args) >= 4:
            label, val, limits, comment = args
            test = Test(label, val, limits, comment, areaLabel=kwargs.get('areaLabel', None))
        elif len(args) == 1:
            test, = args

        self.tests.append(test)

        #cache the results
        passed = test.evaluate()
        npassed = 1 if passed else 0
        
        # grab a traceback for failed tests
        backtrace = ""
        try:
            if not passed:
                if self.wwwCache:
                    curs = self.cacheConnect()
                    self._writeFailure(test.label, test.value, test.limits[0], test.limits[1])
                    self.cacheClose()
                raise TestFailError("Failed test '"+str(test.label)+"': " +
                                        "value '" + str(test.value) + "' not in range '" +
                                        str(test.limits)+"'.")
        except TestFailError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            backtrace = "".join(traceback.format_stack()[:-1]) + "\n" + str(e)
            
        if kwargs.has_key('backtrace'):
            backtrace = kwargs['backtrace']
            
        # enter the test in the db
        keys = [x.split()[0] for x in self.tables[self.summTable]]
        replacements = dict( zip(keys, [test.label, test.value, test.limits[0], test.limits[1], test.comment,
                                        backtrace]) )
        self._insertOrUpdate(self.summTable, replacements, ['label'])

        self.updateCounts() #increment=[1, npassed])



    def addMetadata(self, *args):
        """Associate metadata with this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        def addOneKvPair(k, v):
            keys = [x.split()[0] for x in self.tables[self.metaTable]]
            replacements = dict( zip(keys, [k, v]))
            self._insertOrUpdate(self.metaTable, replacements, ['key'])
            
        if len(args) == 1:
            kvDict, = args
            for k, v in kvDict.items():
                addOneKvPair(k, v)
        elif len(args) == 2:
            k, v = args
            addOneKvPair(k, v)
        else:
            raise Exception("Metadata must be either dict (1 arg) or key,value pair (2 args).")


        
    def importExceptionDict(self, exceptDict):
        """Given a dictionary of exceptions from TestData object, add the entries to the db."""

        keys = sorted(exceptDict.keys())
        for key in keys:
            tablekeys = [x.split()[0] for x in self.tables[self.summTable]]
            replacements = dict( zip(tablekeys, [key, 0, 1, 1, "Uncaught exception", exceptDict[key]]) )
            self._insertOrUpdate(self.summTable, replacements, ['label'])

            
    def _writeWrapperScript(self, pymodule, figname, plotargs, pythonpath=""):
        pyscript = re.sub(".pyc$", ".py", pymodule.__file__)
        pypath, pyfile = os.path.split(pyscript)
        
        
        s = ""
        fig_path = os.path.join(self.wwwDir, figname)
        sh_wrapper = fig_path + ".sh"

        fig_path = os.path.join(self.testDir, figname)
        
        if plotargs is None:
           plotargs = "" 

        enviro_path = os.path.join(self.wwwBase, "environment.php")
        if not os.path.exists(enviro_path) or os.stat(enviro_path).st_size == 0:
            fp = open(enviro_path, 'w')
            s  = "<?php\n"
            s += "$qa_environment = array(\n"
            s += "   'MPLCONFIGDIR' => '%s',\n" % (os.path.join(os.getenv('WWW_ROOT'), ".matplotlib"))
            s += "   'PATH' => '%s:%s',\n" % (os.getenv('PATH'), pypath)
            s += "   'PYTHONPATH' => '%s',\n" % (os.getenv('PYTHONPATH'))
            s += "   'LD_LIBRARY_PATH' => '%s'\n" % (os.getenv('LD_LIBRARY_PATH'))
            s += "   );\n"
            fp.write(s)
            fp.close()
            
        fp = open(sh_wrapper, 'w')
        s = "#!/usr/bin/env bash\n"
        s += pyfile + " " + fig_path + " " + plotargs + "\n"
        fp.write(s)
        fp.close()
        os.chmod(sh_wrapper, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)


        
    def cacheLazyData(self, dataDict, filename, toggle=None, areaLabel=None,
                      masterToggle=None):
        """ """

        cacheName = filename
        if not toggle is None:
            filename = re.sub("(\.\w{3})$", r"."+toggle+r"\1", filename)
            if masterToggle  and  toggle != masterToggle:
                cacheName = re.sub("(\.\w{3})$", r"."+masterToggle+r"\1", cacheName)
        if not areaLabel is None:
            filename = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", filename)
            cacheName = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", cacheName)

        path = os.path.join(self.wwwDir, filename)
        cachePath = os.path.join(self.wwwDir, cacheName)
            
        if masterToggle is None  or  toggle == masterToggle:
            self.shelve(filename, dataDict, useCache=True)
        else:
            for f in glob.glob(cachePath+".shelve*"):
                shelfLink = re.sub(masterToggle, toggle, f)
                if not os.path.exists(shelfLink):
                    os.symlink(f, shelfLink)
        

                
    def addLazyFigure(self, dataDict, filename, caption, pymodule,
                      plotargs=None, toggle=None, areaLabel=None, pythonpath="", masterToggle=None):
        """Add a figure to this test suite.
        """

        
        cacheName = filename
        if not toggle is None:
            filename = re.sub("(\.\w{3})$", r"."+toggle+r"\1", filename)
            if masterToggle  and  toggle != masterToggle:
                cacheName = re.sub("(\.\w{3})$", r"."+masterToggle+r"\1", cacheName)
        if not areaLabel is None:
            filename = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", filename)
            cacheName = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", cacheName)
            
        path = os.path.join(self.wwwDir, filename)
        cachePath = os.path.join(self.wwwDir, cacheName)
        
        # shelve the data
        if areaLabel != 'all':
            # if there's no masterToggle, or if this toggle is the master ... cache
            if masterToggle is None  or  toggle == masterToggle:
                self.shelve(filename, dataDict, useCache=True)
            else:
                for f in glob.glob(cachePath+".shelve*"):
                    shelfLink = re.sub(masterToggle, toggle, f)
                    if not os.path.exists(shelfLink):
                        os.symlink(f, shelfLink)
        
        # create an empty file
        fp = open(path, 'w')
        fp.close()
        os.chmod(path,
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
                 
        
        # write the script to generate the real figure
        self._writeWrapperScript(pymodule, filename, plotargs, pythonpath)

        
        keys = [x.split()[0] for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename'])

        if self.wwwCache:
            curs = self.cacheConnect()
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
            self.cacheClose()
        

            
    def addFigure(self, fig, basename, caption, areaLabel=None, toggle=None, navMap=False):
        """Add a figure to this test suite.
        
        @param fig      a matplotlib figure
        @param filename The basename of the figure.
        @param caption  text describing the figure
        @param areaLabel a string associating the figure with a map area in a navigation figure
        @param navMap    Identify this figure as a navigation map figure containing linked map areas.
        """


        # sub in the areaLabel, if given
        filename = basename
        if not toggle is None:
            filename = re.sub("(\.\w{3})$", r"."+toggle+r"\1", filename)
        if not areaLabel is None:
            filename = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", filename)

        path = os.path.join(self.wwwDir, filename)

        fig.savefig(path)

        if hasattr(fig, "mapAreas") and len(fig.mapAreas) > 0:
            suffix = ".map"
            if navMap:
                suffix = ".navmap"
            mapPath = re.sub("\.\w{3}$", suffix, path)
            fig.savemap(mapPath)
        
        keys = [x.split()[0] for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename'])

        if self.wwwCache:
            curs = self.cacheConnect()
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
            self.cacheClose()


    def addFigureFile(self, basename, caption, areaLabel=None, toggle=None, navMap=False):
        """Add a figure to this test suite.
        
        @param filename The basename of the figure.
        @param caption  text describing the figure
        @param areaLabel a string associating the figure with a map area in a navigation figure
        @param navMap    Identify this figure as a navigation map figure containing linked map areas.
        """

        orig_path, orig_file = os.path.split(basename)
        
        # sub in the areaLabel, if given
        filename = orig_file
        if not toggle is None:
            filename = re.sub("(\.\w{3})$", r"."+toggle+r"\1", filename)
        if not areaLabel is None:
            filename = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", filename)

        path = os.path.join(self.wwwDir, filename)

        #os.symlink(basename, path)
        shutil.copyfile(basename, path)

        convert = which("convert")
        size = "200x200"
        if convert:
            os.system(convert + " " + path + " -resize "+size+" " + re.sub(".png$", "Thumb.png", path))
            
        
        keys = [x.split()[0] for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename'])

        if self.wwwCache:
            curs = self.cacheConnect()
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
            self.cacheClose()

            
    def importLogs(self, logFiles):
        """Import logs from logFiles output by pipette."""
        
        # convert our ascii logfile to a sqlite3 database    
        def importLog(logFile):
            base = os.path.basename(logFile)
            table = "log_" + re.sub(".log", "", base)
            converter = logConv.LogFileConverter(logFile)
            converter.writeSqlite3Table(self.dbFile, table)

        # allow a list of filenames to be provided, or just a single filename
        if isinstance(logFiles, list):
            for logFile in logFiles:
                importLog(logFile)
        else:
            importLog(logFile)

            
    def importEupsSetups(self, eupsSetupFiles):
        """Import the EUPS setup packages from files written by TestData object during pipette run."""

        # note that this only works if we ran the test ourselves.

        def importEups(eupsFile):
            base = os.path.basename(eupsFile)
            table = "eups_" + re.sub(".eups", "", base)
            setups = []
            fp = open(eupsFile, 'r')
            for line in fp.readlines():
                setups.append(line.split())
            fp.close()

            mykeys = ["product text", "version text"]
            keys = self.stdKeys + mykeys

            cmd = "create table if not exists " + table + " ("+",".join(keys)+")"
            self.connect()
            if useApsw:
                self.curs.execute(cmd)
            else:
                self.curs.execute(cmd)
                self.conn.commit()
            self.close()

            for setup in setups:
                product, version = setup
                mykeys = [x.split()[0] for x in mykeys]
                replacements = dict( zip(mykeys, [product, version]))
                self._insertOrUpdate(table, replacements, ['product'])
            
        # allow a list of files or just one
        if isinstance(eupsSetupFiles, list):
            for eupsSetupFile in eupsSetupFiles:
                importEups(eupsSetupFile)
        else:
            importEups(eupsSetupFile)
                
