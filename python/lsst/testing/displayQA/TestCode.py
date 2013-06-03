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
import datetime

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
    
    def __init__(self, label=None, group="", clean=False, useCache=False, alias=None, wwwCache=False, sqliteSuffix=""):
        """
        @param label  A name for this testSet
        @param group  A category this testSet belongs to
        """

        self.debugConnect = True
        
        self.conn = None
        self.cacheConn = None
        self.sqliteSuffix = sqliteSuffix
        if len(sqliteSuffix) > 0:
            self.sqliteSuffix = "-" + self.sqliteSuffix
        
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
        self.summTable, self.figTable, self.metaTable, self.eupsTable = \
                        "summary", "figure", "metadata", "eups"
        self.tables = {
            self.summTable : ["label text unique", "value double",
                              "lowerlimit double", "upperlimit double", "comment text",
                              "backtrace text"],
            self.figTable  : ["filename text unique", "caption text"],
            self.metaTable : ["key text unique", "value text"],
            }

        self.stdKeys = ["id integer primary key autoincrement", "entrytime timestamp DEFAULT (strftime('%s','now'))"]

        try:
            self.connect()
            for k, v in self.tables.items():
                keys = self.stdKeys + v
                cmd = "create table if not exists " + k + " ("+",".join(keys)+")"
                self.curs.execute(cmd)
            self.conn.commit()
        except:
            print "WARNING: sqlite access error: "+self.dbFile
        finally:
            self.close()

        
        self.tests = []

        # create the cache table
        if self.wwwCache:
            self.countsTable = "counts"
            self.failuresTable = "failures"
            self.allFigTable = "allfigures"
            self.cacheTables = {
                self.countsTable : ["test text unique", "ntest integer", "npass integer", "dataset text",
                                   "oldest timestamp", "newest timestamp", "extras text"],
                self.failuresTable : ["testandlabel text unique", "value double",
                                      "lowerlimit double", "upperlimit double", "comment text"],
                self.allFigTable : ["path text unique", "caption text"],
                }
            
            self.countKeys = self.cacheTables[self.countsTable]
            self.failureKeys = self.cacheTables[self.failuresTable]
            
            for k,v in self.cacheTables.items():
                keys = self.stdKeys + v
                cmd = "create table if not exists " + k + " ("+",".join(keys)+")"
                try:
                    curs = self.cacheConnect()
                    curs.execute(cmd)
                except Exception, e:
                    print "WARNING: Database access error: " + self.cacheDbFile
                finally:
                    self.cacheClose()

                
    def __del__(self):
        self.close()
        self.cacheClose()


    def accrete(self):
        dbs = glob.glob(os.path.join(self.wwwDir, "db-*.sqlite3"))

        # go to each db and dump it
        statementsToAdd = []
        for db in dbs:

            # go through all 'INSERT' statements and execute on outself
            allStatements = []
            try:
                conn = sqlite.connect(db)
                for d in conn.iterdump():
                    allStatements.append(d)
            except Exception, e:
                print "WARNING: Exception occurred while accessing "+db+ str(e)
            finally:
                conn.close()
                
            for d in allStatements:
                if re.search("INSERT INTO", d):
                    # kill the PK so it'll autoincrement
                    d2 = re.sub("VALUES\(\d+,", "VALUES(NULL,", d)
                    d3 = d2
                    # if this is the summary table, use REPLACE to handle unique 'label' column
                    if (re.search("INTO\s+\""+self.summTable, d2) or
                        re.search("INTO\s+\""+self.metaTable, d2) or
                        re.search("INTO\s+\""+self.figTable, d2)):
                        d3 = re.sub("INSERT", "REPLACE", d2)
                    statementsToAdd.append(d3)
                    
        try:
            thisCurs = self.connect()
            for s in statementsToAdd:
                thisCurs.execute(s)
            self.conn.commit()
        except Exception, e:
            print "WARNING: An error occurred gathering db statements to: "+self.dbFile
        finally:
            self.close()

            
    def _connDebug(self, f, msg):

        backtrace = ""
        try:
            raise TestFailError(msg)
        except TestFailError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            backtrace = "".join(traceback.format_stack()[:-2])

        
        fp = open(f+".debug", 'a')
        fp.write("%s %s PID%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"), msg, os.getpid()))
        fp.write("%s\n" % (backtrace))
        fp.close()
        

    def connect(self):
        self.dbFile = os.path.join(self.wwwDir, "db"+self.sqliteSuffix+".sqlite3")
        if self.debugConnect:
            self._connDebug(self.dbFile, "open")
        if useApsw:
            self.conn = apsw.Connetion(self.dbFile)
        else:
            self.conn = sqlite.connect(self.dbFile)

        self.curs = self.conn.cursor()
        return self.curs

    def close(self):
        if self.conn:
            if self.debugConnect:
                self._connDebug(self.dbFile, "close")
            self.conn.close()
        

    def cacheConnect(self):
        self.cacheDbFile = os.path.join(self.wwwBase, "db.sqlite3")
        if self.debugConnect:
            self._connDebug(self.cacheDbFile, "open")
        if useApsw:
            self.cacheConn = apsw.Connection(self.cacheDbFile)
        else: 
            self.cacheConn = sqlite.connect(self.cacheDbFile)
        self.cacheCurs = self.cacheConn.cursor()
        return self.cacheCurs

    def cacheClose(self):
        if self.cacheConn:
            if self.debugConnect:
                self._connDebug(self.cacheDbFile, "close")
            self.cacheConn.close()
            
        
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

        try:
            self.connect()

            if useApsw:
                results = self.curs.execute(sql)
            else:
                self.curs.execute(sql)
                results = self.curs.fetchall()
        except Exception, e:
            print "WARNING: An error occurred loading results from: "+self.dbFile
        finally:
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

        try:
            self.connect()
            if useApsw:
                metaresults = self.curs.execute(sql)
            else:
                self.curs.execute(sql)
                metaresults = self.curs.fetchall()
        except Exception, e:
            print "WARNING: an error occurred loading metadata results from "+self.dbFile
        finally:
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

        keys = [x.split()[0] for x in self.countKeys]
        replacements = dict( zip(keys, [self.testDir, ntest, npass, dataset, oldest, newest, extras]))
        self._insertOrUpdate(self.countsTable, replacements, ['test'], cache=True)


    def _writeFailure(self, label, value, lo, hi, overwrite=True):
        """Cache failure info for this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        keys = [x.split()[0] for x in self.failureKeys]
        testandlabel = self.testDir + "QQQ" + str(label)
        replacements = dict( zip(keys, [testandlabel, value, lo, hi]))
        if overwrite:
            self._insertOrUpdate(self.failuresTable, replacements, ['testandlabel'], cache=True)
        else:
            self._pureInsert(self.failuresTable, replacements, ['testandlabel'], cache=True)

        
    def _insertOrUpdate(self, table, replacements, selectKeys, cache=False):
        """Insert entries into a database table, overwrite if they already exist."""
        
        # there must be a better sql way to do this ... but my sql-foo is weak
        # we want to overwrite entries if they exist, or insert them if they don't
        
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
            
        if not cache:
            try:
                self.connect()
                self.curs.execute(cmd, values)
                if not useApsw:
                    self.conn.commit()
            except Exception, e:
                print "sqlite File:   ", self.dbFile
                print "sqlite Cmd:    ", cmd
                print "sqlite Values: ", values
                raise
            finally:
                self.close()
                
        else:
            try:
                self.cacheConnect()
                self.cacheCurs.execute(cmd, values)
                if not useApsw:
                    self.cacheConn.commit()
            except Exception, e:
                print "sqlite File:   ", self.cacheDbFile
                print "sqlite Cmd:    ", cmd
                print "sqlite Values: ", values
                raise
            finally:
                self.cacheClose()
                

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
            try:
                self.connect()
                self.curs.execute(cmd, values)
                if not useApsw:
                    self.conn.commit()
            except Exception, e:
                print "WARNING: insert failed in "+self.dbFile
            finally:
                self.close()
        else:
            try:
                self.cacheConnect()
                self.cacheCurs.execute(cmd, values)
                if not useApsw:
                    self.cacheConn.commit()
            except Exception, e:
                print "WARNING: insert failed in "+self.cacheDbFile
            finally:
                self.cacheClose()

    def addTests(self, testList):

        for test in testList:
            self.addTest(test)
            

    def updateFailures(self, overwrite=True):

        if self.wwwCache:

            # load the summary
            sql = "select label,entrytime,value,lowerlimit,upperlimit from summary"
            try:
                self.connect()
                if useApsw:
                    results = self.curs.execute(sql)
                else:
                    self.curs.execute(sql)
                    results = self.curs.fetchall()
            except Exception, e:
                print "WARNING: an error occurred updating failures in "+self.dbFile
            finally:
                self.close()
            
            # write failures
            failSet = []
            for r in results:
                label, etime, value, lo, hi = r
                if re.search("-*-", label):
                    labelsplit = label.split("-*-")
                    tag = labelsplit[1].strip()
                    failSet.append(tag)
                    cmp = self._verifyTest(value, lo, hi)
                    if cmp:
                        self._writeFailure(str(label), value, lo, hi, overwrite)

            failSet = set(failSet)
            
            # load the figures
            sql = "select filename from figure"
            try:
                self.connect()
                if useApsw:
                    figures = self.curs.execute(sql)
                else:
                    self.curs.execute(sql)
                    figures = self.curs.fetchall()
            except Exception, e:
                print "WARNING: an exception occured loading figures in "+self.dbFile
            finally:
                self.close()

            # write allfigtable
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            for f in figures:
                filename, = f
                filebase = re.sub(".png", "", filename)
                if re.search("[^-]-[^-]", filebase):
                    fileqqq = re.sub("([^-])-([^-])", r"\1QQQ\2", filebase)
                    tag = fileqqq.split("QQQ")[-1]
                    tag = tag.strip()
                    if tag in failSet:
                        path = str(os.path.join(self.wwwDir, filename))
                        replacements = dict( zip(keys, [path, ""]))
                        if overwrite:
                            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
                        else:
                            self._pureInsert(self.allFigTable, replacements, ['path'], cache=True)

    

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
                    self._writeFailure(test.label, test.value, test.limits[0], test.limits[1])
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

        keys = [x.split()[0] for x in self.tables[self.metaTable]]
        def addOneKvPair(k, v):
            replacements = dict( zip(keys, [k, v]))
            self._insertOrUpdate(self.metaTable, replacements, ['key'])
            
        if len(args) == 1:
            kvDict, = args
            for k, v in kvDict.items():
                addOneKvPair(k, v)
                #replacements = dict( zip(keys, [k, v]))
                #self._insertOrUpdate(self.metaTable, replacements, ['key'])
        elif len(args) == 2:
            k, v = args
            addOneKvPair(k, v)
            #replacements = dict( zip(keys, [k, v]))
            #self._insertOrUpdate(self.metaTable, replacements, ['key'])
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
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)
        

            
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
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)


    def addFigureFile(self, basename, caption, areaLabel=None, toggle=None, navMap=False, doCopy=True, doConvert=False):
        """Add a figure to this test suite.
        
        @param filename The basename of the figure.
        @param caption  text describing the figure
        @param areaLabel a string associating the figure with a map area in a navigation figure
        @param navMap    Identify this figure as a navigation map figure containing linked map areas.
        @param doCopy   if True make a copy of the file, if False make a symlink to the file.
        @param doConvert if True create a smaller thumbnail png file. if False, just make a symlink.
        """

        orig_path, orig_file = os.path.split(basename)
        
        # sub in the areaLabel, if given
        filename = orig_file
        if not toggle is None:
            filename = re.sub("(\.\w{3})$", r"."+toggle+r"\1", filename)
        if not areaLabel is None:
            filename = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", filename)

        path = os.path.join(self.wwwDir, filename)

        if os.path.exists(path):
            os.remove(path)
                        

        if doCopy:
            shutil.copyfile(basename, path)
        else:
            os.symlink(basename, path)


        thumbPath = re.sub(".png$", "Thumb.png", path)
        if os.path.exists(thumbPath):
            os.remove(thumbPath)

        if doConvert:
            convert = which("convert")
            size = "200x200"
            if convert:
                os.system(convert + " " + path + " -resize "+size+" " + thumbPath)
            else:
                os.symlink(path, thumbPath)
        else:
                os.symlink(path, thumbPath)


        keys = [x.split()[0] for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename'])

        if self.wwwCache:
            keys = [x.split()[0] for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)

