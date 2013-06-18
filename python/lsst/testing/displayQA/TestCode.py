import sys
import traceback
import os
import re
import glob
import inspect
import stat
import shutil
import eups
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




class DbField(object):
    def __init__(self, name, typ, **kwargs):
        self.name = name
        self.typ  = typ
        self.size = kwargs.get('size', None)
        self.uniq = kwargs.get('uniq', False)
        
    
class SqliteField(DbField):
    def __init__(self, name, typ, **kwargs):
        super(SqliteField, self).__init__(name, typ, **kwargs)
        self.lookup = {"i": 'integer', 's' : 'text', 'd' : 'double', 'b' : 'integer'}
    def __str__(self):
        s = self.name + " " + self.lookup[self.typ]
        if self.uniq:
            s += " unique"
        return s

    
class PgsqlField(DbField):
    def __init__(self, name, typ, **kwargs):
        super(PgsqlField, self).__init__(name, typ, **kwargs)
        self.lookup = {"i": 'integer', 's' : 'varchar', 'd' : 'decimal', 'b' : 'bigint'}
    def __str__(self):
        s = self.name + " " + self.lookup[self.typ]
        if self.size:
            s += "(%d)" % (self.size)
        if self.uniq:
            s += " unique"
        return s
        
        
        
        
class DbInterface(object):
    
    def __init__(self, dbId):
        self.dbId = dbId
        self.dbId.dbname = 'pqa_'+str(self.dbId.dbname)

        self.wwwRoot  = os.getenv("WWW_ROOT")
        self.wwwRerun = os.getenv("WWW_RERUN")
        
    def _connDebug(self, f, msg):
        backtrace = "".join(traceback.format_stack()[:-2])
        fp = open(f+".debug", 'a')
        fp.write("%s %s PID%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"), msg, os.getpid()))
        fp.write("%s\n" % (backtrace))
        fp.close()
        
            

        
class PgsqlInterface(DbInterface):
    
    def __init__(self, dbId, **kwargs):
        """
        @param dbId  A databaseIdentity object contain connection information
        """
        super(PgsqlInterface, self).__init__(dbId)
        self.db = None
        self.cursor = None

        self.debug  = kwargs.get('debug', False)
        
        self.dbModule = __import__('psycopg2')

        self.debugFile = os.path.join(self.wwwRoot, self.wwwRerun)
        if self.debug:
            self._connDebug(self.debugFile, "open")

    def int(self, name):
        return PgsqlField(name, 'i')
    def bigint(self, name):
        return PgsqlField(name, 'b')
    def dec(self, name):
        return PgsqlField(name, 'd')
    def text(self, name, n, uniq=False):
        return PgsqlField(name, 's', size=n, uniq=uniq)

    
    def connect(self):

        if self.debug:
            self._connDebug(self.debugFile, "connect")
        self.conn = self.dbModule.connect(
            host     = self.dbId.dbhost,
            database = self.dbId.dbname,
            user     = self.dbId.dbuser,
            password = self.dbId.dbpwd,
            port     = self.dbId.dbport
            )
        self.cursor = self.conn.cursor()
        return self.cursor

    
    def execute(self, sql, values=None, fetch=False, lock_table=None):
        """Execute an sql command

        @param sql Command to be executed.
        """


        sql2 = sql
        if lock_table:
            sql2  = "BEGIN WORK;"
            sql2 += "LOCK TABLE "+lock_table+" IN ACCESS SHARE MODE;"
            sql2 += sql
            sql2 += "COMMIT WORK;"
        
        if self.cursor:
            if values is None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, values)
            if not fetch:
                self.conn.commit()
        else:
            raise RuntimeError, "Database is not connected"

        if fetch:
            results = self.cursor.fetchall()
            return results
        else:
            return 0
        
    def close(self):
        if self.conn:
            if self.debug:
                self._connDebug(self.debugFile, "close")
            return self.conn.close()

        
class SqliteInterface(DbInterface):
    
    def __init__(self, dbId, **kwargs):
        super(SqliteInterface, self).__init__(dbId)
                
        self.wwwDir = kwargs.get('wwwDir', os.path.join(self.wwwRoot, self.wwwRerun))
        self.suffix = kwargs.get('suffix', "")        
        self.debug  = kwargs.get('debug', False)
        self.dbFile = os.path.join(self.wwwDir, "db"+self.suffix+".sqlite3")
        self.debugFile = os.path.join(self.wwwDir, "db"+self.suffix)
        if self.debug:
            self._connDebug(self.debugFile, "open")

        self.dbModule = __import__('sqlite')


    def int(self, name):
        return SqliteField(name, 'i')
    def bigint(self, name):
        return SqliteField(name, 'b')
    def dec(self, name):
        return SqliteField(name, 'd')
    def text(self, name, n, uniq=False):
        return SqliteField(name, 's', size=n, uniq=uniq)
        
        
    def connect(self):
        self.conn = self.dbModule.connect(self.dbFile)
        self.cursor = self.conn.cursor()
        return self.cursor
        
    def execute(self, sql, values=None, fetch=False, lock_table=None):
        # sqlite doesn't need to lock
        if values:
            self.cursor.execute(sql, values)
        else:
            self.cursor.execute(sql)
            
        if fetch:
            results = self.cursor.fetchall()
        else:
            results = self.conn.commit()
        return results

    def close(self):
        if self.conn:
            if self.debug:
                self._connDebug(self.debugFile, "close")
            return self.conn.close()


            

class Database(object):
    """
    Defaults to a file that looks like:
    
    cat ~/.pqa/db-auth.paf
    database: {
        authInfo: {
            host: hsca-01.ipmu.jp
            user: "XXXX"
            password: "YYYY"
            port: 5432
        }
    }
    """
    def __init__(self, dbname, **kwargs):
        self.dbname   = dbname

        # defaults
        self.dbuser = None
        self.dbhost = None
        self.dbpwd  = None
        self.dbport = None
        self.dbsys  = 'sqlite'
        
        dbAuthFile = os.path.join(os.environ["HOME"], ".pqa", "db-auth.py")
        if os.path.exists(dbAuthFile):
            exec(open(dbAuthFile).read())
            self.dbuser = user
            self.dbhost = host
            self.dbpwd  = password
            self.dbport = port
            self.dbsys  = dbsys

        # override policy file with kwargs
        self.dbuser = kwargs.get("dbuser", self.dbuser)
        self.dbhost = kwargs.get("dbhost", self.dbhost)
        self.dbpwd  = kwargs.get("dbpwd",  self.dbpwd)
        self.dbport = kwargs.get("dbport", self.dbport)
        self.dbsys  = kwargs.get("dbsys",  self.dbsys)

        options = ["sqlite", "pgsql", "mysql"]
        if self.dbsys.lower() not in options:
            raise ValueError, "Database system (dbsys) must be "+", ".join(options)

        if self.dbsys == 'sqlite':
            self.interface = SqliteInterface(self, **kwargs)
        if self.dbsys == 'pgsql':
            self.interface = PgsqlInterface(self, **kwargs)

            
    def connect(self):
        return self.interface.connect()
    def execute(self, *args, **kwargs):
        return self.interface.execute(*args, **kwargs)
    def close(self):
        return self.interface.close()
            



















        
class TestSet(object):
    """A container for Test objects and associated matplotlib figures."""
    
    def __init__(self, label=None, group="", clean=False, useCache=False, alias=None, wwwCache=False, sqliteSuffix=""):
        """
        @param label  A name for this testSet
        @param group  A category this testSet belongs to
        """

        self.debugConnect = False
         
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


        self.db = Database(qaRerun, wwwDir=self.wwwDir, suffix=self.sqliteSuffix, debug=self.debugConnect)
        self.interface = self.db.interface
        self.db.connect()

        # very short abbreviations for creating db-appropriate variable definitions
        i = self.interface.int
        I = self.interface.bigint
        s = self.interface.text
        d = self.interface.dec

        # connect to the db and create the tables
        self.summTable, self.figTable, self.metaTable, self.testdirTable = \
                        "summary", "figure", "metadata", "testdir"
        self.tables = {
            self.summTable : [i("testdirId"), s("label", 80), d("value"),
                              d("lowerlimit"), d("upperlimit"), s("comment", 160),
                              s("backtrace", 480)],
            self.figTable  : [i("testdirId"), s("filename", 80, uniq=True), s("caption", 160)],
            self.metaTable : [i("testdirId"), s("key", 80), s("value", 2048)],
            self.testdirTable : [s("testdir", 160, uniq=True)],
            }

        self.stdKeys = {
            'sqlite':
                ["id integer primary key autoincrement",
                 "entrytime timestamp DEFAULT (strftime('%s','now'))"],
            'pgsql':
                ["id SERIAL",
                 "entrytime bigint DEFAULT date_part('epoch', now())"],
            }
        
        for k, v in self.tables.items():
            keys = map(str, self.stdKeys[self.db.dbsys] + v)

            
            sql = "CREATE TABLE IF NOT EXISTS " + k + " ("+",".join(keys)+");"
            if self.db.dbsys == 'sqlite':
                self.db.execute(sql)
                
            # pgsql can't do concurrent 'create table' ... race condition
            elif self.db.dbsys == 'pgsql':
                threw = False
                try:
                    self.db.execute(sql)
                except self.interface.dbModule.IntegrityError, e:
                    threw = True
                if threw:
                    self.db.close()
                    self.db.connect()

                    
                
        # ... and make sure this testdir exists in the db, and get the testDirID
        if self.db.dbsys == 'sqlite':
            sql = "INSERT OR IGNORE INTO testdir (id, entrytime, testdir) VALUES (NULL, strftime('%s', 'now'), '"+\
            self.testDir+"');"
            
        elif self.db.dbsys == 'pgsql':
            sql = "INSERT INTO testdir (entrytime, testdir) SELECT date_part('epoch', now()), '"+\
                self.testDir+"' WHERE NOT EXISTS (SELECT testdir FROM testdir WHERE testdir = '%s'); "% (self.testDir)
            
        self.db.execute(sql, lock_table='testdir')
        sql = "SELECT id FROM testdir WHERE testdir = '%s';" % (self.testDir)
        results = self.db.execute(sql, fetch=True)[0]
        self.testdirId = results[0]
        self.db.close()
        
        self.tests = []

        # create the cache table
        self.cache = None
        if self.wwwCache:
            self.cache = Database(qaRerun, wwwDir=self.wwwBase, suffix="", debug=self.debugConnect)
            # same abbreviations as definied above will work
            # i = int, s = str/text, d = double/float
            
            self.countsTable = "counts"
            self.failuresTable = "failures"
            self.allFigTable = "allfigures"
            self.cacheTables = {
                self.countsTable : [s("test", 80, uniq=True), i("ntest"), i("npass"), s("dataset", 80),
                                    I("oldest"), I("newest"), s("extras", 160)],
                self.failuresTable : [s("testandlabel", 160, uniq=True), d("value"),
                                      d("lowerlimit"), d("upperlimit"), s("comment", 160)],
                self.allFigTable : [s("path", 160, uniq=True), s("caption", 160)],
                }
            
            self.countKeys = self.cacheTables[self.countsTable]
            self.failureKeys = self.cacheTables[self.failuresTable]

            self.cache.connect()
            for k,v in self.cacheTables.items():
                keys = map(str, self.stdKeys[self.db.dbsys] + v)
                sql = "CREATE TABLE IF NOT EXISTS " + k + " ("+",".join(keys)+");"
                if self.db.dbsys == 'sqlite':
                    self.cache.execute(sql)
                elif self.db.dbsys == 'pgsql':
                    threw = False
                    try:
                        self.cache.execute(sql)
                    except self.interface.dbModule.IntegrityError, e:
                        threw = True
                    if threw:
                        self.cache.close()
                        self.cache.connect()

                        
            self.cache.close()

            
                
    def __del__(self):
        self.db.close()
        if self.wwwCache and (self.cache is not None):
            self.cache.close()


    def accrete(self):


        # for sqlite, we have to combine the per-ccd db.sqlite files into one file
        if self.db.dbsys == 'sqlite':

            import sqlite
            
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
                        if re.search("INTO\s+\""+self.testdirTable, d2):
                            d3 = re.sub("INSERT", "INSERT OR IGNORE", d2) 

                        statementsToAdd.append(d3)

            self.db.connect()
            for s in statementsToAdd:
                self.db.execute(s)
            self.db.close()

        
            

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

        # get the dataset from the metadata
        sql_meta = "select key,value from metadata"
        
        sql = "SELECT s.label,s.entrytime,s.value,s.lowerlimit,s.upperlimit FROM summary as s, testdir as t"\
            " WHERE s.testdirId = t.id and t.testdir = '%s'" % (self.testDir)
        
        try:
            self.db.connect()
            results     = self.db.execute(sql, fetch=True, lock_table='summary')
            metaresults = self.db.execute(sql_meta, fetch=True)
        except Exception, e:
            print "WARNING: An error occurred loading results from: ", str(self.db.dbId)
        finally:
            self.db.close()


        dataset = "unknown"
        for m in metaresults:
            k, v = m
            if k == 'dataset':
                dataset = v

            
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
                v3list = map(float, v[3])
                extraValues.append("%s:%.2f:%.2f:%s" % (v[1], numpy.mean(v3list), numpy.std(v3list), v[2]))
        extraStr = ",".join(extraValues)
        
        
        return ntest, npass, dataset, oldest, newest, extraStr


    def _writeCounts(self, ntest, npass, dataset="unknown", oldest=None, newest=None, extras=""):
        """Cache summary info for this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        keys = [x.name for x in self.countKeys]
        replacements = dict( zip(keys, [self.testDir, ntest, npass, dataset, oldest, newest, extras]))
        self._insertOrUpdate(self.countsTable, replacements, ['test'], cache=True)


    def _writeFailure(self, label, value, lo, hi, overwrite=True):
        """Cache failure info for this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        keys = [x.name for x in self.failureKeys]
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

        qmarkChar = "?"
        if self.db.dbsys == 'pgsql':
            qmarkChar = "%s"
        
        # insert the new data
        keys = []
        values = []
        for k,v in replacements.items():
            keys.append(k)
            values.append(v)
        values = tuple(values)
        inlist = " ("+ ",".join(keys) + ") "
        qmark = ",".join([qmarkChar]*len(values))
        qmarkB = " ("+ qmark + ") "

        whereComponents = []
        for s in selectKeys:
            sval = str(replacements[s])
            # add quotes if it's a string or label (which might be a visit [int])
            if isinstance(replacements[s], str) or s == 'label':
                sval = "'%s'" % (sval)
            whereComponents.append(s + " = " + sval)
        whereStatement = " AND ".join(whereComponents)
            
        allvalues = values[:]
        if self.db.dbsys == 'sqlite':
            cmd = "REPLACE INTO "+table+inlist + " VALUES " + qmarkB

            
        if self.db.dbsys == 'pgsql':
            # this is a mildly sleezy upsert.  pgsql has no upsert (replace into...)
            # so, do ...
            # ... an UPDATE which succeeds if there, and fails silently
            # ... an INSERT which succeeds if not there, and fails silently
            update = "UPDATE " + table + " SET " + inlist + " = " + qmarkB + \
                " WHERE " + whereStatement + ";"
            insert = "INSERT INTO " + table + inlist + " SELECT " + qmark + \
                " WHERE NOT EXISTS (SELECT 1 FROM " + table + " WHERE "+whereStatement+");";
            cmd = update + insert
            allvalues = values + values
            
        if not cache:
            self.db.connect()
            self.db.execute(cmd, allvalues, lock_table=table)
            self.db.close()
        else:
            
            if self.db.dbsys == 'sqlite':
                self.cache.connect()
                self.cache.execute(cmd, allvalues)
                self.cache.close()

            # race condition can occur here
            if self.db.dbsys == 'pgsql':
                i = 10
                while i > 0:
                    threw = False
                    self.cache.connect()
                    try:
                        self.cache.execute(cmd, allvalues, lock_table=table)
                    except self.interface.dbModule.IntegrityError, e:
                        threw = True
                    self.cache.close()

                    # try again (otherwise, our counts will be off in the cache)
                    if threw:
                        i -= 1
                        print "WARNING: Failed cache write.  Trying again."
                        print "SQL>"+cmd
                    # ... or we're ok
                    else:
                        break
                        
            
                    
    def _pureInsert(self, table, replacements, selectKeys, cache=False):
        """Insert entries into a database table, overwrite if they already exist."""
        
        # insert the new data
        keys = []
        values = []
        for k,v in replacements.items():
            keys.append(k)
            values.append(v)
        values = tuple(values)
        inlist = " (id, entrytime,testdirId,"+ ",".join(keys) + ") "
        qmark  = " (NULL, strftime('%s', 'now'),'"+self.testdirId + "',".join("?"*len(values)) + ");"
        cmd    = "insert into "+table+inlist + " values " + qmark

        if not cache:
            self.db.connect()
            self.db.execute(cmd, values)
            self.db.close()
        else:
            self.cache.connect()
            self.cache.execute(cmd, values)
            self.cache.close()

            
    def addTests(self, testList):
        for test in testList:
            self.addTest(test)
            

    def updateFailures(self, overwrite=True):

        if self.wwwCache:

            # load the summary
            sql = "select label,entrytime,value,lowerlimit,upperlimit from summary;"
            self.db.connect()
            results = self.db.execute(sql, fetch=True)
            self.db.close()
            
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
            sql = "select filename from figure;"
            self.db.connect()
            figures = self.db.execute(sql, fetch=True)
            self.db.close()

            # write allfigtable
            keys = [x.name for x in self.cacheTables[self.allFigTable]]
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
            
            # ignore the final line (it's this addTest() call)
            btList = traceback.format_stack()[:-1]
            # keep only the final two lines (the test which failed)
            if len(btList) > 2:
                btList = btList[-2:]
            backtrace = "".join(btList) + "\n" + str(e)

            
        if kwargs.has_key('backtrace'):
            backtrace = kwargs['backtrace']
            
        # enter the test in the db
        keys = [x.name for x in self.tables[self.summTable]]
        replacements = dict( zip(keys, [self.testdirId, test.label, test.value,
                                        test.limits[0], test.limits[1], test.comment,
                                        backtrace]) )
        self._insertOrUpdate(self.summTable, replacements, ['label', 'testdirId'])

        self.updateCounts() #increment=[1, npassed])



    def addMetadata(self, *args):
        """Associate metadata with this TestSet

        @param *args A dict of key,value pairs, or a key and value
        """

        keys = [x.name for x in self.tables[self.metaTable]]
        def addOneKvPair(k, v):
            replacements = dict( zip(keys, [self.testdirId, k, v]))
            self._insertOrUpdate(self.metaTable, replacements, ['key', 'testdirId'])
            
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
            tablekeys = [x.name for x in self.tables[self.summTable]]
            replacements = dict( zip(tablekeys, [key, 0, 1, 1, "Uncaught exception", exceptDict[key]]) )
            self._insertOrUpdate(self.summTable, replacements, ['label', 'testdirId'])

            
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

        
        keys = [x.name for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [self.testdirId,filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename', 'testdirId'])

        if self.wwwCache:
            keys = [x.name for x in self.cacheTables[self.allFigTable]]
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
        
        keys = [x.name for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [self.testdirId, filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename', 'testdirId'])

        if self.wwwCache:
            keys = [x.name for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)


    def addFigureFile(self, basename, caption, areaLabel=None, toggle=None, navMap=False,
                      doCopy=True, doConvert=False):
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


        keys = [x.name for x in self.tables[self.figTable]]
        replacements = dict( zip(keys, [self.testdirId, filename, caption]))
        self._insertOrUpdate(self.figTable, replacements, ['filename', 'testdirId'])

        if self.wwwCache:
            keys = [x.name for x in self.cacheTables[self.allFigTable]]
            replacements = dict( zip(keys, [path, caption]))
            self._insertOrUpdate(self.allFigTable, replacements, ['path'], cache=True)

