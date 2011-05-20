import sys
import traceback
import os
import re
import inspect
import stat
import eups
import sqlite

import LogConverter as logConv

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
        self.value = value
        self.limits = limits
        self.comment = comment

    def __str__(self):
        return self.label+" "+str(self.evaluate())+" value="+str(self.value)+" limits="+str(self.limits)

    def evaluate(self):
        """Verify that our value is within our limits."""
        
        # grab a traceback for failed tests
        if (self.value < self.limits[0] or self.value > self.limits[1]):
            return False
        else:
            return True


class TestSet(object):
    """A container for Test objects and associated matplotlib figures."""
    
    def __init__(self, label=None, group=""):
        """
        @param label  A name for this testSet
        @param group  A category this testSet belongs to
        """


        self.conn = None

        #prodDir = eups.productDir("testing_displayQA")
        prodDir = os.environ['TESTING_DISPLAYQA_DIR']
        wwwBase = os.path.join(prodDir, "www")
        testfileName = inspect.stack()[-1][1]
        self.testfileBase = re.sub(".py", "", os.path.split(testfileName)[1])
        prefix = "test_"
        if len(group) > 0:
            prefix += group+"_"
        self.wwwDir = os.path.join(wwwBase, prefix+self.testfileBase)
        if not label is None:
            self.wwwDir += "."+label

        if not os.path.exists(self.wwwDir):
            os.mkdir(self.wwwDir)


        # connect to the db and create the tables
        self.dbFile = os.path.join(self.wwwDir, "db.sqlite3")
        self.conn = sqlite.connect(self.dbFile)
        self.curs = self.conn.cursor()
        self.summTable, self.figTable, self.metaTable, self.eupsTable = \
                        "summary", "figure", "metadata", "eups"
        self.tables = {
            self.summTable : ["label text unique", "value double",
                              "lowerlimit double", "upperlimit double", "comment text",
                              "backtrace text"],
            self.figTable  : ["filename text", "caption text"],
            self.metaTable : ["key text", "value text"],
            }

        self.stdKeys = ["id integer primary key autoincrement", "entrytime timestamp"]
        for k, v in self.tables.items():
            keys = self.stdKeys + v
            cmd = "create table if not exists " + k + " ("+",".join(keys)+")"
            self.conn.execute(cmd)

        
        self.conn.commit()

        self.tests = []
        
    def __del__(self):
        if not self.conn is None:
            self.conn.close()


        
    def _insertOrUpdate(self, table, replacements, selectKeys):
        """Insert entries into a database table, overwrite if they already exist."""
        
        # there must be a better sql way to do this ... but my sql-foo is weak
        # we want to overwrite entries if they exist, or insert them if they don't
        
        # delete the rows which match the selectKeys
        where = []
        for key in selectKeys:
            if isinstance(replacements[key], str):
                where.append(key + "='" + replacements[key] + "'")
            else:
                where.append(key + "=" + str(replacements[key]))
        where = " where " + " and ".join(where)
        
        cmd = "delete from " + table + " " + where
        self.curs.execute(cmd)

        
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

        self.curs.execute(cmd, values)
        self.conn.commit()


    def addTests(self, testList):

        for test in testList:
            self.addTest(test)
            
        
    def addTest(self, *args, **kwargs):
        """Add a test to this testing suite.

        @param *args  Either a Test object, or the arguments to create one
        """

        if len(args) >= 4:
            test = Test(*args, **kwargs)
        elif len(args) == 1:
            test, = args

        self.tests.append(test)

        # grab a traceback for failed tests
        backtrace = ""
        try:
            if not test.evaluate():
                raise TestFailError("Failed test '"+test.label+"': " +
                                        "value '" + str(test.value) + "' not in range '" +
                                        str(test.limits)+"'.")
        except TestFailError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            backtrace = "".join(traceback.format_stack()[:-1]) + "\n" + str(e)
            
        # enter the test in the db
        keys = [x.split()[0] for x in self.tables[self.summTable]]
        replacements = dict( zip(keys, [test.label, test.value, test.limits[0], test.limits[1], test.comment,
                                        backtrace]) )
        self._insertOrUpdate(self.summTable, replacements, ['label'])


    def addMetadata(self, *args):
        """Associate metadata with this TestSet

	@param *args A dict of key,value pairs, or a key and value
	"""

        def addOneKvPair(k, v):
            keys = [x.split()[0] for x in self.tables[self.metaTable]]
            replacements = dict( zip(keys, [k, v]))
            self._insertOrUpdate(self.metaTable, replacements, ['key'])
            
        if len(args) == 1:
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

        
    def addFigure(self, fig, filename, caption, areaLabel=None, navMap=False):
        """Add a figure to this test suite.
        
        @param fig      a matplotlib figure
        @param filename The basename of the figure.
        @param caption  text describing the figure
	@param areaLabel a string associating the figure with a map area in a navigation figure
	@param navMap    Identify this figure as a navigation map figure containing linked map areas.
        """

        path = os.path.join(self.wwwDir, filename)

        # sub in the areaLabel, if given
        if not areaLabel is None:
            path = re.sub("(\.\w{3})$", r"-"+areaLabel+r"\1", path)
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
            self.curs.execute(cmd)
            self.conn.commit()

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
                
