#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import sys, os, re, glob, shutil, stat
import commands
import argparse

haveEups = True
try:
    import eups
except:
    haveEups = False
    

#############################################################
#
# Main body of code
#
#############################################################

def main(qaName, wwwRoot=None, force=False, forceClean=False, color="blue", project_icons='lsst'):

    pqaDb = "pqa_"+qaName
    
    # verify that we have WWW_ROOT and TESTING_DISPLAYQA_DIR
    envVars = ['TESTING_DISPLAYQA_DIR']
    if wwwRoot is None:
        envVars.append('WWW_ROOT')
    missing = []
    for envVar in envVars:
        if not os.environ.has_key(envVar):
            missing.append(envVar)
    if len(missing) > 0:
        raise Exception("Missing environment variable(s):\n", "\n".join(missing))
    
    if wwwRoot is None:
        wwwRoot = os.environ['WWW_ROOT']
    dqaDir = os.environ['TESTING_DISPLAYQA_DIR']


    # get our version
    if haveEups:
        e = eups.Eups()
        version, eupsPathDir, productDir, tablefile, flavour = e.findSetupVersion('testing_displayQA')
    else:
        path, v = os.path.split(dqaDir)
        if re.search("^v[\d.-_]+$", v):
            version = v
        else:
            version = "working-copy-%s" % (v)


    # check to see if qaname already exists with different version
    dest = os.path.join(wwwRoot, qaName)

    if os.path.exists(dest):
        vFile = os.path.join(dest, "version")
        if os.path.exists(vFile):
            fp = open(vFile)
            v = fp.readlines()[0]
            v = v.strip()
            fp.close()

            if not force:
                if (v != version):
                    raise Exception(qaName,
                                    "already exists with different version. Use -f to force.")
                else:
                    print "QA site '" + qaName + "' already exists at:"
                    print "    ", dest
                    print "Exiting (nothing done)."
                    sys.exit()
            else:
                fp = open(os.path.join(dest, "version"), 'w')
                fp.write("%s\n" % version)
                fp.close()
            
    else:
        os.mkdir(dest)
        fp = open(os.path.join(dest, "version"), 'w')
        fp.write("%s\n" % version)
        fp.close()


        
    ###############################################
    # copy the www/ to the destination
    src = os.path.join(dqaDir, "www")
    patterns = ["php", "js"]
    files = []
    for p in patterns:
        files += glob.glob(os.path.join(src, "[a-zA-Z]*." + p))
    doc = os.path.join(dqaDir, "doc")
    files += glob.glob(os.path.join(doc, "README"))

    for f in files:
        dir, script = os.path.split(f)
        print "installing: ", script
        cmd = "cp -r %s %s" % (f, dest)
        os.system(cmd)

    # handle the css and favicon files based on color chosen
    style_base = "style_"+color+".css"
    favicon_base = project_icons+"_favicon_"+color+".png"
    files = [
        [style_base,   os.path.join(src, style_base),   os.path.join(dest, "style.css")],
        [favicon_base, os.path.join(src, favicon_base), os.path.join(dest, "favicon.ico")],
        ]
        
    for file_base, file_color, file_dest in files:
        if os.path.exists(file_color):
            print "installing: ", file_base
            os.system("cp %s %s" % (file_color, file_dest))
        else:
            color_files = glob.glob(os.path.join(src, "style_*.css"))
            colors = []
            for f in color_files:
                m = re.search("style_(.*).css", f)
                if m:
                    colors.append(m.group(1))
            msg = "Cannot install color '"+color+"'. "
            msg += "Available colors: " + ", ".join(colors)
            print msg
            sys.exit()
        
    print ""
    print "Created new QA site served from:"
    print "   ",dest



    #####################################
    # load DB info, will we use sqlite, pgsql ... host, port, user?
    dbAuthFile = os.path.join(os.environ["HOME"], ".pqa", "db-auth.py")
    if os.path.exists(dbAuthFile):
        exec(open(dbAuthFile).read())
    else:
        print "\033[32;1m"
        print "File:", dbAuthFile, "is missing.  It should look like this: "
        print "\n".join(['$ cat ~/.pqa/db-auth.py"',
                         'host = "host.domain.edu"',
                         'port = "5432"',
                         'user = "you"',
                         'password = "secret"',
                         'dbsys = "pgsql"'])
        print "\033[0m"
        raise RuntimeError("Missing pipeQa database authetication file: " + dbAuthFile)


    
    ######################################
    # handle forced cleaning
    envFile = os.path.join(dest, "environment.php")
    if forceClean:
        print ""
        print "Cleaning existing data from", dest, ":"
        dbFile = os.path.join(dest, "db.sqlite3")
        for f in [dbFile, envFile]:
            if os.path.exists(f):
                print "   ", os.path.split(f)[1]
                os.remove(f)
        for testDir in glob.glob(os.path.join(dest, "test_*")):
            print "   ", os.path.split(testDir)[1]
            shutil.rmtree(testDir)

        # we'll blow away the db if needed
        if dbsys == 'pgsql':
            print "Determining if database", pqaDb, "exists (set PGPASSWORD to avoid typing password)."
            cmdCheckExists = "psql -l -h %s -p %s -U %s | grep ' %s ' | wc -l" % (host, port, user, pqaDb)
            existsStatus, existsRet = commands.getstatusoutput(cmdCheckExists)
            if existsRet != '0':
                print "Database", pqaDb, "already exists."
                dropdb = "dropdb -h %s -p %s -U %s %s" % (host, port, user, pqaDb)
                print "Dropping database:", pqaDb
                os.system(dropdb)

        if dbsys == 'mysql':
            pass
                
    if dbsys == 'pgsql':

        cmdCheckExists = "psql -l -h %s -p %s -U %s | grep ' %s ' | wc -l" % (host, port, user, pqaDb)
        existsStatus, existsRet = commands.getstatusoutput(cmdCheckExists)

        # only try if it doesn't already exist
        # -F means it should have been blown away by dropdb above
        # -f means leave it alone if it's there (force install, but no overwrite of data)
        if existsRet == '0':
            createdb = "createdb -h %s -p %s -U %s %s" % (host, port, user, pqaDb)
            print "Attempting to create new database: ", pqaDb
            createStatus, createRet = commands.getstatusoutput(createdb)
            # we should never see this
            if createStatus == 256:
                print "Database ", pqaDb, "already exists.  Use -F to force delete."
            # some generic error occurred
            elif createStatus != 0:
                print "createdb failed for %s. Returned status %s: %s" % (qaName, createStatus, createRet)
            elif createStatus == 0:
                print "Database ", pqaDb, "successfully created."
            
    if dbsys == 'mysql':
        pass

            
    #########################################
    # touch the environment file to make sure it's there.
    with file(envFile, 'a'):
        os.utime(envFile, None)

        
    #########################################
    # make sure the matplotlib hidden dir is there and is writeable
    mpldir = os.path.join(wwwRoot, ".matplotlib")
    if not os.path.exists(mpldir):
        os.mkdir(mpldir)
    os.chmod(mpldir, os.stat(mpldir).st_mode | stat.S_IRWXO)
        

    #########################################
    # generate a dbconfig.php file
    dbConfFile = os.path.join(dest, "dbconfig.php")
    with open(dbConfFile, 'w') as dbFp:
        dbFp.write("<?php\n")
        dbFp.write("$host = '"+host+"';\n")
        dbFp.write("$port = '"+port+"';\n")
        dbFp.write("$user = '"+user+"';\n")
        dbFp.write("$password = '"+password+"';\n")
        dbFp.write("$dbsys = '"+dbsys+"';\n")
        dbFp.write("$dbname = '"+pqaDb+"';\n")
        
        
#############################################################
# end
#############################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("qaName",
                      help="Name of the project.  Used as new directory under WWW_ROOT.")
    parser.add_argument('-c', '--color', default="blue",
                      choices=("blue", "green", "red", "brown"),
                      help="Specify style color.")
    
    parser.add_argument("-f", '--force', default=False, action="store_true",
                      help="Force a reinstall if already exists.")
    parser.add_argument("-F", '--forceClean', default=False, action="store_true",
                      help="Force a reinstall and remove existing data")
    
    parser.add_argument('-r', '--root', default=None, help="Override WWW_ROOT.")
    parser.add_argument("-n", '--noquery', default=False, action="store_true",
                      help="Don't query about options ... user knows what user is doing.")
    parser.add_argument("-p", '--project_icons', default='lsst',
                      choices=('lsst', 'hsc', 'sc'),
                      help="Specify project-specific icons for page.")

    args = parser.parse_args()

    if args.forceClean and not args.noquery:
        query = raw_input("--forceClean is set, and will to delete any/all existing data in rerun %s."\
                          " Continue [y/N]: " % (args.qaName))
        if not re.search("^[yY]", query):
            print "Exiting. (You may wish to consider --force instead of --forceClean)"
            sys.exit(1)
        

    if args.forceClean:
        args.force = True

    main(args.qaName, wwwRoot=args.root, force=args.force, forceClean=args.forceClean,
         color=args.color, project_icons=args.project_icons)
    
