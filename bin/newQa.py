#!/usr/bin/env python
#
# Original filename: bin/newQa.py
#
# Author: 
# Email: 
# Date: Thu 2011-06-09 14:00:22
# 
# Summary: 
# 
"""
%prog [options] name
"""

import sys, os, re, glob, shutil
import optparse

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

def main(qaName, wwwRoot=None, force=False, forceClean=False, color="blue"):

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
    favicon_base = "favicon_"+color+".png"
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


    if forceClean:
        print ""
        print "Cleaning existing data from", dest, ":"
        dbFile = os.path.join(dest, "db.sqlite3")
        if os.path.exists(dbFile):
            print "   ", os.path.split(dbFile)[1]
            os.remove(dbFile)
        for testDir in glob.glob(os.path.join(dest, "test_*")):
            print "   ", os.path.split(testDir)[1]
            shutil.rmtree(testDir)

#############################################################
# end
#############################################################

if __name__ == '__main__':
    parser = optparse.OptionParser(usage=__doc__)
    
    parser.add_option('-c', '--color', default="blue",
                      help="Specify style color (default=%default)")
    parser.add_option("-f", '--force', default=False, action="store_true",
                      help="Force a reinstall if already exists (default=%default)")
    parser.add_option("-F", '--forceClean', default=False, action="store_true",
                      help="Force a reinstall and remove existing data (default=%default)")
    parser.add_option('-r', '--root', default=None, help="Override WWW_ROOT (default=%default")
    parser.add_option("-n", '--noquery', default=False, action="store_true",
                      help="Don't query about options ... user knows what user is doing. (default=%default)")
    opts, args = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    qaName, = args

    if opts.forceClean and not opts.noquery:
        query = raw_input("--forceClean is set, and will to delete any/all existing data in rerun %s."\
                          " Continue [y/N]: " % (qaName))
        if not re.search("^[yY]", query):
            print "Exiting. (You may wish to consider --force instead of --forceClean)"
            sys.exit(1)
        

    if opts.forceClean:
        opts.force = True
        
    main(qaName, opts.root, opts.force, opts.forceClean, opts.color)
    
