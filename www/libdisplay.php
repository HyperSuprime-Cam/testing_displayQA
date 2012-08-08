<?php

include_once("Html.php");
include_once("libdb.php");

date_default_timezone_set('America/New_York');

function p($val,$ret=0) {
    static $i = 0;
    $retStr = ($ret > 0) ? "<br/>\n" : "";
    echo "<font color=\"#ff0000\">$i</font>:($val) ".$retStr;
    $i += 1;
    if ($ret > 0) {$i = 0;}
}


######################################
# true/false color
# color text green if true, else red 
######################################
function tfColor($string, $tf) {
    if ( $tf ) {
        $colorout = "<font color=\"#00aa00\">$string</font>";
    } else {
        $colorout = "<font color=\"#880000\">$string</font>";
    }
    return $colorout;
}

function verifyTest($value, $lo, $hi) {
    
    $cmp = 0; #true;  # default true (ie. no limits were set)
    if ($lo and $hi) {
        if ($value < $lo) {
            $cmp = -1;
        } elseif ($value > $hi) {
            $cmp = 1;
        }
    } elseif ($lo and !$hi and ($value < $lo)) {
        $cmp = -1;
    } elseif (!$lo and $hi and ($value > $hi)) {
        $cmp = 1;
    }
    return $cmp;
}

function hiLoColor($value, $lo, $hi) {
    $fvalue = floatval($value);
    $valueStr = sprintf("%.4f", $fvalue);
    $cmp = verifyTest($fvalue, $lo, $hi);
    if ($cmp == 0) { #(!$lo or $fvalue >= $lo) and (!$hi or $fvalue <=$hi)) {
        $colorout = "<font color=\"#00aa00\">$valueStr</font>";
    } elseif ($cmp < 0) { #$lo and $fvalue < $lo) {
        $colorout = "<font color=\"#000088\">$valueStr</font>";
    } elseif ($cmp > 0) { #$hi and $fvalue > $hi) {
        $colorout = "<font color=\"#880000\">$valueStr</font>";
    } else {
        $colorout = "$valueStr";
    }
    return $colorout;
}


#######################################
#
#
#######################################
function getCurrentUriDir() {
    $uri = $_SERVER['SCRIPT_NAME'];
    $dirs = preg_split("/\//", $uri);
    $dir = $dirs[count($dirs)-2];
    return $dir;
}

function getDefaultTitle() {
    return getCurrentUriDir()." pipeQA";
}

function getDefaultH1() {
    return "Q.A. Test Summary &nbsp;&nbsp; <a href=\"../\"><font size=\"3\">Go to main rerun list.</font></a>";
}


function stdev($aValues, $bSample = false) {
    if (count($aValues) < 2) {
        return 0.0;
    }
    $fMean = array_sum($aValues) / count($aValues);
    $fVariance = 0.0;
    foreach ($aValues as $i) {
        $fVariance += pow($i - $fMean, 2);
    }
    $fVariance /= ( $bSample ? count($aValues) - 1 : count($aValues) );
    return (float) sqrt($fVariance);
}



function getTestLinksThisGroup($page) {
    
    $group = getGroup();
    $active = getActive();
    $allGroups = getGroupList();
    list($results, $nGrp) = loadCache();
    $currTest = getDefaultTest();

    # handle possible failure of the cache load
    $testList = array();
    if ($results != -1) {
        foreach ($results as $r) {
            $testList[] = $r['test'];
        }
    } else {
        $dir = "./";
        $d = @dir($dir) or dir("");
        while(false !== ($testDir = $d->read())) {
            if (preg_match("/test_/", $testDir)) {
                $testList[] = $testDir;
            }
        }
    }

    $testParts = preg_split("/_/", $currTest);
    $testName = "";
    if (count($testParts) > 2) {
        $testName = $testParts[2];
    }

    # get the prev/next group
    $groupDirs = array();
    $groupNames = array_keys($allGroups);

    # handle the unnamed group possibility
    $index0 = ( (count($groupNames) > 0) and $groupNames[0] === "" and (count($groupNames) > 1) ) ? 1 : 0;
    
    $index = array_search($group, $groupNames);

    #######
    # straight group advancing
    $space = "<br/>";
    if ($index > $index0) {
        $prev = $groupNames[$index-1];
        $groupDirs["<<- prev-group$space($prev)"] = array($prev, "test_${prev}_${testName}");
    } else {
        $groupDirs["<<- prev-group$space(none)"] = array("None", "");
    }
    if ($index and $index < count($groupNames)-1) {
        $next = $groupNames[$index+1];
        $groupDirs["next-group ->>$space($next)"] = array($next, "test_${next}_${testName}");
    } else {
        $groupDirs["next-group ->>$space(none)"] = array("None", "");
    }

    #######
    # filter advancing
    $filterDirs = array();
    if (preg_match("/-[ugrizy]$/", $group)) {
        $thisFilter = substr($group, -1);
        
        $havePrev = false;
        for ($i = $index-1; $i>-1; $i--) {
            $gname = $groupNames[$i];
            $cmpFilter = substr($gname, -1);
            if ($thisFilter == $cmpFilter) {
                $filterDirs["<<- prev-$cmpFilter$space($gname)"] = array($gname, "test_${gname}_${testName}");
                $havePrev = true;
                break;
            }
        }

        if (!$havePrev) {
            $filterDirs["<<- prev-$thisFilter$space(none)"] = array("None", "");
        }
        
        $haveNext = false;
        for ($i = $index+1; $i< count($groupNames); $i++) {
            $gname = $groupNames[$i];
            $cmpFilter = substr($gname, -1);
            if ($thisFilter == $cmpFilter) {
                $filterDirs["next-$cmpFilter ->>$space($gname)"] = array($gname, "test_${gname}_${testName}");
                $haveNext = true;
                break;
            }
        }

        if (!$haveNext) {
            $filterDirs["next-$thisFilter ->>$space(none)"] = array("None", "");
        }
        
                
    }

    
    # get the tests for this group
    $testDirs = array();
    $testNames = array();
    foreach ($testList as $t) {
        if (preg_match("/test_${group}_/", $t)) {
            $parts = preg_split("/_/", $t);
            if (count($parts) > 2) {
                #$testDirs[] = $t;
                $testName = $parts[2];
                $subparts = preg_split("/\./", $testName);
                $label = substr($subparts[1], 0, 4);
                if (count($subparts) > 2) {
                    $label .= "<br/>".$subparts[2];
                }
                $testDirs[$label] = $t;
                $testNames[$label] = $testName;
            }
        }
    }
    ksort($testDirs);


    $outString = "";
    
    # make the table for the test links
    if ($page == 'summary') {
        $table = new Table("width=\"100%\"");
        $row = array();
        foreach ($testDirs as $label=>$testDir) {
            $testName = $testNames[$label];
            $link = "<a href=\"summary.php?test=".$testDir."&active=$active&group=$group\" title=\"$testName\">".$label."</a>";
            $row[] = $link;
        }
        $table->addRow($row);
        $outString .= $table->write();
    }

    # make the table for the next/prev group links
    $tableG = new Table("width=\"100%\"");
    $row = array();
    $navDirs = array_merge($groupDirs, $filterDirs);
    foreach ($navDirs as $label=>$groupDirInfo) {
        list($g, $groupDir) = $groupDirInfo;
        if ($groupDir) {
            $link = ($page=='group') ?
                "<a href=\"group.php?group=$g\" title=\"$g\">".$label."</a>":
                "<a href=\"summary.php?test=".$groupDir."&active=$active&group=$g\">".$label."</a>";
        } else {
            $link = "$label";
        }
        $row[] = $link;
    }
    $tableG->addRow($row);

    $outString .= $tableG->write();

    return $outString;
}


function getShowHide() {
    $show = "0";
    if (array_key_exists('show', $_GET)) {
        $show = $_GET['show'];
        setcookie('displayQA_show', $show);
    } elseif (array_key_exists('displayQA_show', $_COOKIE)) {
        $show = $_COOKIE['displayQA_show'];
    }
    if ($show != "0" and $show != "1") {
        $show = "0";
    }
    return $show;
}


function getToggleNames() {

    static $toggleNames = array();

    if (count($toggleNames) > 0) { return $toggleNames; }
    
    $testDir = getDefaultTest();

    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();

    $d = @dir("$testDir");
    $toggles = array();
    while( false !== ($f = $d->read())) {
        if (! preg_match("/.(png|PNG|jpg|JPG)/", $f)) { continue; }
        if (! preg_match("/$active/", $f)) { continue; }
        $tog = preg_replace("/^.*\.([^-.]+)[\.-].*$/", "$1", $f);
        if ($tog != $f) {
            $toggles[$tog] = 1;
        }
    }
    $toggleNames = array_keys($toggles);
    sort($toggleNames);
    
    return $toggleNames;
}

function getToggle() {
    $toggleNames = getToggleNames();
    $n = count($toggleNames);

    if ($n == 0) {
        return ".*";
    }

    $toggle = 0;
    if (array_key_exists('toggle', $_GET)) {
        $toggle = $_GET['toggle'];
        setcookie('displayQA_toggle', $toggle);
    } elseif (array_key_exists('displayQA_toggle', $_COOKIE)) {
        $toggle = $_COOKIE['displayQA_toggle'];
    }
    if (!preg_match("/^\d+$/", $toggle)) {
        $toggle = 0;
    }
    if ($toggle > $n-1) {
        $toggle = 0;
    }

    $toggleName = $toggleNames[$toggle];
    return $toggleName;
}



function getDefaultTest() {

    $testDir = "";
    if (array_key_exists('test', $_GET)) {
        $testDir = $_GET['test'];
        setcookie('displayQA_test', $testDir);
    } elseif (array_key_exists('displayQA_test', $_COOKIE)) {
        $testDir = $_COOKIE['displayQA_test'];
    }

    # if it didn't get set, or if it doesn't exists (got deleted since cookie was set.
    # ... use the first available test directory
    if (strlen($testDir) == 0 or !file_exists($testDir)) {
        $d = @dir(".");
        $foundOne = false;
        while(false !== ($f = $d->read())) {
            if (preg_match("/test_/", $f) and is_dir($f)) {
                $testDir = $f;
                $foundOne = true;
                break;
            }
        }
        if (!$foundOne) {
            $testDir = "";
        }
    }
    return $testDir;
}


function haveMaps($testDir) {
    if (!file_exists($testDir)) {
        return array(false, "");
    }
    $d = @dir("$testDir");
    $haveMaps = false;
    $navmapFile = "";
    while(false !== ($f = $d->read())) {
        ##p($f,1);
        if (preg_match("/\.navmap/", $f)) {$navmapFile =  "$testDir/$f";}
        if (preg_match("/\.(map|navmap)$/", $f)) { $haveMaps = true; }
    }
    return array($haveMaps, $navmapFile);
}
function getActive() {

    $testDir = getDefaultTest();

    # if there are no tests yet, just default to .*
    if (strlen($testDir) < 1) {
        return ".*";
    }
    
    # see if there are maps associated with this
    list($haveMaps, $navmapFile) = haveMaps($testDir);
    #p($testDir); p($haveMaps); p($navmapFile,1);
    
    # validation list from the navmap file
    # ... a bit excessive to read the whole file, but it should only contain max ~100 entries.
    $validActive = array("all", ".*");
    if (strlen($navmapFile) > 0) {
        $lines = file($navmapFile);
        foreach ($lines as $line) {
            ##p($line, 1);
            $arr = preg_split("/\s+/", $line);
            $validActive[] = $arr[0];
        }
    }
    
    # if there are .map files, the default is a *_all.png file
    $active = $haveMaps ? "all" : ".*";
    if (array_key_exists('active', $_GET) and in_array($_GET['active'], $validActive)) {
        $active = $_GET['active'];
        setcookie('displayQA_active', $active);

    # get a value stored as a cookie, but not if the test changed (then use the default)
    } elseif (array_key_exists('displayQA_active', $_COOKIE) and
              (in_array($_COOKIE['displayQA_active'], $validActive)) and 
              (!array_key_exists('test', $_GET))) {
        $active = $_COOKIE['displayQA_active'];
        if ($haveMaps and preg_match("/\.\*/", $active)) {
            $active = "all";
        }
    }

    
    return $active;    
}


####################################################
# groups
####################################################
function getGroupListFromCache() {
    
    list($results, $nGrp) = loadCache();
    if ($results == -1) {
        return -1;
    }
    
    $groups = array();
    #$entrytime = 0;
    foreach ($results as $r) {
        $testDir = $r['test'];
        $parts = preg_split("/_/", $testDir);

        if (count($parts) > 2) {
            $group = $parts[1];
        } else {
            $group = "";
        }
        
        if (array_key_exists($group, $groups)) {
            $groups[$group] += 1;
        } else {
            $groups[$group] = 1;
        }
    }
    ksort($groups);
    return $groups;
}

function getGroupList() {

    $fromCache = getGroupListFromCache();
    if ($fromCache != -1) {
        return $fromCache;
    }
    
    $dir = "./";
    $groups = array();
    $d = @dir($dir) or dir("");
    while(false !== ($testDir = $d->read())) {
        $parts = preg_split("/_/", $testDir);

        if (count($parts) > 2) {
            $group = $parts[1];
        } else {
            $group = "";
        }
        
        if (array_key_exists($group, $groups)) {
            $groups[$group] += 1;
        } else {
            $groups[$group] = 1;
        }
    }
    ksort($groups);
    return $groups;
}
function getGroup() {
   if (array_key_exists('group', $_GET)) {
        $group = $_GET['group'];
        setcookie('displayQA_group', $group);
   } elseif (array_key_exists('displayQA_group', $_COOKIE)) {
       $group = $_COOKIE['displayQA_group'];
   } else {
       $group = "";
   }

   $allGroups = array_keys(getGroupList());
   # if we don't have the group, default to ""
   if (strlen($group) > 0 and ! in_array($group, $allGroups)) {
       $group = "";
   }
   
   return $group;
}





####################################################
#
####################################################
function getTimeStampsFromCache() {

    list($results, $nGrp) = loadCache();
    if ($results == -1) {
        return -1;
    }
    
    $tstamps = array();
    foreach ($results as $r) {
        $ts = $r['entrytime'];
        if (array_key_exists('newest', $r)) {
            $ts = $r['newest'];
        }
        $ts = intval($ts);
        if ($ts > 0) {
            $tstamps[] = $ts;
        }
    }
    if (count($tstamps) > 0) {
        $min = min($tstamps);
        $max = max($tstamps);
    } else {
        $min = 0;
        $max = 0;
    }
    return array($min, $max);
}


function writeTable_timestamps($group=".*") {

    $minmax = getTimeStampsFromCache();
    if ($minmax == -1) {
        $i = 0;
        $min = 0;
        $max = 0;
        $n = 0;
        
        $dirs = glob("test_".$group."_*"); #array();
        sort($dirs);
        
        #$d = @dir(".");
        foreach($dirs as $f) {
            #while(false !== ($f = $d->read())) {
            if (! is_dir($f) or preg_match("/^\./", $f)) { continue; }
            
            #if (! preg_match("/^test_$group/", $f)) { continue; }
            
            $db = connect($f);
            $cmd = "select count(entrytime),min(entrytime),max(entrytime) from summary";
            $prep = $db->prepare($cmd);
            $prep->execute();
            $results = $prep->fetchAll();
            $db = null;
            $result = $results[0];
            $thisN = 0;
            if ($i == 0 or $n == 0) {
                list($thisN, $min,$max) = $result;
            }
            $n += $thisN;
            
            if ($result[0] > 0) {
                if ($result[1] < $min) { $min = $result[1]; }
                if ($result[2] > $max) { $max = $result[2]; }
            }
            
            $i += 1;
        }
    } else {
        list($min, $max) = $minmax;
    }

    
    $table = new Table("width=\"80%\"");
    $table->addHeader(array("Oldest Entry", "Most Recent Entry"));
    $now = time();
    $oldest = date("Y-m-d H:i:s", $min);
    $latest = date("Y-m-d H:i:s", $max);

    if ($now - $max < 120) {
        $latest .= "<br/><font color=\"#880000\">(< 2m ago, testing in progress)</font>";
    }
    $table->addRow(array($oldest, $latest));

    return "<h2>Timestamps</h2>\n".$table->write();
}



####################################################
#
####################################################
function writeTable_ListOfTestResults() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();
    
    $table = new Table("width=\"90%\"");

    $headAttribs = array("align=\"center\"");
    $table->addHeader(
        array("Label", "Timestamp", "Value", "Limits", "Comment"),
        $headAttribs
        );

    global $dbFile;

    $db = connect($testDir);
    if (! $db) { return "Unable to query database for $testDir."; }
    $cmd = "select * from summary order by label";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $result = $prep->fetchAll();
    $db = null;
    
    $tdAttribs = array("align=\"left\"", "align=\"center\"",
                       "align=\"right\" width=\"50\"", "align=\"center\"",
                       "align=\"left\" width=\"200\"");

    # sort the values so the failures come up on top
    $passed = array();
    $failed = array();
    foreach ($result as $r) {
        list($test, $lo, $value, $hi, $comment) =
            array($r['label'], $r['lowerlimit'], $r['value'], $r['upperlimit'], $r['comment']);
        $cmp = verifyTest($value, $lo, $hi);
        if ($cmp) {
            $failed[] = $r;
        } else {
            $passed[] = $r;
        }
    }
    $result = array_merge($failed, $passed);
    foreach ($result as $r) {
        list($test, $lo, $value, $hi, $comment) =
            array($r['label'], $r['lowerlimit'], $r['value'], $r['upperlimit'], $r['comment']);

        if (preg_match("/NaN/", $lo)) { $lo = NULL; }
        if (preg_match("/NaN/", $hi)) { $hi = NULL; }
        
        if (! preg_match("/$active/", $test) and ! preg_match("/all/", $active)) { continue; }
        
        $cmp = verifyTest($value, $lo, $hi);
        
        if ($cmp) {
            $test .= " <a href=\"backtrace.php?label=$test\">Backtrace</a>";
        }

        # allow the test to link to
        $labelWords = preg_split("/\s+-\*-\s+/", $r['label']);
        $thisLabel = $labelWords[count($labelWords)-1]; # this might just break ...
        $test .= ", <a href=\"summary.php?test=$testDir&active=$thisLabel\">Active</a>";

        $mtime = date("Y-m-d H:i:s", $r['entrytime']);

        $loStr = $lo ? sprintf("%.4f", $lo) : "None";
        $hiStr = $hi ? sprintf("%.4f", $hi) : "None";

        $table->addRow(array($test, $mtime,
                             hiLoColor($value, $lo, $hi), "[$loStr, $hiStr]", $comment), $tdAttribs);
    }

    return $table->write();
    
}
function displayTable_ListOfTestResults($testDir) {
    echo writeTable_ListOfTestResults($testDir);
}



function writeTable_OneTestResult($label) {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    
    if (empty($label)) {
        return "<h2>No test label specified. Cannot display test result.</h2><br/>\n";
    }
    
    $table = new Table("width=\"90%\"");
    
    $headAttribs = array("align=\"center\"");
    $table->addHeader(
        array("Label", "Timestamp", "Value", "Limits", "Comment"),
        $headAttribs
        );
    #$table->addHeader(array("Label", "Timestamp", "LowerLimit", "Value", "UpperLimit", "Comment"));

    global $dbFile;
    #$mtime = date("Y-m_d H:i:s", filemtime("$testDir/$dbFile"));
    $db = connect($testDir);
    $cmd = "select * from summary where label = ?";
    $prep = $db->prepare($cmd);
    $prep->execute(array($label));
    $result = $prep->fetchAll();
    $db = null;
    
    $tdAttribs = array("align=\"left\"", "align=\"center\"",
                       "align=\"right\"", "align=\"center\"",
                       "align=\"left\" width=\"200\"");
    foreach ($result as $r) {
        list($test, $timestamp, $lo, $value, $hi, $comment, $backtrace) =
            array($r['label'], $r['entrytime'], $r['lowerlimit'], $r['value'], $r['upperlimit'],
                  $r['comment'], $r['backtrace']);
        
        $cmp = verifyTest($value, $lo, $hi);
        if (!$lo) { $lo = "None"; }
        if (!$hi) { $hi = "None"; }

        $mtime = date("Y-m_d H:i:s", $r['entrytime']);

        $loStr = sprintf("%.3f", $lo);
        $hiStr = sprintf("%.3f", $hi);
        $valueStr = sprintf("%.3f", $value);

        $table->addRow(array($test, $mtime,
                             hiLoColor($valueStr, $lo, $hi), "[$loStr, $hiStr]", $comment), $tdAttribs);
        #$table->addRow(array($test, date("Y-m-d H:i:s", $timestamp),
        #                    $lo, hiLoColor($value, $pass), $hi, $comment));
    }

    return $table->write();
}
function write_OneBacktrace($label) {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    
    $out = "<h2>Backtrace</h2><br/>\n";
    if (empty($label)) {
        return "<b>No test label specified. Cannot display backtrace.</b><br/>\n";
    }
    
    global $dbFile;
    $db = connect($testDir);
    $cmd = "select * from summary where label = ?";
    $prep = $db->prepare($cmd);
    $prep->execute(array($label));
    $result = $prep->fetchAll();
    $db = null;
    
    $backtrace = "";
    foreach ($result as $r) {
        $backtrace .= $r['backtrace'];
    }

    $out .= preg_replace("/\n/", "<br/>\n", $backtrace);
    $out = preg_replace("/(\t|\s{4})/", "&nbsp;&nbsp;", $out);
    
    return $out;
    
}
function displayTable_OneTestResult($testDir, $label) {
    echo writeTable_OneTestResult($testDir);
}



function writeTable_summarizeMetadata($keys, $group=".*") {

    $tables = "";

    $dir = "./";

    #$d = @dir($dir) or dir("");
    if ($group === '.*') {
        $dirs = getAllTestDirs();
        $datasets = getDataSets();
    } else {
        $dirsByGroup = getAllTestDirsByGroup();
        if (array_key_exists($group, $dirsByGroup)) {
            $dirs = $dirsByGroup[$group];
        } else {
            $dirs = array();
        }
        $datasetsByGroup = getDataSetsByGroup();
        if ($datasetsByGroup != -1 and array_key_exists($group, $datasetsByGroup)) {
            $datasets = $datasetsByGroup[$group];
        } else {
            $datasets = -1;
        }
    }
    
    foreach ($keys as $key) {

        if (preg_match("/[dD]escription/", $key)) {
            continue;
        } 
        
        $meta = new Table();
        $meta->addHeader(array("$key"));
        $values = array();

        #dataset is in the cache, so we can skip the directory listing
        if (($key == 'dataset') and ($datasets != -1) ) {

            $values = array_merge($values, $datasets);
            foreach (array_unique($datasets) as $value) {
                if ($value != "unknown") {
                    $meta->addRow(array("$value"));
                }
            }
        # other keys we'll do the search
        } else {
            foreach ($dirs as $testDir) {
                
                $db = connect($testDir);
                $cmd = "select key, value from metadata where key = ?";
                $prep = $db->prepare($cmd);
                $prep->execute(array($key));
                $results = $prep->fetchAll();
                $db = null;
                
                foreach ($results as $r) {
                    $values[] = $r['value'];
                }
            }
            foreach (array_unique($values) as $value) {
                $meta->addRow(array("$value"));
            }
        }
        if (count($values) > 0) {
            $tables .= $meta->write();
        }
    }

    return $tables;
    
}


function getDescription() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();
    $db = connect($testDir);
    $cmd = "select key, value from metadata";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $results = $prep->fetchAll();
    $db = null;
    
    $description = "";
    foreach ($results as $r) {
        if (preg_match("/[dD]escription/", $r['key'])) {
            $description = $r['value'];
        }
    }
    
    $link = "Description: <a href=\"#\" id=\"displayText\"></a><br/><br/>\n".
        "<div id=\"toggleText\" style=\"display:none\">$description<br/></div>\n";
    
    return $link;

}

function getDescription2() {

    $show = getShowHide();
    
    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();
    $db = connect($testDir);
    $cmd = "select key, value from metadata";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $results = $prep->fetchAll();
    $db = null;
    
    $description = "";
    foreach ($results as $r) {
        if (preg_match("/[dD]escription/", $r['key'])) {
            $description = $r['value'];
        }
    }

    $out = "";
    if ($show == "1") {
        $link = "<a href=\"summary.php?test=$testDir&active=$active&show=0\">[hide description]</a>";
        $out = $link . " ". $description . "<br/>\n";
    } else {
        $link = "<a href=\"summary.php?test=$testDir&active=$active&show=1\">[show description]</a><br/>\n";
        $out = $link;
    }
    return $out;
}


function getToggleLinks() {
    $show = getShowHide();
    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();
    
    $toggleNames = getToggleNames();
    $toggle = getToggle();

    $links = array();
    if (count($toggleNames) > 0) {
        $links[] = "Display figure: ";
    }
    $i = 0;
    foreach ($toggleNames as $toggleName) {
        if ($toggleName == $toggle) {
            $links[] = $toggleName;
        } else {
            $links[] = "<a href=\"summary.php?test=$testDir&active=$active&show=$show&toggle=$i\">$toggleName</a><br/>\n";
        }
        $i += 1;
    }
    $table = new Table();
    $table->addRow($links, array("align=\"left\" width=\"100\""));
    return $table->write();
    
}

function writeTable_metadata() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();
        
    $meta = new Table();
    
    $db = connect($testDir);
    $cmd = "select key, value from metadata";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $results = $prep->fetchAll();
    $db = null;
    
    foreach ($results as $r) {
        if (preg_match("/[dD]escription/", $r['key'])) {
            continue;
        } 
        $meta->addRow(array($r['key'].":", $r['value']));
    }
    $meta->addRow(array("Active:", $active));
    return $meta->write();
}




####################################################
#
# Figures
#
####################################################
 
function writeMappedFigures($suffix="map") {

    $testDir = getDefaultTest();

    if (strlen($testDir) < 1) {
        return "";
    }
    $active = getActive();

    $figNum = ($suffix=="map") ? 2 : 1;
    $j = 0;
    $out = "";

    $d = @dir("$testDir");
    $imFiles = array();
    while( false !== ($f = $d->read())) {
        if (! preg_match("/.(png|PNG|jpg|JPG)/", $f)) { continue; }
        $imFiles[] = $f;
    }
    asort($imFiles);
    
    $toggle = getToggle();
    
    foreach ($imFiles as $f) {
        $base = preg_replace("/\.(png|PNG|jpg|JPG)/", "", $f);
        $mapfile = $base . "." . $suffix;

        if (! preg_match("/${toggle}.*$active/", $f) and $suffix != 'navmap') { continue; }

        # get the image path
        $path = "$testDir/$f";
        $mtime = date("Y-m_d H:i:s", filemtime($path));
        $mapPath = "$testDir/$mapfile";

        if (! file_exists($mapPath)) { continue; }
        
        # get the caption
        $db = connect($testDir);
        $cmd = "select caption from figure where filename = ?";
        $prep = $db->prepare($cmd);
        $prep->execute(array($f));
        $result = $prep->fetchColumn();
        $db = null;
        
        # load the map
        $mapString = "<map id=\"$base\" name=\"$base\">\n";
        $mapList = file($mapPath);
        $activeArea = array(0.0, 0.0, 0.0, 0.0);
        $activeTooltip = "";
        foreach($mapList as $line) {
            list($label, $x0, $y0, $x1, $y1, $info) = preg_split("/\s+/" ,$line);
            if (preg_match("/^nolink:/", $info)) {
                $tooltip = preg_replace("/^nolink:/", "", $info);
                $mapString .= sprintf("<area shape=\"rect\" coords=\"%d,%d,%d,%d\" title=\"%s\">\n",
                                      $x0, $y0, $x1, $y1, $tooltip);
            } else {
                $href = "summary.php?test=$testDir&active=$label";
                $tooltip = $label." ".$info;
                $mapString .= sprintf("<area shape=\"rect\" coords=\"%d,%d,%d,%d\" href=\"%s\" title=\"%s\">\n",
                                      $x0, $y0, $x1, $y1, $href, $tooltip);
            }
            if ( preg_match("/$active/", $label) ) {
                $activeArea = array($x0, $y0, $x1, $y1);
                $activeTooltip = $tooltip;
            }
        }
        $mapString .= "</map>\n";


        # make the img tag and wrap it in a highlighted div
        $imgDiv = new Div("style=\"position: relative;\"");
        list($x0, $y0, $x1, $y1) = $activeArea;
        # not sure why these are too big ...
        $dx = 1.0*(intval($x1) - intval($x0));
        $dy = 1.0*(intval($y1) - intval($y0));

        $hiliteColor2 = "magenta"; #"#00ff00";
        $hiliteColor1 = "#00ff00";
        if ($suffix == "navmap" and $x0 > 0 and $y0 > 0) {
            $wid = 2;
            $hilightDiv1 = new Div("style=\"position: absolute; left: ${x0}px; top: ${y0}px; width: ${dx}px; height: ${dy}px; border: $hiliteColor1 ${wid}px solid; z-index: 0;\" align=\"center\" title=\"$activeTooltip\"");
            list($x0, $y0, $dx, $dy) = array($x0 + $wid, $y0 + $wid, $dx-2*$wid, $dy-2*$wid);
            $hilightDiv2 = new Div("style=\"position: absolute; left: ${x0}px; top: ${y0}px; width: ${dx}px; height: ${dy}px; border: $hiliteColor2 ${wid}px solid; z-index: 0;\" align=\"center\" title=\"$activeTooltip\"");
            $imgDiv->append($hilightDiv1->write());
            $imgDiv->append($hilightDiv2->write());
        }

        if (preg_match("/.tiff$/", $path)) {
            $imgTag = "<object data=\"$path\" type=\"image/tiff\" usemap=\"#$base\"><param name=\"negative\" value=\"yes\"></object>\n";
        } else {
            $imgTag = "<img src=\"$path\" usemap=\"#$base\">\n";
        }

        $imgDiv->append($imgTag);

        
        $img = new Table();
        if ($suffix == 'navmap') {
            $img->addRow(array("Show <a href=\"summary.php?test=$testDir&active=all\">all</a>"));
        }
        $img->addRow(array($imgDiv->write() )); #"<center>".$imgDiv->write()."</center>"));
        $img->addRow(array("<b>Figure $figNum.$j</b>: $result"));
        $img->addRow(array("<b>$f</b>: timestamp=$mtime"));

        $out .= $img->write();
        $out .= $mapString;
        $out .= "<br/>";
        
        $j += 1;
    }
    return $out;
}


function writeFigures() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    
    $active = getActive();
    $d = @dir($testDir);

    $figures = array();
    while( false !== ($f = $d->read())) {
        if (! preg_match("/.(png|PNG|jpg|JPG)/", $f)) { continue; }
        if (! preg_match("/$active/", $f)) { continue; }
        $figures[] = $f;
    }
    asort($figures);
    
    $toggle = getToggle();

    
    ## get the captions
    $db = connect($testDir);
    $cmd = "select filename, caption from figure";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $results = $prep->fetchAll();
    $db = null;

    $captions = array();
    foreach ($results as $r) {
        $captions[$r['filename']] = $r['caption'];
    }
    
    $j = 0;
    $out = "";
    foreach ($figures as $f) {

        if (! preg_match("/$toggle/", $f) ) { continue; }
        
        # get the image path
        $path = "$testDir/$f";

        # skip mapped files (they're obtained with writeMappedFigures() )
        $base = preg_replace("/.(png|PNG|jpg|JPG)/", "", $path);
        $map = $base . ".map";
        $navmap = $base . ".navmap";
        if (file_exists($map) or file_exists($navmap)) {
            continue;
        }

        $mtime = date("Y-m_d H:i:s", filemtime($path));

        # tiff must be handled specially
         
        if (preg_match("/.tiff$/", $path)) {
            # this doesn't work.  tiffs disabled for now.
            $imgTag = "<object data=\"$path\" type=\"image/tiff\"><param name=\"negative\" value=\"yes\"></object>";
        } else {
            $imgTag = "<img src=\"$path\">";
        }
        
        $img = new Table();
        $img->addRow(array("$imgTag")); #<center>$imgTag</center>"));
        $img->addRow(array("<b>Figure 2.$j</b>:".$captions[$f]));
        $img->addRow(array("<b>$f</b>: timestamp=$mtime"));
        $out .= $img->write();
        $out .= "<br/>";
        $j += 1;
    }
    
    return $out;
}
function displayFigures($testDir) {
    echo writeFigures($testDir);
}



function summarizeTestsFromCache() {
    
    list($results, $nGrp) = loadCache();
    if ($results == -1) {
        return -1;
    }
    
    $ret = array();
    #$entrytime = 0;
    foreach ($results as $r) {
        $test = $r['test'];
        $ret[$test] = $r;
        #array('name' => $r['test'],
        #                    'entrytime' => $r['entrytime'],
        #                    'npass' => $r['npass'],
        #                    'ntest' => $r['ntest'],
        #                    'oldest' => $['oldest'],
        #    );
    }
    return $ret;

}



function summarizeTestByCounting($testDir) {

    $db = connect($testDir);
    $passCmd = "select * from summary";
    $prep = $db->prepare($passCmd);
    $prep->execute();
    $results = $prep->fetchAll();
    $db = null;

    $nTest = 0;
    $nPass = 0;
    $timestamp = 0;
    foreach($results as $result) {
        if (verifyTest($result['value'], $result['lowerlimit'], $result['upperlimit']) == 0){
            $nPass += 1;
        }
        $timestamp = $result['entrytime'];
        $nTest += 1;
    }

    $ret = array();
    $ret['name'] = $testDir;
    $ret['entrytime'] = $timestamp;
    $ret['ntest'] = $nTest;
    $ret['npass'] = $nPass;
    return $ret;
}

#function summarizeTest($testDir) {
#    $summ = summarizeTestFromCache($testDir);
#    if ($summ == -1) {
#        $summ = summarizeTestByCounting($testDir);
#    }
#    return $summ;
#}




function writeTable_SummarizeAllTests() {
    $dir = "./";

    $msg = "";
    
    $group = getGroup();

    $msg .= "group: $group ".date("H:i:s")."<br/>";

    ## go through all directories and look for .summary files
    $d = @dir($dir) or dir("");
    $dirs = glob("test_".$group."_*"); #array();
    #while(false !== ($testDir = $d->read())) {
    #    $dirs[] = $testDir;
    #}
    sort($dirs);

    $tdAttribs = array("align=\"left\"", "align=\"left\"",
                       "align=\"right\"","align=\"right\"", "align=\"right\"");

    $summs = summarizeTestsFromCache();
    
    $d = @dir($dir) or dir("");
    $table = new Table("width=\"100%\"");

    $head = array("Test", "mtime", "No. Tests", "Pass/Fail", "Fail Rate");
    $table->addHeader($head, $tdAttribs);
    #while(false !== ($testDir = $d->read())) {
    $summAll = 0;
    $passAll = 0;
    foreach ($dirs as $testDir) {
        $msg .= "$testDir  ".date("H:i:s")."<br/>";
        
        # only interested in directories, but not . or ..
        if ( preg_match("/^\./", $testDir) or ! is_dir("$testDir")) {
            continue;
        }

        # only interested in the group requested
        if (! preg_match("/test_".$group."_/", $testDir)) {
            continue;
        }

        # if our group is "" ... ignore other groups
        $parts = preg_split("/_/", $testDir);
        if ( $group == "" and (strlen($parts[1]) > 0)) {
            continue;
        }

        if ($summs == -1 or !array_key_exists($testDir, $summs)) {
            $summ = summarizeTestByCounting($testDir);
        } else {
            $summ = $summs[$testDir];
        }
        list($haveMaps, $navmapFile) = haveMaps($testDir);
        $active = ($haveMaps) ? "all" : ".*";
        $testDirStr = preg_replace("/^test_${group}_/", "", $testDir);
        $testLink = "<a href=\"summary.php?test=${testDir}&active=$active\">$testDirStr</a>";

        $nFail = $summ['ntest'] - $summ['npass'];
        $passLink = tfColor($summ['npass'] . " / " . $nFail, ($summ['npass']==$summ['ntest']));
        $failRate = "n/a";
        if ($summ['ntest'] > 0) {
            $failRate = 1.0 - 1.0*$summ['npass']/$summ['ntest'];
            $failRate = tfColor(sprintf("%.3f", $failRate), ($failRate == 0.0));
        }
        $lastUpdate = $summ['entrytime'];
        if (array_key_exists('newest', $summ)) {
            $lastUpdate = $summ['newest'];
        }
        if ($lastUpdate  > 0) {
            $timestampStr = date("Y-m-d H:i:s", $lastUpdate);
        } else {
            $timestampStr = "n/a";
        }
        
        $table->addRow(array($testLink, $timestampStr,
                             $summ['ntest'], $passLink, $failRate), $tdAttribs);
        $summAll += $summ['ntest'];
        $passAll += $summ['npass'];
    }
    $failAll = $summAll - $passAll;
    $table->addRow(array("Total", "", $summAll, $passAll." / ".$failAll,
                         sprintf("%.3f", 1.0 - $passAll/($summAll ? $summAll : 1))), $tdAttribs);
    
    return $table->write();
    
}


function loadCache() {

    static $alreadyLoaded = false;
    static $results2 = array();
    static $nGroup = 0;
    
    $results = array();
    if ($alreadyLoaded) {
        return array($results2, $nGroup);
    } else {
        if (!file_exists("db.sqlite3")) {
            return array(-1, -1);
        }
        $db = connect("."); #$testDir);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        try {
            $testCmd = "select * from counts order by test;";
            $prep = $db->prepare($testCmd);
            $prep->execute();
            $results = $prep->fetchAll();
            #$testCmd = "select * from counts order by test where test not like \"test__%\";";
            #$prep = $db->prepare($testCmd);
            #$prep->execute();
            #$resultsB = $prep->fetchAll();
            #$results = array_merge($resultsA, $resultsB);
        } catch (PDOException $e) {
            return array(-1, -1);
        }
        $db = null;
        $alreadyLoaded = true;
    }

    $nInSet = getFloat('ninset', '100');
    $iSet = getFloat('iset', '0');

    $gstart = $iSet*$nInSet + 1;
    $gend = ($iSet+1)*$nInSet + 1;

    $resultsTmp = array();
    foreach($results as $r) {
        if (preg_match("/^test__/", $r['test'])) {
            array_unshift($resultsTmp, $r);
        } else {
            $resultsTmp[] = $r;
        }
    }
    
    $results2 = array();
    $groups = array();
    foreach($resultsTmp as $r) {
        $testDir = $r['test'];
        $dataset = $r['dataset'];
        if (!preg_match("/^test_.*/", $testDir)) {
            continue;
        }
        $parts = preg_split("/_/", $testDir);
        $group = $parts[1];
        $groups[$group] = 1;
        $nGroup = count($groups);
        if ($nGroup >= $gstart and $nGroup < $gend) {
            $results2[] = $r;
        }
        #if ($n >= $gend) { break;}
    }    
    
    return array($results2, $nGroup);
}


function nGroupToggle() {

    list($results, $nGrp) = loadCache();

    # only offer this service if the cache is present
    if ($results == -1) { return ""; }
    
    $iSet = getFloat('iset', '0');
    $nInSet = getFloat('ninset', '100');
    $nSet = intval($nGrp/$nInSet) + 1;

    $out = "Total: $nGrp groups.<br/>";
    $out .= "Display sets of: ";
    foreach (array("50", "100", "200", "500") as $iInSet) {
        if ($iInSet == $nInSet) {
            $out .= "&nbsp; $iInSet &nbsp;";
        } else {
            $out .= "&nbsp; <a href=\"index.php?ninset=$iInSet&iset=0\">$iInSet</a> &nbsp;";
        }
    }
    $out .= "<br/>\n";

    $table = new Table();
    $maxCols = 20;
    $out .= "Show set: <br/>";
    $row = array();
    for ($i = 0; $i < $nSet; $i++) {

        if ($i and $i % $maxCols == 0) {
            $table->addRow($row);
            $row = array();
        }
        
        if ($i == $iSet) {
            $row[] = "&nbsp; $i &nbsp;";
        } else {
            $row[] = "&nbsp; <a href=\"index.php?ninset=$nInSet&iset=$i\">$i</a> &nbsp;\n";
        }
    }
    
    $table->addRow($row);
    return $out . $table->write();
}

function getDataSetsByGroup() {
    list($results, $nGrp) = loadCache();
    if ($results == -1) {
        return -1;
    }

    $dirs = array();
    foreach($results as $r) {
        $testDir = $r['test'];
        $dataset = $r['dataset'];
        if (!preg_match("/^test_.*/", $testDir)) {
            continue;
        }
        $parts = preg_split("/_/", $testDir);
        $group = $parts[1];

        if (array_key_exists($group, $dirs)) {
            $dirs[$group][] = $dataset;
        } else {
            $dirs[$group] = array($dataset);
        }
    }
    asort($dirs);
    return $dirs;
}
function getDataSets() {
    $datasetsByGroup = getDataSetsByGroup();
    if ($datasetsByGroup == -1) {
        return -1;
    }
    $datasets = array();
    foreach ($datasetsByGroup as $k=>$v) {
        $datasets = array_merge($datasets, $v);
    }
    return $datasets;
}


function getAllTestDirs() {
    $dirsByGroup = getAllTestDirsByGroup();
    $dirs = array();
    foreach ($dirsByGroup as $k=>$v) {
        $dirs = array_merge($dirs, $v);
    }
    return $dirs;
}
function getAllTestDirsByGroupFromCache() {

    list($results, $nGrp) = loadCache();
    if ($results == -1) {
        return -1;
    }
    
    $dirs = array();
    foreach($results as $r) {
        $testDir = $r['test'];
        if (substr($testDir, 0, 5) != 'test_') {
            continue;
        }
        #if (!preg_match("/^test_.* /", $testDir)) { continue; }

        $parts = preg_split("/_/", $testDir);
        $group = $parts[1];
        if (array_key_exists($group, $dirs)) {
            $dirs[$group][] = $testDir;
        } else {
            $dirs[$group] = array($testDir);
        }
    }
    asort($dirs);
    return $dirs;
}
function getAllTestDirsByGroup() {

    $dirs = getAllTestDirsByGroupFromCache();
    if ($dirs != -1) {
        return $dirs;
    }
    
    $dir = "./";
    $d = @dir($dir) or dir("");
    $dirs = array();
    while(false !== ($testDir = $d->read())) {
        if (!preg_match("/^test_.*/", $testDir)) {
            continue;
        }
        $parts = preg_split("/_/", $testDir);
        $group = $parts[1];
        if (array_key_exists($group, $dirs)) {
            $dirs[$group][] = $testDir;
        } else {
            $dirs[$group] = array($testDir);
        }
    }
    asort($dirs);
    return $dirs;
}

function writeTable_SummarizeAllGroups() {

    $groups = getGroupList();
    #echo "have groups<br/>";

    $summs = summarizeTestsFromCache();
    #echo "have summs<br/>";
    
    $dirs = getAllTestDirsByGroup();
    #echo "have testdirs<br/>";

    #".*-u$" => array(),
    #".*-g$" => array(),
    #".*-r$" => array(),
    #".*-i$" => array(),
    #".*-z$" => array(),
    #".*-y$" => array()
    
    $specialGroups = array(
        ".*" => array(),
        ".*[12]-.$" => array(),
        ".*0-.$" => array(),
        ".*0-u$" => array(),
        ".*0-g$" => array(),
        ".*0-r$" => array(),
        ".*0-i$" => array(),
        ".*0-z$" => array(),
        ".*0-y$" => array(),
        ".*[12]-u$" => array(),
        ".*[12]-g$" => array(),
        ".*[12]-r$" => array(),
        ".*[12]-i$" => array(),
        ".*[12]-z$" => array(),
        ".*[12]-y$" => array()
        );
    $specialGroupLabels = array(
        ".*" => "all data",
        ".*[12]-.$" => "all cloudless",
        ".*0-.$" => "all cloud",
        ".*0-u$" => "u cloud",
        ".*0-g$" => "g cloud",
        ".*0-r$" => "r cloud",
        ".*0-i$" => "i cloud",
        ".*0-z$" => "z cloud",
        ".*0-y$" => "y cloud",
        ".*[12]-u$" => "u cloudless",
        ".*[12]-g$" => "g cloudless",
        ".*[12]-r$" => "r cloudless",
        ".*[12]-i$" => "i cloudless",
        ".*[12]-z$" => "z cloudless",
        ".*[12]-y$" => "y cloudless"
        );
    
    ## go through all directories
    # get the extraKeys
    $extraKeys = array();
     
    foreach ($groups as $group=>$n) {
        if (!array_key_exists($group, $dirs)) {
            continue;
        }
        foreach ($dirs[$group] as $testDir) {
            # must deal with default group "" specially
            $parts = preg_split("/_/", $testDir);
            if (strlen($parts[1]) > 0 and $group == "") { continue;}

            if ($summs == -1 or !array_key_exists($testDir, $summs)) {
                $summ = summarizeTestByCounting($testDir);
            } else {
                $summ = $summs[$testDir];
            }
            
            # if there's extra data from the cache, build an array of key=>array(values)
            if (array_key_exists('extras', $summ) and strlen($summ['extras']) > 0) {
                $kves = preg_split("/,/", $summ['extras']);
                $extras[$group] = array();
                foreach ($kves as $kve) {
                    list($k, $v, $e, $u) = preg_split("/:/", $kve);
                    $extraKeys[$k] = 1;
                }
            }
        }
    }
    $extraKeys = array_keys($extraKeys);
    sort($extraKeys);
    
    $extras = array();

    $iSet = getFloat('iset', '0');
    $nInSet = getFloat('ninset', '100');
    $groupShowing = array();
    $rows = array();
    $iGroup = $iSet*$nInSet + 1;
    foreach ($groups as $group=>$n) {

        #echo "group: ".$group."<br/>";
        $nTestSets = 0;
        $nTestSetsPass = 0;
        $nTest = 0;
        $nPass = 0;
                
        $lastUpdate = 0;
        #$d = @dir($dir) or dir("");
        #while(false !== ($testDir = $d->read())) {
        if (!array_key_exists($group, $dirs)) {
            continue;
        }

        #echo "$group<br/>";
        foreach ($dirs[$group] as $testDir) {

            # must deal with default group "" specially
            $parts = preg_split("/_/", $testDir);
            if (strlen($parts[1]) > 0 and $group == "") {
                continue;
            }

            if ($summs == -1 or !array_key_exists($testDir, $summs)) {
                $summ = summarizeTestByCounting($testDir);
            } else {
                $summ = $summs[$testDir];
            }

            $nTestSets += 1;
            $nTest += $summ['ntest'];
            $nPass += $summ['npass'];
            if ($summ['ntest'] == $summ['npass']) {
                $nTestSetsPass += 1;
            }
            if (array_key_exists('newest', $summ)) {
                if ($summ['newest'] > $lastUpdate) {
                    $lastUpdate = $summ['newest'];
                }
            } else {
                if ($summ['entrytime'] > $lastUpdate) {
                    $lastUpdate = $summ['entrytime'];
                }
            }

            # if there's extra data from the cache, build an array of key=>array(values)
            if (array_key_exists('extras', $summ) and strlen($summ['extras']) > 0) {
                $kves = preg_split("/,/", $summ['extras']);
                if (!array_key_exists($group, $extras)) {
                    $extras[$group] = array();
                }
                foreach ($kves as $kve) {
                    list($k, $v, $e, $u) = preg_split("/:/", $kve);
                    $extras[$group][$k] = array(floatval($v), floatval($e), $u);
                }
            }
                
        }

        # don't bother posting a TestSet with no Tests (ie. an empty directory)
        if ($nTest == 0) {
            continue;
        }
        
        if ($group == "") {
            $testLink = "<a href=\"group.php?group=\">Top level</a>";
        } else {
            $testLink = "<a href=\"group.php?group=$group\">$group</a>";
        }

        $nFail = $nTest - $nPass;
        $failRate = 0; #"n/a";
        if ($nTest > 0) {
            $failRate = 1.0 - 1.0*$nPass/$nTest;
            #$failRate = tfColor(sprintf("%.3f", $failRate), ($failRate == 0.0));
        }
        if ($lastUpdate > 0) {
            $timestampStr = date("Y-m-d H:i", $lastUpdate);
        } else {
            $timestampStr = "n/a";
        }
        $passLink = tfColor(sprintf("$nFail / %.1f", 100.0*$failRate), ($nPass==$nTest));

        $nTestSetsFail = $nTestSets - $nTestSetsPass;
        $row = array($iGroup, $testLink, $timestampStr,
                     "$nTestSets / $nTestSetsFail", $nTest, $passLink);
        $rows[] = $row;
        $groupShowing[] = $group;
        $iGroup += 1;


        # see if this is a special group
        foreach ($specialGroups as $sg => $arr) {
            if (preg_match("/$sg/", $group)) {
                if (count($arr) == 0) {
                    $arr = array(0, 0, 0, 0, 0);
                    #foreach ($extraKeys as $ek) {
                    #    $arr[] = array();
                    #}
                }
                $arr[0] += $nTestSets;
                $arr[1] += $nTestSetsPass;
                $arr[2] += $nTest;
                $arr[3] += $nPass;
                $arr[4] += 1;
                $i = 5;
                foreach ($extraKeys as $k) {

                    if (array_key_exists($group, $extras) and array_key_exists($k, $extras[$group])) {
                        list($val,$err,$unit) = $extras[$group][$k];
                        if (!array_key_exists($i, $arr)) {
                            $arr[$i] = array();
                        }
                        $arr[$i][]  = $val;
                        $i += 1;
                    }
                }
                $specialGroups[$sg] = $arr;

            }
        }
    }

    
    $sgRows = array();
    foreach ($specialGroups as $sg => $arr) {
        if (count($arr) == 0) {
            continue;
            #$arr = array(0, 0, 0, 0);
        }
        $nTestSets = $arr[0];
        $nTestSetsPass = $arr[1];
        $nTest = $arr[2];
        $nPass = $arr[3];
        $nMatch = $arr[4];

        
        $nTestSetsFail = $nTestSets - $nTestSetsPass;
        $nFail = $nTest - $nPass;
        $failRate = ($nTest > 0) ? sprintf("%.3f", 1.0*$nFail/$nTest) : 0; #"n/a";
        #$failRate = tfColor(sprintf("%.3f", $failRate), ($failRate == 0.0));

        $passLink = tfColor(sprintf("$nFail / %.1f", 100.0*$failRate), ($nPass==$nTest));
        
        $row = array("n=".$nMatch, $specialGroupLabels[$sg], "n/a",
                     "$nTestSets / $nTestSetsFail", $nTest, $passLink);

        $i = 5;
        foreach ($extraKeys as $k) {
            if (array_key_exists($i, $arr)) {
                $values = $arr[$i];
                $mean = array_sum($values)/$nMatch;
                $stdev = stdev($values);
                if (preg_match("/^n/", $k)) {
                    $entry = sprintf("<div title=\"&plusmn;%d\">%d</div>", $stdev, $mean);
                } else {
                    $fmt = ($mean > 0.1) ? "%.2f" : "%.3f";
                    $entry = sprintf("<div title=\"&plusmn;$fmt\">$fmt</div>", $stdev, $mean);
                }                    
                    
                $row[] = $entry;
            } else {
                $row[] = "";
            }
            $i += 1;
        }
        
        $sgRows[] = $row;
    }
    $spaceRow = array("&nbsp;", "", "", "", "", "", "", "");
    

    
    $table = new Table("width=\"100%\"");
    #$tdAtt = array();
    $pSymb = "Pass"; #"<font color=\"#009900\">&#x2713;</font>";
    $fSymb = "Fail"; #"<font color=\"#990000\">X</font>";
    $head= array("No.", "Test", "mtime", "Sets/$fSymb", "Tests", "$fSymb / %");
    $tdAttribs = array("align=\"left\"", "align=\"left\"", # "align=\"left\"",
                       "align=\"right\"", "align=\"right\"",
                       "align=\"right\"", "align=\"right\"");

    foreach ($extraKeys as $k) {
        $head[] = $k;
        $tdAttribs[] = "align=\"right\" width=\"40\"";
        $spaceRow[] = "";
    }
    $sgRows[] = $spaceRow;
    
    $table->addHeader($head, $tdAttribs);
    foreach ($sgRows as $row) {
        #foreach ($extraKeys as $k=>$v) {
        #    $row[] = "";
        #}
        $table->addRow($row, $tdAttribs);
    }
    
    for($i=0; $i < count($rows); $i++) {
        $row = $rows[$i];
        $group = $groupShowing[$i];
        foreach ($extraKeys as $k) {
            if (array_key_exists($group, $extras) and array_key_exists($k, $extras[$group])) {
                list($v, $e, $u) = $extras[$group][$k];
                if (preg_match("/^n/", $k)) {
                    $row[] = sprintf("<div title=\"&plusmn;%d %s\">%d</div>", $e, $u, $v);
                } else {
                    $fmt = (floatval($v) < 0.1) ? "%.3f" : "%.2f";
                    $row[] = sprintf("<div title=\"&plusmn;$fmt %s\">$fmt</div>", $e, $u, $v);
                }
            } else {
                $row[] = "";
            }
        }
        $table->addRow($row, $tdAttribs);
    }
    return $table->write();
    
}

function displayTable_SummarizeAllTests() {
    echo writeTable_SummarizeAllTests($group);
}




####################################################
# Logs
####################################################

function writeTable_Logs() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    
    $db = connect($testDir);

    # first get the tables ... one for each ccd run
    $cmd = "select name from sqlite_sequence where name like 'log%'";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $db = null;
    $dbtables = $prep->fetchAll();
    
    # make links at the top of the page
    $tables = "";
    $ul = new UnorderedList();
    foreach ($dbtables as $dbtable) {

        $name = $dbtable['name'];
        $ul->addItem("<a href=\"#$name\">$name</a>");
        
        $cmd = "select * from ?";
        $prep = $db->prepare($cmd);
        $prep->execute(array($name));
        $logs = $prep->fetchAll();

        $tables .= "<h2 id=\"$name\">$name</h2><br/>";
        
        $table = new Table("width=\"80%\"");
        $table->addHeader(array("Module", "Message", "Date", "Level"));
        foreach ($logs as $log) {

            # check for tracebacks from TestData
            $module = $log['module'];
            $msg = $log['message'];
            if (preg_match("/testQA.TestData$/", $module)) {
                # get the idString from the message
                $idString = preg_replace("/:.*/", "", $msg);
                $module .= " <a href=\"backtrace.php?label=$idString\">Backtrace</a>";
            }
            $table->addRow(array($module, $msg, $log['date'], $log['level']));
        }
        $tables .= $table->write();
    }
    $contents = "<h2>Data Used in This Test</h2><br/>" . $ul->write() . "<br/><br/>";
    return $contents . $tables;
}

function displayTable_Logs() {
    echo writeTable_Logs();
}




####################################################
# EUPS
####################################################
 
function writeTable_EupsSetups() {

    $testDir = getDefaultTest();
    if (strlen($testDir) < 1) {
        return "";
    }
    $db = connect($testDir);

    # first get the tables ... one for each ccd run
    $cmd = "select name from sqlite_sequence where name like 'eups%'";
    $prep = $db->prepare($cmd);
    $prep->execute();
    $db = null;
    
    $dbtables = $prep->fetchAll();

    # make links at the top of the page
    $tables = "";
    $ul = new UnorderedList();
    foreach ($dbtables as $dbtable) {

        $name = $dbtable['name'];
        $ul->addItem("<a href=\"#$name\">$name</a>");
        
        $cmd = "select * from ?";
        $prep = $db->prepare($cmd);
        $prep->execute(array($name));
        $logs = $prep->fetchAll();

        $tables .= "<h2 id=\"$name\">$name</h2><br/>";
        
        $table = new Table("width=\"80%\"");
        $table->addHeader(array("Product", "Version", "Timestamp"));
        foreach ($logs as $log) {
            $table->addRow(array($log['product'],$log['version'],date("Y-m-d H:i:s", $log['entrytime'])));
        }
        $tables .= $table->write();
    }
    $contents = "<h2>Data Sets Used in This Test</h2><br/>" . $ul->write() . "<br/><br/>";
    return $contents . $tables;

}

function displayTable_EupsSetups() {
    echo writeTable_EupsSetups();
}




function getRegex($label, $default="") {
    $regex = $default;
    if (array_key_exists($label, $_GET)) {
        $regex = $_GET[$label];
        setcookie('displayQA_'.$label, $regex);
    } elseif (array_key_exists('displayQA_'.$label, $_COOKIE)) {
        $regex = $_COOKIE['displayQA_'.$label];
    }
    # sanity?

    # must be set (otherwise, would match everything!)
    if (strlen($regex) == 0) { $regex = $default; }

    return $regex;
}


function getFloat($label, $default="") {
    $value = $default;
    if (array_key_exists($label, $_GET)) {
        $value = $_GET[$label];
        setcookie('displayQA_'.$label, $value);
    } elseif (array_key_exists('displayQA_'.$label, $_COOKIE)) {
        $value = $_COOKIE['displayQA_'.$label];
    }
    # sanity?

    # must be a float
    if (!preg_match("/^[+-]?\d+\.?\d*$/", $value)) { $value = ""; }
    
    return $value;
}



function failureSelectionBoxes() {

    #test regex box
    $testregex = getRegex('testregex', 'psf-cat');
    $testregexBox = "<input class=\"textinput\" type=\"text\" size=\"10\" name=\"testregex\" value=\"$testregex\">\n";
    $labelregex = getRegex('labelregex', 'median');
    $labelregexBox = "<input class=\"textinput\" type=\"text\" size=\"10\" name=\"labelregex\" value=\"$labelregex\">\n";
    $lo = getFloat('lo');
    $loBox = "<input class=\"textinput\" type=\"text\" size=\"10\" name=\"lo\" value=\"$lo\">\n";
    $hi = getFloat('hi');
    $hiBox = "<input class=\"textinput\" type=\"text\" size=\"10\" name=\"hi\" value=\"$hi\">\n";

    $nshow = getFloat('nshow', '10');
    $nshowBox = "<input class=\"textinput\" type=\"text\" size=\"10\" name=\"nshow\" value=\"$nshow\">\n";
    
    #submit
    $submit = "<input type=\"submit\" value-\"Compute\" class=\"button\"/>\n";
    
    $table = new Table();
    $table->addRow(
        array(
            "<b>Test(reg.ex.): ".$testregexBox,
            "<b>Label(reg.ex.): ".$labelregexBox,
            "<b>Low(float): ".$loBox,
            "<b>High(float): ".$hiBox,
            "<b>N-show(int): ".$nshowBox,
            $submit
            )
        );

    
    
    #build the form
    $form = "<form method=\"get\" action=\"failures.php\">\n";
    $form .= $table->write();
    $form .= "</form>";

    return $form;
}



function loadFailureCache() {
    

    static $alreadyLoaded = false;
    static $results = array();
    static $figures = array();

    
    if ($alreadyLoaded) {
        return array($results, $figures);
    } else {
        if (!file_exists("db.sqlite3")) {
            return array(-1, -1);
        }
        $db = connect("."); #$testDir);
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        try {
            $testCmd = "select * from failures;";
            $prep = $db->prepare($testCmd);
            $prep->execute();
            $results = $prep->fetchAll();
        } catch (PDOException $e) {
            return array(-1, -1);
        }

        try {
            $testCmd = "select * from allfigures;";
            $prep = $db->prepare($testCmd);
            $prep->execute();
            $figures = $prep->fetchAll();
        } catch (PDOException $e) {
            return array(-1, -1);
        }
        $db = null;
        
        $alreadyLoaded = true;
    }
    
    return array($results, $figures);
}



function listFailures() {

    list($failureList, $figureList) = loadFailureCache();

    if ($failureList == -1 or $figureList == -1) {
        return;
    }
    
    $figLookup = array();
    foreach ($figureList as $fig) {
        list($id, $tstamp, $path, $caption) = $fig;
        $testDir = basename(dirname($path));
        $figname = basename($path);
        $base = basename($path, ".png");

        $s = preg_replace('/([^-])-([^-])/', '\1QQQ\2', $base); 
        $s = preg_split("/QQQ/", $s);
        $tag = trim($s[count($s)-1]);
        
        if (!array_key_exists($testDir, $figLookup)) {
            #echo "new testDir $testDir $tag $figname<br/>";
            $figLookup[$testDir] = array($tag => array($figname));
        } else {
            if (!array_key_exists($tag, $figLookup[$testDir])) {
                #echo "new tag $testDir $tag $figname<br/>";
                $figLookup[$testDir][$tag] = array($figname);
            } else {
                #echo "new figname $testDir $tag $figname<br/>";
                $figLookup[$testDir][$tag][] = $figname;
            }
        }
    }

    $testregex = getRegex('testregex');
    $labelregex = getRegex('labelregex');

    $newLo = getFloat('lo');
    $newHi = getFloat('hi');
    $nShow = intval(getFloat('nshow'));
    if (!$nShow) { $nShow = count($failureList); }
    
    $out = "";
    $i = 0;
    foreach ($failureList as $failure) {
        list($id, $tstamp, $testandlabel, $value, $lo, $hi) = $failure;
        $arr = preg_split("/QQQ/", $testandlabel);
        list($testDir, $label) = $arr;
        if (!preg_match("/$testregex/", $testDir) or !preg_match("/$labelregex/",$label)) { continue; }
        $valueStr = sprintf("%.4f", $value);

        $group = "";
        $parts = preg_split("/_/", $testDir);
        if (count($parts) > 2) {
            $group = $parts[1];
        }
        
        $loOrig = $lo;
        $hiOrig = $hi;
        if (strlen($newLo) > 0 and ($newLo < $lo) or ($newLo > $hi)) { $lo = $newLo; }
        if (strlen($newHi) > 0 and ($newHi > $hi) or ($newHi < $lo)) { $hi = $newHi; }
        $loStr = ($lo == $loOrig) ? sprintf("<font color=\"#ff0000\">%.4f (unchanged)</font>", $lo) :
            sprintf("%.4f", $lo);
        $hiStr = ($hi == $hiOrig) ? sprintf("<font color=\"#ff0000\">%.4f (unchanged)</font>", $hi) :
            sprintf("%.4f", $hi);

        $s = preg_split("/\s*-\*-\s*/", $label);
        $tag = trim($s[count($s)-1]);
        #echo $testDir." ".$tag."<br/>";

        $figs = array();
        if (array_key_exists($testDir, $figLookup) and array_key_exists($tag, $figLookup[$testDir]) ) {
            #echo "Have it<br/>";
            $figs = $figLookup[$testDir][$tag];
        }
        
        $cmp = verifyTest(floatval($value), floatval($lo), floatval($hi));
        if ($cmp) {
            $table = new Table();
            $table->addHeader(array("Test", "Label", "Value", "Range", "Select"));
            $link = "<a href=\"summary.php?test=$testDir&active=$tag&group=$group\">$testDir</a>\n";
            $checked = "";
            $name = preg_replace("/\./", "DDD", $testandlabel);
            $checkbox = "<input type=\"checkbox\" name=\"$name\" value=\"1\" $checked>";
            $table->addRow(array($link, $label, $valueStr, "[$loStr, $hiStr]", $checkbox));
            $out .= $table->write();
            foreach ($figs as $f) {
                $out .= "<img src=\"$testDir/$f\">\n";
            }
        }
        $i += 1;
        if ($i > $nShow - 1) { break; }
        
    }
    $form = "<form method=\"get\" action=\"selected.php\">\n";
    $form .= $out; 
    $form .= "<input type=\"submit\" value-\"Select\" class=\"button\"/>\n";
    $form .= "</form>";
    
    return $form;
}


function listSelected() {

    $get = $_GET;

    $table = new Table();
    foreach ($get as $testandlabel => $dummy) {
        $testandlabel = preg_replace("/DDD/", ".", $testandlabel);
        $arr = preg_split("/QQQ/", $testandlabel);
        list($testDir, $label) = $arr;
        $group = "";
        $parts = preg_split("/_/", $testDir);
        if (count($parts) > 2) {
            $group = $parts[1];
        }

        $parts = preg_split("/_*-\*-_*/", $label);
        $sensor = "";
        $testLabel = "";
        if (count($parts) > 1) {
            $testLabel = $parts[0];
            $sensor = $parts[1];
        }

        $link = "<a href=\"summary.php?test=$testDir&active=$sensor&group=$group\">$group $sensor</a>\n";
        
        $table->addRow(array($link, $testLabel));
    }

    return $table->write();
}





function writeTable_listTestResults() {

    $groups = getGroupList();
    
    $summs = summarizeTestsFromCache();
    #echo "have summs<br/>";
    
    $dirs = getAllTestDirsByGroup();
    #echo "have testdirs<br/>";

    # get a list of possible tests
    $tests = array();
    foreach ($dirs as $group => $testDirs) {
        if (strlen(trim($group)) == 0) { continue; }
        
        foreach ($testDirs as $testDir) {
            $parts = preg_split("/_/", $testDir);
            $test = $parts[count($parts)-1];
            $tests[$test] = 1;
        }
    }
    
    $tests = array_keys($tests);
    sort($tests);
    
    $table = new Table();
    $head = array("No.", "Group");
    $attribs = array("align=\"left\"", "align=\"left\"");
    foreach ($tests as $test) {
        #echo $test."<br/>";
        $subparts = preg_split("/\./", $test);
        $lab = substr($subparts[1], 0, 4);
        if (count($subparts) > 2) {
            $lab .= "<br/>".$subparts[2];
        }
        $head[] = $lab;
        $attribs[] = "align=\"right\"";
    }
    $table->addHeader($head);

    $showTotals = false;
    
    $nt = count($head);
    $filters = array_fill(0, 7, array_fill(0, $nt, 0.0));
    $filters[0][0] = "u";
    $filters[1][0] = "g";
    $filters[2][0] = "r";
    $filters[3][0] = "i";
    $filters[4][0] = "z";
    $filters[5][0] = "y";
    $filters[6][0] = "x";
    
    $flookup = array("u"=>0, "g"=>1, "r"=>2, "i"=>3, "z"=>4, "y"=>5, "x"=>6);
    $nfilters = array("u"=>0, "g"=>0, "r"=>0, "i"=>0, "z"=>0, "y"=>0, "x"=>0);
    
    $totals = array_fill(0, count($head), 0.0);
    $rows = array();
    $iGroup = 1;
    foreach ($groups as $group=>$n) {
        if (strlen(trim($group)) == 0) { continue; }

        $f = substr($group, -1);
        if (!preg_match("/^[ugrizy]$/", $f)) {
            $f = 'x';
        }
        $i_f = $flookup[$f];
        $glink = "<a href=\"group.php?group=$group\" title=\"$group\">".$group."</a>";
        $row = array($iGroup, $glink);
        $i = 2;
        foreach ($tests as $test) {

            $testDir = "test_${group}_$test";

            # if we didn't run this test for this group, move along ...
            if (!file_exists($testDir)) {
                $totals[$i] += 0.0;
                $filters[$i_f][$i] += 0.0;
                $row[] = "";
                $i += 1;
                continue;
            }
            
            if ($summs == -1) {
                $summ = summarizeTestByCounting($testDir);
            } else {
                $summ = $summs[$testDir];
            }

            
            $npass = $summ['npass'];
            $ntest = $summ['ntest'];
            $nfail = $ntest - $npass;
            if ($ntest > 0) {
                $failrate = floatval($nfail)/$ntest;
            } else {
                $failrate = 0.0;
            }
            $totals[$i] += $failrate;
            $filters[$i_f][$i] += $failrate;
            
            $link = "<a href=\"summary.php?test=$testDir&active=all&group=$group\">".
                tfColor(sprintf("%.1f", 100.0*$failrate), ($npass==$ntest))."</a>\n";
            $row[] = $link;
            $i += 1;
        }
        $nfilters[$f] += 1;
        $rows[] = $row;
        $iGroup += 1;
    }
    $totals[0] = "n=".count($rows);
    $totals[1] = "all data";
    
    # grand totals
    for($i=2; $i < count($totals); $i++) {
        $failrate = 100.0*$totals[$i]/count($rows);
        $totals[$i] = tfColor(sprintf("%.2f", $failrate), (abs($failrate) < 1.0e-6));
    }
    $table->addRow($totals, $attribs);
    $table->addRow(array_fill(0, count($totals), "&nbsp;"));
        

    # totals per filter
    foreach($filters as $farr) {
        $f = $farr[0];
        if ($f == 'x') {
            continue;
        }
        $i_f = $flookup[$f];
        $row = array("n=".$nfilters[$f], $f);
        for($i=2; $i < count($filters[$i_f]); $i++) {
            if ($nfilters[$f] > 0) {
                $failrate = 100.0*$filters[$i_f][$i]/$nfilters[$f];
                $row[] = tfColor(sprintf("%.2f", $failrate), (abs($failrate)<1.0e-6));
            } else {
                $row[] = "n/a";
            }
        }
        $table->addRow($row, $attribs);
    }
    $table->addRow(array_fill(0, count($totals), "&nbsp;"));

    # add the regular visit rows
    $i = 0;
    foreach($rows as $row) {
        if ($i and $i%20==0) {
            $table->addRow($head);
        }
        $table->addRow($row, $attribs);
        $i += 1;
    }
    
    return $table->write();
}
