<?php

$dbFile = "db.sqlite3";

function connect($dir=".") {
    global $dbFile;
    if (!file_exists("$dir/$dbFile")) {
        #print "File $dir/$dbFile does not exist.";
        return NULL;
    }

    try {
        $db = new PDO("sqlite:$dir/$dbFile");
    } catch(PDOException $e) {
        print 'Exception : '.$e->getMessage();
    }
    return $db;
}

?>