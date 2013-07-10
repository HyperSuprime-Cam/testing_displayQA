<?php

include_once('dbconfig.php');
    
$dbFile = "db.sqlite3";

function connect($dir=".") {

    global $host, $port, $user, $password, $dbsys, $dbname; # these should be defined in dbconfig.php
    global $dbFile;

    $connectStr = NULL;

    # handle sqlite
    if ($dbsys === 'sqlite') {
        if (!file_exists("$dir/$dbFile")) {
            #print "File $dir/$dbFile does not exist.";
            return NULL;
        }
        $connectStr = "sqlite:$dir/$dbFile";
    }
        
    # handle pgsql
    if ($dbsys === 'pgsql') {
        $connectStr = "pgsql:host=$host;port=$port;dbname=$dbname;user=$user;password=$password";
    }


    $db = NULL;
    try {
        $db = new PDO($connectStr);
    } catch(PDOException $e) {
        print 'Exception : '.$e->getMessage();
    }
    return $db;
}

