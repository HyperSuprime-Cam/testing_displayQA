<?php

include_once('config.php');
include_once('environment.php');

global $qa_environment;

set_time_limit(10);

$path      = $_GET['imgen_path'];
$force     = isset($_GET['force']);

if (! file_exists($path) || filesize($path) < 10 || $force) {
    foreach ($qa_environment as $envar => $value) {
        putenv($envar."=".$value);
    }
    system($path.".sh 2>&1", $output);
}


if ($force) {
     $s = "<html>\n";
     $s .= "<head><meta http-equiv=\"Refresh\" content=\"0; url=".$_SERVER['HTTP_REFERER']."\"></head>\n";
     $s .= "</html>\n";
     echo $s;
} else {
    header('Content-Type:image/png');
    echo file_get_contents($path);
}




