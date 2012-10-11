<?php

include_once('config.php');
include_once('environment.php');

global $qa_environment;

set_time_limit(10);

$path      = $_GET['imgen_path'];
if (! file_exists($path) || filesize($path) < 10) {
    foreach ($qa_environment as $envar => $value) {
        putenv($envar."=".$value);
    }
    system($path.".sh 2>&1", $output);
}

header('Content-Type:image/png');
echo file_get_contents($path);




