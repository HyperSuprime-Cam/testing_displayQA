<?php

include_once("Html.php");
include_once("libdisplay.php");

class Menu {

    protected $_tabs = array();
    private $_active;
    private $_group;
    private $_test;
    
    public function __construct() {

        ## a dictionary entry for each tab.
        ## contains: (1) text on the tab, (2) path to link
        $this->_tabs["home"]      = array("Home", "index.php");
        $this->_tabs["testlist"]  = array("TestList", "testlist.php");
        $this->_tabs["group"]     = array("Group", "group.php");
        $this->_tabs["summ"]      = array("Summary", "summary.php");
        $haveFailureTable = false;
        if (file_exists('db.sqlite3')) {
            $sql = "SELECT count(*) FROM failures;";
            $db = connect(".");
            $prep = $db->prepare($sql);
            $prep->execute();
            $results = $prep->fetchAll();

            if (intval($results[0][0]) > 0) {
                $haveFailureTable = true;
            }
        }
        if ($haveFailureTable) {
            $this->_tabs["fail"] = array("Failures", "failures.php");
        }
        
        #$this->_tabs["log"]  = array("Logs",    "logs.php");
        #$this->_tabs["sdqa"] = array("SDQA",    "sdqa.php");
        #$this->_tabs["eups"] = array("EUPS",    "eups.php");
        $this->_tabs["help"] = array("Help",    "help.php");

        $this->_active = getActive();
        $this->_test   = getDefaultTest();
        $this->_group  = getGroup();
    }

    
    public function __toString() {

        $uri = $_SERVER['SCRIPT_NAME'];
        
        ## need to determine which page we're on
        ## so we can highlight the appropriate tab
        $selected = array();
        $found = false;
        foreach (array_keys($this->_tabs) as $key) {
            list($label, $path) = $this->_tabs[$key];
            $path = preg_replace("/.*\//", "", $path);
            $selected[$key] = false;
            if ($path and preg_match("/$path/", $uri)) {
                $selected[$key] =  true;
                $found = true;
            }
        }
        if (!$found) {
            $selected["home"] = true;
        }

        $ul = new UnorderedList("id=\"nav\"");
        foreach (array_keys($this->_tabs) as $key) {
            list($label, $path) = $this->_tabs[$key];
            $href = $path . $this->_getArguments($key);
            $ul->addItem("<a href=\"$href\">$label</a>", $selected[$key] ? "id=\"selected\"" : "");
        }
        $menu = "<div id=\"menu\">\n".$ul->write()."</div>\n";
        return $menu;
    }

    public function write() { return $this->__toString(); }

    private function _getArguments($key) {
        $args = "";
        switch($key) {
        case "home":
            $args = "";
            break;
        case "group":
            $args = "?group=".$this->_group;
            break;
        case "summ":
            $args = "?test=".$this->_test."&active=".$this->_active;
            break;
        }
        return $args;
    }
    
  }


class SpecificTestMenu extends Menu {
    
    public function __construct() {
        parent::__construct();
        $this->_tabs["backtrace"] = array("Backtrace", "backtrace.php");
    }
}


