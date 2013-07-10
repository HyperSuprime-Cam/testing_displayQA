<?php
include_once("config.php");
if ($display_errors) {
    ini_set('display_errors', 'On');
    error_reporting(E_ALL);
}
include_once("Menu.php");
include_once("Page.php");
include_once("libdisplay.php");

# echo phpinfo();
$menu = new Menu();
$page = new Page(getDefaultTitle(), getDefaultH1(), $menu);

$group = getGroup();
$page->appendContent(getTestLinksThisGroup('group')."<br/>");
$page->appendContent("<h2>Group: $group</h2><br/>");
#$page->appendContent(writeQuickLookSummary());
$page->appendContent(writeTable_SummarizeAllTests());

$page->addSidebar(writeTable_timestamps($group));
$page->addsidebar(writeTable_summarizeMetadata(array("dataset"), $group));

echo $page;
