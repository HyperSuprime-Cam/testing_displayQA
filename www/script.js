window.onload = initAll;

function initAll() {
    show();
    showSql();
    showSetup();
    document.getElementById("displayText").onclick = toggle;
    document.getElementById("displaySql").onclick = toggleSql;
    document.getElementById("displaySetup").onclick = toggleSetup;
}

function show() {
    var ele = document.getElementById("toggleText");
    var text = document.getElementById("displayText");
    ele.style.display = "none";
    text.innerHTML = "Show"
        }
function toggle() {
    var ele = document.getElementById("toggleText");
    var text = document.getElementById("displayText");
    if(ele.style.display == "block") {
        ele.style.display = "none";
        text.innerHTML = "Show"
            }
    else {
        ele.style.display = "block";
        text.innerHTML = "Hide"
            }
} 




function showSql() {
    var ele = document.getElementById("toggleSql");
    var text = document.getElementById("displaySql");
    ele.style.display = "none";
    text.innerHTML = "Show"
        }
function toggleSql() {
    var ele = document.getElementById("toggleSql");
    var text = document.getElementById("displaySql");
    if(ele.style.display == "block") {
        ele.style.display = "none";
        text.innerHTML = "Show"
            }
    else {
        ele.style.display = "block";
        text.innerHTML = "Hide"
            }
} 


function showSetup() {
    var ele = document.getElementById("toggleSetup");
    var text = document.getElementById("displaySetup");
    ele.style.display = "none";
    text.innerHTML = "Show"
        }
function toggleSetup() {
    var ele = document.getElementById("toggleSetup");
    var text = document.getElementById("displaySetup");
    if(ele.style.display == "block") {
        ele.style.display = "none";
        text.innerHTML = "Show"
            }
    else {
        ele.style.display = "block";
        text.innerHTML = "Hide"
            }
} 
