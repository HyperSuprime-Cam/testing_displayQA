window.onload = initAll;

function initAll() {
    show();
    document.getElementById("displayText").onclick = toggle;
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
