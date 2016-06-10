$(function() {

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
    $("#toggleNav").click(function(){
        $("body").toggleClass("open-nav");
    });

});
function checkBrowserWidth(){
    if (window.matchMedia("(min-width: 800px)").matches) {
    	console.log("remove");
        $("body").removeClass("open-nav");
    } else {
    	console.log("add");
    	$("body").addClass("open-nav");
    }
}