$(function() {

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
    $("#toggleNav").click(function(){
        $("body").toggleClass("open-nav");
    });

});
function checkBrowserWidth(){
    if (window.matchMedia("(min-width: 800px)").matches) {
        $("body").removeClass("open-nav");
    } else {
    	$("body").addClass("open-nav");
    }
}