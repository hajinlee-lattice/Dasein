$(function() {

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
    $("#toggleNav").click(function(){
        $("body").toggleClass("open-nav");
    });

});
function checkBrowserWidth(){
    if (window.matchMedia("(max-width: 1366px)").matches) {
        $("body").removeClass("open-nav");
    }
}