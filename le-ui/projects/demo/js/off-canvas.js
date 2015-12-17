$(function() {

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);
    $("#toggleNav").click(function(){
        $("body").toggleClass("open-nav");
    });

});
function checkBrowserWidth(){
    if (window.matchMedia("(min-width: 1024px)").matches) {
        $("body").addClass("open-nav");
    } else {
        $("body").removeClass("open-nav");
    }
}