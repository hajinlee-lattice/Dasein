$(function() {

	// Clickable Dropdown
	$(".dropdown > a").click(function(e){
		$(this).toggleClass("active");
		$(".dropdown > ul").toggle();
		e.stopPropagation();
	});
	$(document).click(function() {
		if ($(".dropdown > ul").is(':visible')) {
			$(".dropdown > ul", this).hide();
			$(".dropdown > a").removeClass('active');
		}
	});


	// Toggle Collapsible Areas
	$(".toggle > a").click(function(e){
		$(this).parent().toggleClass("open");
		e.preventDefault();
		if ($(".toggle > ul").is(':visible')) {
			$(".toggle > a > span:last-child").removeClass("fa-angle-double-down");
			$(".toggle > a > span:last-child").addClass("fa-angle-double-up");
		} else {
			$(".toggle > a > span:last-child").removeClass("fa-angle-double-up");
			$(".toggle > a > span:last-child").addClass("fa-angle-double-down");
		}
	});


});