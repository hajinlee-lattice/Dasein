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


});