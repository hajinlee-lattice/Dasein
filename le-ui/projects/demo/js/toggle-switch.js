function toggleCheckFunctionality(){
	$(".toggle-check label").on("click", function(){
		$(".switch-tab").toggleClass("open");
	});
}

$(document).ready(function(){
	toggleCheckFunctionality();
});