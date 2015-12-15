function toggleCheckFunctionality(){
	console.log("toggle script function ready");
	$(".toggle-check label").on("click", function(){
		console.log("toggle");
		$(".switch-tab").toggleClass("open");
	});
}

$(document).ready(function(){

	console.log("toggle script ready");
	toggleCheckFunctionality();
	
});