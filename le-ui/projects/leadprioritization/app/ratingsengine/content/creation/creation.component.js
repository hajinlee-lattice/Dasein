angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, Rating
) {
    var vm = this;

    angular.extend(vm, {
        ratingEngine: Rating
    });

    vm.init = function() {
    	console.log(vm.ratingEngine);
    };

    vm.init();

});