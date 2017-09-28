angular.module('lp.ratingsengine.wizard.summary', [])
.controller('RatingsEngineSummary', function ($q, $state, $stateParams, Rating, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        block_user: false,
    });

    vm.init = function() {

    	// console.log(vm.rating);

    };

    vm.init();

});