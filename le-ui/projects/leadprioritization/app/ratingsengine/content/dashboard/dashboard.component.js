angular.module('lp.ratingsengine.dashboard', [])
.controller('RatingsEngineDashboard', function($q, $stateParams, $state, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        rating: null
    });

    RatingsEngineStore.getRating($stateParams.rating_id).then(function(results){
        console.log(results);
        vm.ratingsengine = results;
    });

});
