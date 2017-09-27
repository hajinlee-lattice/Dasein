angular.module('lp.ratingsengine.dashboard', [])
.controller('RatingsEngineDashboard', function($q, $stateParams, $state, 
    RatingsEngineStore, Rating, TimestampIntervalUtility) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        TimestampIntervalUtility: TimestampIntervalUtility
    });

    // RatingsEngineStore.getRating($stateParams.rating_id).then(function(results){
    //     console.log(results);
    //     vm.ratingsengine = results;
    // });

});
