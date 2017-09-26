angular.module('lp.ratingsengine.dashboard', [])
.controller('RatingsEngineDashboard', function($q, $stateParams, $state, RatingsEngineStore, Rating) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating
    });

console.log(Rating);

    // RatingsEngineStore.getRating($stateParams.rating_id).then(function(results){
    //     console.log(results);
    //     vm.ratingsengine = results;
    // });

});
