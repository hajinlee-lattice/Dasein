angular.module('lp.ratingsengine.ratingsenginetype', [])
.controller('RatingsEngineType', function ($state, RatingsEngineStore) {
    var vm = this;

    vm.setType = function(type, engineType) {
        RatingsEngineStore.setType(type, engineType);
        $state.go('home.ratingsengine.' + type);
    }
});