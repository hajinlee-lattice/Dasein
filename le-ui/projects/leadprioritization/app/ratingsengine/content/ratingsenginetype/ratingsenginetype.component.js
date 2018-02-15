angular.module('lp.ratingsengine.ratingsenginetype', [])
.controller('RatingsEngineType', function ($state, $stateParams, RatingsEngineStore) {
    var vm = this;

    vm.setType = function(wizardSteps, engineType) {
        // RatingsEngineStore.setType(type, engineType);
        $state.go('home.ratingsengine.' + wizardSteps, {engineType: engineType});
    }
});