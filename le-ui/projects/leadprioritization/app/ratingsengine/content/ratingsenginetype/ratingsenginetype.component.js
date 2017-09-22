angular.module('lp.ratingsengine.ratingsenginetype', [])
.controller('RatingsEngineType', function ($q, $state, $stateParams, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        foo: null
    });

    vm.setType = function(type) {
        RatingsEngineStore.setType(type);
        $state.go('home.ratingsengine.wizard');
    }
});