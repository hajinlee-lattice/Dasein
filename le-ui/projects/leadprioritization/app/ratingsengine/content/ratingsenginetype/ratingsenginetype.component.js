angular.module('lp.ratingsengine.ratingsenginetype', [])
.controller('RatingsEngineType', function ($q, $state, $stateParams, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        foo: null
    });

    vm.setType = function(type) {
        RatingsEngineStore.setType(type);
        var url = 'home.ratingsengine';
        if('AI' === type) {
            url += '.ai';
        } else {
            url += '.wizard';
        }
        $state.go(url);
    }
});