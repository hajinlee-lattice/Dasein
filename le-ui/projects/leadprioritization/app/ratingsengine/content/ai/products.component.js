angular.module('lp.ratingsengine.ai.products', [])
.controller('RatingsEngineAIProducts', function ($q, $timeout, $state, $stateParams, $scope, RatingsEngineAIStore, RatingsEngineAIService, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        test: true
       
    });

    vm.init = function () {
        console.log('Products initialized');
    }

    vm.init();
});