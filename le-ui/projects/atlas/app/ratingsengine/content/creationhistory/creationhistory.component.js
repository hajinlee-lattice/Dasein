angular.module('lp.ratingsengine.creationhistory', [])
.controller('RatingsEngineCreationHistory', function ($scope, $timeout, $element, $state, 
$stateParams, RatingsEngineStore, RatingsEngineService) {

    var vm = this;
    angular.extend(vm, {
        current: RatingsEngineStore.current,
        currentPage: 1,
        pageSize: 10,
        sortBy: 'created',
        sortDesc: true

    });

    vm.init = function() {

    }
    vm.init();

});
