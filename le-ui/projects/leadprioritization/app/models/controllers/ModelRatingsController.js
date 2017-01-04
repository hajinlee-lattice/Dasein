angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('ModelRatingsController', function ($stateParams,
    ResourceUtility) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName
    });

    console.log($stateParams.modelId, vm.modelId);
    vm.init = function() {

    }

    vm.init();
});