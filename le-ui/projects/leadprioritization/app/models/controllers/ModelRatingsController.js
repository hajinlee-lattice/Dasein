angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('ModelRatingsController', function ($stateParams,
    ResourceUtility, ModelStore) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore.data,
        chartNotUpdated: false
    });

    console.log(vm.data);
    vm.init = function() {

    }

    vm.init();
});