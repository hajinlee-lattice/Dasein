angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('ModelRatingsController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, ABCDBuckets, BucketSummary) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore.data,
        chartNotUpdated: true,
        saveInProgress: false,
        showSaveBucketsError: false,
        ResourceUtility: ResourceUtility,
        buckets: ABCDBuckets.Result,
        summary: BucketSummary.Result
    });

    vm.init = function() {
        $scope.data = ModelStore.data;
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
    }

    vm.publishConfiguration = function() {
        vm.chartNotUpdated = false;
        ModelRatingsService.CreateABCDBuckets().then(function(result){
            if (result != null && result.success === true) {
                $state.go('home.model.ratings', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.createBucketsErrorMessage = result;
                vm.showSaveBucketsError = true;
            }
        });
    }

    vm.init();
});