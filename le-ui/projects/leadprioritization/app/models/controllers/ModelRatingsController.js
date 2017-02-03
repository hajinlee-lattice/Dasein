angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('ModelRatingsController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, MostRecentConfiguration, BucketSummary) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore.data,
        chartNotUpdated: true,
        saveInProgress: false,
        showSaveBucketsError: false,
        ResourceUtility: ResourceUtility,
        currentConfiguration: MostRecentConfiguration,
        summary: BucketSummary
    });

    vm.init = function() {
        $scope.data = ModelStore.data;
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

        $scope.Math = window.Math;

        console.log(vm.currentConfiguration);

        renderChart();
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

    // Render Chart
    function renderChart(){
        
        // Define height of chart
        vm.chartContainerHeight = Math.round(12*vm.summary.bar_lifts[0] + 10);

        // Define height of dugout
        vm.dugoutHeight = vm.chartContainerHeight - 8;

        // Get highest lift value and round to next integer up
        function getMaxLift(arr, prop) {
            var max;
            for (var i=0 ; i<arr.length ; i++) {
                if (!max || parseInt(arr[i][prop]) > parseInt(max[prop]))
                    max = arr[i];
            }
            return max;
        }
        var maxLift = getMaxLift(vm.currentConfiguration, "lift");
        
        // Create vertical axis based on maxLift
        vm.yAxisNumber = Math.round(maxLift.lift);
        vm.getNumber = function(num) {return new Array(num);}
        vm.axisItemHeight = vm.chartContainerHeight / vm.yAxisNumber;

        handleSliders();

    }
    function handleSliders(){
                        
    };

    vm.init();
})
.controller('ModelRatingsHistoryController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, HistoricalABCDBuckets) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore.data,
        ResourceUtility: ResourceUtility,
        historicalBuckets: HistoricalABCDBuckets
    });

    vm.init = function() {
        $scope.data = ModelStore.data;
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

        console.log(vm.historicalBuckets);

        $scope.Math = window.Math;
    };

    vm.init();

});