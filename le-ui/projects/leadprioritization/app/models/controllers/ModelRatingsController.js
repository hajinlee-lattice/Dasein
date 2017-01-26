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

        $scope.Math = window.Math;
        renderChart();

        console.log(vm.buckets, vm.summary);
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
        vm.chartContainerHeight = Math.round(10*vm.summary.bar_lifts[0] + 10);

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
        var maxLift = getMaxLift(vm.buckets, "lift");
        
        // Create vertical axis based on maxLift
        vm.yAxisNumber = maxLift.lift;
        vm.getNumber = function(num) {return new Array(num);}
        vm.axisItemHeight = vm.chartContainerHeight / vm.yAxisNumber;

        handleSliders();

    }
    function handleSliders(){
        // Sliders
        var ele = document.getElementsByClassName("handle")[0];
        //ele.onmousedown = eleMouseDown;
        if (ele) {
            ele.addEventListener("mousedown" , eleMouseDown , false);
        };

        function eleMouseDown () {
            stateMouseDown = true;
            document.addEventListener ("mousemove" , eleMouseMove , false);
        }

        function eleMouseMove (ev) {
            var pX = ev.pageX;
            ele.style.left = pX + "px";
            document.addEventListener ("mouseup" , eleMouseUp , false);
        }

        function eleMouseUp () {
            document.removeEventListener ("mousemove" , eleMouseMove , false);
            document.removeEventListener ("mouseup" , eleMouseUp , false);
        }
    };

    vm.init();
});