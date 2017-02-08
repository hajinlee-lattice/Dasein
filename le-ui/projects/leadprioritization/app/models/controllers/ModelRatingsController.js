angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelRatingsController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, MostRecentConfiguration, BucketSummary) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
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

        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

        console.log(vm.modelType, vm.currentConfiguration);

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

            
    }

    function initSliders(){

        var vanilla = document.getElementById("sliders"),
            vListItems = vanilla.getElementsByClassName("slider"),
            vDragItem,
            vDropItem;

        console.log(vListItems);

        for (var i = 0; i < vListItems.length; i++) {
          var el = vListItems.item(i);
          el.draggable = true;

          dataRegistry[getId(el)] = "data: " + el.textContent;
          
          el.addEventListener("dragstart", function(e) {
            e.dataTransfer.setData(dataType, e.target.textContent);
            e.dataTransfer.effectAllowed = "move";
            vDragItem = e.target;
          });
          
          el.addEventListener("dragend", function(e) {
            if (vDropItem) {
              vDropItem.classList.remove("dropzone");
            }
            vDragItem = vDropItem = null;
          });
          
          el.addEventListener("dragenter", function(e) {
            e.preventDefault();
            if (e.dataTransfer.types.indexOf(dataType) != -1) {
              e.target.classList.add("dropzone");      
            }
            vDropItem = e.target;
          });
          
          el.addEventListener("dragover", function(e) {
            e.preventDefault();
          });
          
          el.addEventListener("drop", function(e) {
            e.preventDefault();
            var data = e.dataTransfer.getData(dataType);
            var text = data + " dropped on " + e.target.textContent;
            setData(text || "[no data]");
          });
          
          el.addEventListener("dragleave", function(e) {
            e.target.classList.remove("dropzone");
            if (vDropItem === e.target) {
              vDropItem = null;
            }
          });
        }

    }

    function adjustColors(){
        console.log("adjust colors");
    }
    function adjustWorkingBuckets(){
        console.log("adjust working buckets");
    }

    vm.init();
})
.controller('ModelRatingsHistoryController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, HistoricalABCDBuckets) {

    var vm = this;
    angular.extend(vm, {
        model: Model,
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore,
        ResourceUtility: ResourceUtility,
        historicalBuckets: HistoricalABCDBuckets
    });

    vm.init = function() {
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
        $scope.Math = window.Math;

        console.log(vm.historicalBuckets);

        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

    };

    vm.init();

});