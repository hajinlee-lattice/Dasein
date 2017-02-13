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

    function renderChart(){
        
        var verticalAxis = document.getElementById("verticalAxis");

        // Get tallest bar in set
        vm.largestLiftInSet = Math.max.apply(Math,vm.summary.bar_lifts.map(function(o){return o;}));

        // Set height of chart components based off tallest bar
        vm.relativeHeightOfTallest = Math.round((12*vm.largestLiftInSet) + 10);
        if(vm.relativeHeightOfTallest < 150){
            vm.chartContainerHeight = Math.round((25*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 25;
            console.log("small");
        } else {
            vm.chartContainerHeight = Math.round((15*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 15;
            console.log("large");
        }

        // Define height of dugout
        vm.dugoutHeight = vm.chartContainerHeight - 8;
        
        // Create vertical axis based on maxLift
        vm.yAxisNumber = Math.round(vm.largestLiftInSet);
        if(vm.yAxisNumber >= 3 && vm.yAxisNumber <= 10) {
            verticalAxis.classList.add('reduceSmall');
        } else if (vm.yAxisNumber > 10) {
            verticalAxis.classList.add('reduceBig');
        }

        vm.getNumber = function(num) {return new Array(num);}
        vm.axisItemHeight = vm.chartContainerHeight / vm.yAxisNumber;

        console.log(vm.largestLiftInSet, vm.yAxisNumber, 25*vm.largestLiftInSet+10, vm.chartContainerHeight); 
 
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

        vm.getModelJobNumber = vm.model.ModelDetails.ModelSummaryProvenanceProperties[5].ModelSummaryProvenanceProperty.value;
        // console.log(vm.model.ModelDetails.ModelSummaryProvenanceProperties[5].ModelSummaryProvenanceProperty);
        console.log($stateParams);

        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

        // Set value for total leads in set
        // This will need to get changed when we're saving configurations
        vm.historyTotalLeads = pluckDeepKey("num_leads", vm.historicalBuckets);

        // Add values for a specific key in object
        function pluckDeepKey(key, obj) {
          if (_.has(obj, key)) {
            return obj[key];
          }
          return _.reduce(_.flatten(_.map(obj, function(v) {
            return _.isObject(v) ? pluckDeepKey(key, v) : [];
          }), false), function(a,b) { return a + b });
        }

    };

    vm.init();

});