angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelRatingsController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, CurrentConfiguration, RatingsSummary) {

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
        currentConfiguration: CurrentConfiguration,
        ratingsSummary: RatingsSummary,
        workingBuckets: []
    });

    vm.init = function() {

        $scope.data = ModelStore.data;
        $rootScope.$broadcast('model-details',   { displayName: Model.ModelDetails.DisplayName });
        $scope.Math = window.Math;
        
        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

        renderChart();

    }

    function renderChart(){
        
        var verticalAxis = document.getElementById("verticalAxis");

        // Get tallest bar in set
        vm.largestLiftInSet = Math.max.apply(null, vm.ratingsSummary.bar_lifts);

        // Set height of chart components based off tallest bar
        vm.relativeHeightOfTallest = Math.round((12*vm.largestLiftInSet) + 10);
        if(vm.relativeHeightOfTallest < 150){
            vm.chartContainerHeight = Math.round((25*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 25;
        } else {
            vm.chartContainerHeight = Math.round((15*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 15;
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

        refreshChartData();
        refreshSliders();
    }

    vm.eleMouseDown = function(ev, bucket, index) {
        vm.slider = ev.currentTarget;
        vm.container = document.getElementById("sliders");
        vm.containerBox = vm.container.getBoundingClientRect();
        vm.bucket = bucket;
        console.log('mousedown', bucket, index)

        document.addEventListener('mousemove', eleMouseMove, false);
        document.addEventListener('mouseup', eleMouseUp, false);

        ev.preventDefault();
    }
    function eleMouseMove(ev) {
        var relativeSliderChartPosition = (ev.clientX - vm.containerBox.left) / vm.containerBox.width;
        if (ev.clientY > vm.containerBox.top + 150) {
            vm.slider.style.opacity = .25;
        } else {
            vm.slider.style.opacity = 1;
        }
        vm.slider.style.right = 100 - Math.round(relativeSliderChartPosition * 100) + '%';
        ev.preventDefault();
    }
    function eleMouseUp(ev){
        vm.slider.style.opacity = 1;
        delete vm.slider;

        document.removeEventListener('mousemove', eleMouseMove, false);
        document.removeEventListener('mouseup', eleMouseUp, false);

        ev.preventDefault();
        console.log("mouse up", vm.workingBuckets);
    }
    function refreshChartData(){
        // adjust colors
        vm.workingBuckets = vm.currentConfiguration;
    }
    function refreshSliders(){

        // Add bucket sliders and any needed in the dugout
        var slidersToAdd = 
                {
                    creation_timestamp: null,
                    left_bound_score: 2,
                    lift: null,
                    name: null,
                    num_leads: null,
                    right_bound_score: 2,
                },
            templatesToAdd = 5 - vm.currentConfiguration.length;

        if(vm.currentConfiguration.length <= 4) {
            for (var i = 0; i < templatesToAdd; i++) {
                vm.currentConfiguration.push(slidersToAdd);
            }
        }

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