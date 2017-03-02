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
        
        refreshChartData();

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
   
    }

    vm.eleMouseDown = function(ev, bucket, index) {
        vm.slider = ev.currentTarget;
        vm.container = document.getElementById("sliders");
        vm.containerBox = vm.container.getBoundingClientRect();
        vm.bucket = bucket;

        document.addEventListener('mousemove', eleMouseMove, false);
        document.addEventListener('mouseup', eleMouseUp, false);

        ev.preventDefault();
    }
    function eleMouseMove(ev) {

        vm.relativeSliderChartPosition = (ev.clientX - vm.containerBox.left) / vm.containerBox.width;
        if (ev.clientY > vm.containerBox.bottom + 10) {
            vm.slider.style.opacity = .25;
        } else {
            vm.slider.style.opacity = 1;
        }
        vm.slider.style.right = 100 - Math.round(vm.relativeSliderChartPosition * 100) + '%';

        vm.bucket.right_bound_score = vm.slider.style.right.slice(0, -1);

        ev.preventDefault();

    }
    function eleMouseUp(ev, bucket, index, $state){

        vm.slider.style.opacity = 1;

        if (ev.clientY > vm.containerBox.top + 150) {
            vm.workingBuckets.splice(index, bucket);
            vm.chartNotUpdated = false;
        }
        refreshChartData();

        document.removeEventListener('mousemove', eleMouseMove, false);
        document.removeEventListener('mouseup', eleMouseUp, false);
        ev.preventDefault();

        delete vm.slider;

    }
    function refreshChartData(){
        
        vm.workingBuckets = vm.currentConfiguration;
        var buckets = vm.workingBuckets;

        // check if we can add anymore sliders
        if(buckets.length < 5) {
            vm.canAddBucket = true;
        }

        buckets[0].left_bound_score = 99;
        // loop through buckets in object and set their values
        for (var i = 0, len = buckets.length; i < len; i++) { 
            
            var previousBucket = buckets[i-1];
            for (var bucket in previousBucket) {
              vm.previousRightBoundScore = previousBucket["right_bound_score"];
            }
            // set each buckets left_bound_score to the previous buckets right_bound_score minus one
            buckets[i].left_bound_score = vm.previousRightBoundScore - 1;
            buckets[0].left_bound_score = 99;

            vm.rightScore = buckets[i].right_bound_score;
            vm.rightLeads = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_leads;
            vm.rightConverted = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_converted;
            vm.leftScore = buckets[i].left_bound_score;
            vm.leftLeads = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_leads;
            vm.leftConverted = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_converted;
    
            buckets[i].num_leads = vm.rightLeads - vm.leftLeads;

            buckets[i].lift = (vm.rightConverted - vm.leftConverted)/(vm.rightLeads - vm.leftLeads);

        }

        
        
    }
    vm.addBucket = function(ev){
        
        if(vm.workingBuckets.length < 6) {
            
            vm.container = document.getElementById("sliders");
            vm.containerBox = vm.container.getBoundingClientRect();
            vm.relativeSliderChartPosition = (ev.clientX - vm.containerBox.left) / vm.containerBox.width;

            var addSlider = {
                    creation_timestamp: 0,
                    left_bound_score: 0,
                    lift: 0,
                    name: "",
                    num_leads: 0,
                    right_bound_score: 100 - Math.round(vm.relativeSliderChartPosition * 100)
                };

            vm.currentConfiguration.push(addSlider);
            vm.currentConfiguration.sort(function(a, b){return b.right_bound_score - a.right_bound_score});

            vm.chartNotUpdated = false;
            vm.canAddBucket = true;

            refreshChartData();

        } else if(vm.workingBuckets.length >= 6) {
            
            vm.canAddBucket = false;

        }
        
    }
    vm.publishConfiguration = function() {
        vm.chartNotUpdated = false;

        var params = {id: vm.modelId, bucketMetadatas: vm.workingBuckets}

        ModelRatingsService.CreateABCDBuckets(params).then(function(result){
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