angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService'
])
.directive('refresher', function() {
  return {
    transclude: true,
    controller: function($scope, $transclude,
                         $attrs, $element) {
      var childScope;

      $scope.$watch($attrs.condition, function(value) {
        $element.empty();
        if (childScope) {
          childScope.$destroy();
          childScope = null;
        }

        $transclude(function(clone, newScope) {
          childScope = newScope;
          $element.append(clone);
        });
      });
    }
  };
})
.controller('ModelRatingsController', function (
    $scope, $rootScope, $state, $stateParams, $timeout, ResourceUtility, Model, Notice, 
    ModelStore, ModelRatingsService, WorkingBuckets, RatingsSummary, RatingsEngineStore
) {
    var vm = this;

    angular.extend(vm, {
        stateParams: $stateParams,
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
        saveInProgress: false,
        showSaveBucketsError: false,
        updateContent: false,
        ResourceUtility: ResourceUtility,
        workingBuckets: WorkingBuckets,
        ratingsSummary: RatingsSummary,
        showCurrentConfig: true,
        bucketNames: ['A', 'B', 'C', 'D', 'E', 'F'],
        section: ($state.params && $state.params.section ? $state.params.section : '')
    });

    vm.init = function() {
        // Atlas uses dashboard.ratings for vm.section
        if (vm.section === 'dashboard.ratings') {
            vm.currentRating = RatingsEngineStore.currentRating;
            vm.latest_iteration = vm.currentRating.latest_iteration;
            vm.predictionType = vm.latest_iteration.AI.predictionType;

            // Get dashboard data for list of iterations
            vm.dashboard = ModelStore.getDashboardData();
            var dashboardIterations = vm.dashboard.iterations;

            // Show 'No Ratings Available' message if dashboard bucketMetadata isn't present for the selected iteration
            vm.hasRatingsAvailable = vm.dashboard.summary.bucketMetadata ? true : false;

            // use only iterations that have active modelSummaryId by creating new array
            vm.activeIterations = [];
            angular.forEach(dashboardIterations, function(iteration){
                if (iteration.modelSummaryId && iteration.modelingJobStatus == "Completed") {
                    vm.activeIterations.push(iteration);
                }
            });

            // Currently Published Rating Buckets at bottom of the UI.
            // Don't show for new models that have never been published.
            // Show the latest_iteration config for the current model's model summary
            if (vm.currentRating.scoring_iteration) {
                var id = vm.currentRating.scoring_iteration.AI.modelSummaryId;
                ModelRatingsService.MostRecentConfiguration(id).then(function(result) {
                    vm.currentConfiguration = result;
                    vm.showCurrentConfig = true;
                });
            } else {
                vm.showCurrentConfig = false;
            }

            if ($stateParams.useSelectedIteration) {

                vm.activeIteration = vm.activeIterations.filter(iteration => iteration.modelSummaryId === $stateParams.modelId)[0];

                console.log(vm.workingBuckets);

                // Update UI data if user got to the UI from either the "Activate" button 
                // or used the Iteration select menu from within the UI.
                var id = $stateParams.modelId;
                ModelRatingsService.GetBucketedScoresSummary(id).then(function(result) {
                    vm.ratingsSummary = result;
                });
                ModelRatingsService.MostRecentConfiguration(id).then(function (result) {
                    vm.workingBuckets = result;
                });

            } else {

                // This determines what the active iteration is.
                // Active iteration is what is currently selected in the Iteration select box.
                vm.activeIteration = vm.activeIterations.filter(iteration => iteration.modelSummaryId === $stateParams.modelId)[0];

                // If the model has been published previously and is Active
                if (vm.dashboard.summary.publishedIterationId && vm.dashboard.summary.status == 'ACTIVE'){

                    // Set active iteration and working buckets (determines what is displayed in the chart)
                    vm.activeIteration = vm.activeIterations.filter(iteration => iteration.id === vm.dashboard.summary.publishedIterationId)[0];
                    vm.workingBuckets = vm.dashboard.summary.bucketMetadata ? vm.dashboard.summary.bucketMetadata : [];

                    var id = vm.activeIteration.modelSummaryId;
                    ModelRatingsService.GetBucketedScoresSummary(id).then(function(result) {
                        // Helps with chart data and display
                        vm.ratingsSummary = result;
                    });
                    ModelRatingsService.MostRecentConfiguration(id).then(function (result) {
                        vm.workingBuckets = result;
                    });
                    

                } else {
                    // If the model has not been published or is inactive, 
                    // select the most recent iteration in the select menu
                    vm.activeIteration = vm.activeIterations[vm.activeIterations.length - 1];
                }
            }

            // console.log(vm.currentRating);
            // console.log(vm.activeIteration);
            // console.log(vm.workingBuckets);
            // console.log(vm.currentConfiguration);
            // console.log(vm.showCurrentConfig);

            if (vm.currentRating.scoring_iteration) {
                vm.activeConfigNumber = vm.currentRating.scoring_iteration.AI.iteration;
            }
            vm.ratingModelId = vm.activeIteration.id;
        }
        
        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "All Leads";
        } else {
            vm.modelType = "Scorable Accounts";
        };

        vm.Math = window.Math;
        vm.chartNotUpdated = (vm.section === 'dashboard.scoring' || vm.section === 'dashboard.ratings') ? false : true;

        // Give the above code time to catch up before rendering the chart
        $timeout(function() {
            renderChart();
        }, 500);
    }

    vm.init();

    vm.changeIterationData = function(){
        $state.go('home.model.ratings', {
            modelId: vm.activeIteration.modelSummaryId,
            rating_id: $stateParams.rating_id,
            viewingIteration: false,
            useSelectedIteration: true
        }, {reload: true});
    }

    function renderChart(){

        vm.slidersContainer = document.getElementById("sliders");
        vm.barColors = document.getElementById("barColors");
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

        vm.getNumber = function(num) {
            if(!Number.isNaN(num)) {
                return new Array(num);
            }
        }
        vm.axisItemHeight = vm.chartContainerHeight / vm.yAxisNumber;
        refreshChartData();
    }

    function refreshChartData(){
        vm.buckets = vm.workingBuckets;
        vm.bucketsLength = vm.buckets.length;
        vm.updateContent = false;

        if (vm.buckets.length === 6) {
            vm.bucketNames = ['A', 'B', 'C', 'D', 'E', 'F'];
            vm.canAddBucket = false;
        } else if (vm.buckets.length < 6) {
            vm.bucketNames = ['A', 'B', 'C', 'D', 'E'];
            vm.canAddBucket = true;
        }

        vm.iterationSupplementaryInfo = vm.predictionType === 'EXPECTED_VALUE' ? 
            'Compare to your past average Revenue: $' + (vm.model.ModelDetails.AverageRevenue).toFixed(2) : 
            'Compare to your past average Conversion Rate: ' + ((vm.model.ModelDetails.TestConversionCount / vm.model.ModelDetails.TestRowCount) * 100).toFixed(0) + '%';


        // loop through buckets in object and set their values
        for (var i = 0, len = vm.bucketsLength; i < len; i++) { 
            var bucket = vm.buckets[i];
            var previousBucket = vm.buckets[i-1];

            if (previousBucket != null) {
                vm.previousRightBoundScore = previousBucket["right_bound_score"];
            }

            // set each buckets left_bound_score to the previous buckets right_bound_score minus one
            bucket.left_bound_score = vm.previousRightBoundScore - 1;
            vm.buckets[0].left_bound_score = 99;

            if (bucket.right_bound_score === 0){
                bucket.right_bound_score = 5;
            }

            vm.rightScore = bucket.right_bound_score - 1;
            vm.rightLeads = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_leads;
            vm.rightConverted = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_converted;
            vm.leftScore = bucket.left_bound_score;
            vm.leftLeads = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_leads;
            vm.leftConverted = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_converted;
            
            vm.totalLeads = vm.rightLeads - vm.leftLeads;
            vm.totalConverted = vm.rightConverted - vm.leftConverted;

            var bucketLeads = 0,
                bucketRevenue = 0,
                bucketConverted = 0,
                score = null;


            for (var index = vm.leftScore; index > vm.rightScore; index--) {
                score = vm.ratingsSummary.bucketed_scores[index];

                bucketLeads += score.num_leads;
                bucketConverted += score.num_converted;
                bucketRevenue += score.expected_revenue;

                // firstVal += (score.num_converted * score.num_leads);
                // secondVal += (score.num_leads * score.num_leads);
                
            }

            bucket.conversionRate = (bucketConverted / bucketLeads) * 100;
            // bucket.conversionRate = (firstVal / secondVal) * 100;
            bucket.bucket_name = vm.bucketNames[i];
            bucket.bucketAvgRevenue = bucketRevenue / bucketLeads;
            bucket.avg_expected_revenue = bucket.bucketAvgRevenue;
            bucket.num_leads = vm.rightLeads - vm.leftLeads;
            bucket.total_expected_revenue = (bucket.avg_expected_revenue * bucket.num_leads);

            // bucket.lift = ( bucketAvgRevenue / total average expected revenue across all buckets);

            if (vm.predictionType === 'EXPECTED_VALUE'){
                
                bucket.lift = (bucket.bucketAvgRevenue / (vm.ratingsSummary.total_expected_revenue / vm.ratingsSummary.total_num_leads)) >= 0.1 ? (bucket.bucketAvgRevenue / (vm.ratingsSummary.total_expected_revenue / vm.ratingsSummary.total_num_leads)) : 0.1010101;
            
            } else {
                if (vm.totalLeads === 0 || vm.ratingsSummary.total_num_converted === 0 || vm.ratingsSummary.total_num_leads === 0) {
                    bucket.lift = 0;
                } else {
                    bucket.lift = ( vm.totalConverted / vm.totalLeads ) / ( vm.ratingsSummary.total_num_converted / vm.ratingsSummary.total_num_leads );
                }
            }
        }
    }


    vm.addBucket = function(ev){
        if (vm.workingBuckets.length < 6 && vm.canAddBucket) {
            vm.containerBox = vm.slidersContainer.getBoundingClientRect();
            vm.relativeSliderChartPosition = (ev.clientX - vm.containerBox.left) / vm.containerBox.width;

            var addSlider = {
                    creation_timestamp: 0,
                    left_bound_score: 0,
                    lift: 0,
                    bucket_name: "",
                    num_leads: 0,
                    right_bound_score: 100 - Math.round(vm.relativeSliderChartPosition * 100)
                };

            vm.workingBuckets.push(addSlider);
            vm.workingBuckets.sort(function(a, b){return b.right_bound_score - a.right_bound_score});

            vm.chartNotUpdated = false;
            vm.canAddBucket = true;

            refreshChartData();
        } else {
            vm.canAddBucket = false;
        }
        
    }
    
    vm.eleMouseDown = function(ev, bucket, index) {
        ev.preventDefault();
        ev.stopPropagation();

        vm.slider = ev.currentTarget;
        vm.containerBox = vm.slidersContainer.getBoundingClientRect();
        vm.bucket = bucket;
        vm.index = index;

        vm.bucket.isMoving = true;
        vm.canAddBucket = false;
        vm.showRemoveBucketText = false;
        vm.startingPosition = ev.clientX;
        vm.updateContent = false;
        vm.right = bucket.right_bound_score;
        
        document.addEventListener('mousemove', eleMouseMove, false);
        document.addEventListener('mouseup', eleMouseUp, false);

    }

    function eleMouseMove(ev) {
        ev.preventDefault();
        ev.stopPropagation();

        vm.firstBucket = vm.workingBuckets[Object.keys(vm.workingBuckets)[0]];
        vm.relativeSliderChartPosition = (ev.clientX - vm.containerBox.left) / vm.containerBox.width;

        if (vm.index === 0){
            vm.sliderBoundaryLeft = 98;
            vm.sliderBoundaryRight = vm.workingBuckets[Object.keys(vm.workingBuckets)[vm.index+1]].right_bound_score + 2;  
        } else {
            vm.sliderBoundaryRight = vm.workingBuckets[Object.keys(vm.workingBuckets)[vm.index+1]].right_bound_score + 2;
            vm.sliderBoundaryLeft = vm.workingBuckets[Object.keys(vm.workingBuckets)[vm.index-1]].right_bound_score - 2;
        }

        var right = 100 - Math.round(vm.relativeSliderChartPosition * 100);
        var leftCheck = right <= vm.sliderBoundaryLeft;
        var rightCheck = right >= vm.sliderBoundaryRight;

        if (leftCheck && rightCheck) {
            this.right = right;
            vm.slider.style.right = right + '%';
            vm.right = right;
        } else if (!leftCheck && rightCheck) {
            vm.right = 98;
        } else if (leftCheck && !rightCheck) {
            if(vm.index == (vm.workingBuckets.length - 1)){
                vm.right = 5;    
            } else {
                vm.right = vm.sliderBoundaryRight;
            }
            
        } else {
            vm.slider.style.right = (leftCheck ? vm.sliderBoundaryRight : vm.sliderBoundaryLeft) + '%';
        }

        if (vm.workingBuckets.length > 2 && (ev.clientY > vm.containerBox.bottom + 10)) {
            vm.showRemoveBucketText = true;
            vm.slider.style.opacity = .25;

            if (vm.showRemoveBucketText) {
                $scope.$apply();
            }
        } else {
            vm.showRemoveBucketText = false;
            vm.slider.style.opacity = 1;

            if (!vm.showRemoveBucketText) {
                $scope.$apply();
            }
        }
    }

    function eleMouseUp(ev, index){
        ev.preventDefault();
        ev.stopPropagation();

        vm.bucket.isMoving = false;

        if(vm.startingPosition != ev.clientX) {
            vm.bucket.right_bound_score = this.right;
            vm.chartNotUpdated = false;
        }

        vm.slider.style.opacity = 1;
        vm.canAddBucket = false;

        vm.workingBuckets.sort(function(a, b){return b.right_bound_score - a.right_bound_score});

        if (vm.workingBuckets.length > 2 && ev.clientY > vm.containerBox.bottom + 10) {
            vm.chartNotUpdated = false;
            vm.showRemoveBucketText = false;

            vm.workingBuckets.splice(vm.index, 1);

            var buckets = vm.workingBuckets;

            for (var i = 0, len = buckets.length; i < len; i++) { 
                var previousBucket = buckets[i-1];

                for (var bucket in previousBucket) {
                  vm.previousRightBoundScore = previousBucket["right_bound_score"];
                }

                buckets[i].left_bound_score = vm.previousRightBoundScore - 1;
                buckets[0].left_bound_score = 99;
            }

            $scope.$apply();
        }

        delete vm.slider;

        document.removeEventListener('mousemove', eleMouseMove, false);
        document.removeEventListener('mouseup', eleMouseUp, false);

        $timeout(function() {
            refreshChartData();    
        }, 1);

    }

    vm.publishConfiguration = function() {
        vm.chartNotUpdated = false;
        vm.savingConfiguration = true;

        vm.workingBuckets.forEach(function(bucket){
            delete bucket.bucketAvgRevenue;
        });

        var rating_id = $stateParams.rating_id,
            aiModelId = vm.ratingModelId;
            
        ModelRatingsService.CreateABCDBucketsRatingEngine(rating_id, aiModelId, vm.workingBuckets).then(function(result){
            if (result != null && result.success === true) {

                var id = $stateParams.modelId;
                ModelRatingsService.MostRecentConfiguration(id).then(function(currentConfig) {
                    vm.currentConfiguration = currentConfig;

                    vm.showCurrentConfig = true;
                    refreshChartData();
                });

                RatingsEngineStore.saveRatingStatus(rating_id, 'ACTIVE', 'false').then(function(result){
                    vm.chartNotUpdated = true;
                    vm.updateContent = true;
                    vm.savingConfiguration = false;

                    Notice.success({
                        delay: 4000,
                        title: 'Publish Configuration', 
                        message: 'Your new ratings configuration has been published.'
                    });

                    vm.activeConfigNumber = vm.activeIteration.iteration;

                    $rootScope.$broadcast('statusChange', { 
                        activeStatus: 'ACTIVE',
                        activeIteration: vm.activeIteration.iteration
                    });
                    $timeout( function(){ 
                        vm.updateContent = false;
                    }, 300);
                });
               
            } else {
                vm.savingConfiguration = false;
                vm.createBucketsErrorMessage = result;
                vm.showSaveBucketsError = true;
            }
        });
    }

})
.controller('ModelRatingsHistoryController', function (
    $scope, $rootScope, $state, $stateParams, $window,
    ResourceUtility, Model, ModelStore, ModelRatingsService, RatingsEngineStore, ScoringHistory) {

    var vm = this;
    angular.extend(vm, {
        model: Model,
        rating_id: $stateParams.rating_id,
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore,
        bucketNames: ['A', 'B', 'C', 'D', 'E', 'F'],
        ResourceUtility: ResourceUtility,        
        currentRating: RatingsEngineStore.currentRating,
        scoringHistory: ScoringHistory,
        math: window.Math,
        currentPage: 1,
        pageSize: 4,
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'publishedTimestamp',
                items: [
                    { label: 'Publish Date', icon: 'numeric', property: 'publishedTimestamp' },
                    { label: 'Publisher', icon: 'alpha', property: 'publishedBy' }
                ]
            },
            filter: {
                label: 'Filter By',
                value: {},
                items: [
                    { label: "All Iterations", action: {}, total: vm.totalLength }
                ]
            }
        }
    });

    vm.init = function() {
        
        console.log(vm.scoringHistory);

        vm.latest_iteration = vm.currentRating.latest_iteration;
        vm.predictionType = vm.latest_iteration.AI.predictionType;

        vm.header.filter.unfiltered = vm.scoringHistory;
        vm.header.filter.filtered = vm.scoringHistory;

        let uniqueIterations = [...new Set(vm.scoringHistory.map(item => item.iteration).sort())];
        uniqueIterations.sort(function(a, b) {
          return b - a;
        });

        angular.forEach(uniqueIterations, function(iterationOption){
            var iterationFilter = { label: 'Iteration ' + iterationOption, action: { iteration: iterationOption } }
            vm.header.filter.items.push(iterationFilter);    
        }); 
    };

    vm.init();
})
.directive('modelRatingsChart', function() {
    return {
        restrict: 'EA',
        templateUrl: 'app/models/views/ModelRatingsChartView.html',
        scope: {
            workingBuckets: '=?',
            ratingsSummary: '=?',

            // chartContainerHeight: '=?',
            // showRemoveBucketText: '=?',
            // showSuccess: '=?',
            // dugoutHeight: '=?',
            // addBucket: '=?', // function
            // canAddBucket: '=?',
            // workingBuckets: '=?',
            // eleMouseDown: '=?',
            // right: '=?',
            // getNumber: '=?', // function
            // yAxisNumber: '=?',
            // axisItemHeight: '=?',
            // Math: '=?',
            // barMultiplier: '=?',
            // bucketHover: '=?', // function
            // modelType: '=?'
        },
        controller: ['$scope', '$rootScope', '$state', '$stateParams', '$timeout', 'ResourceUtility', 'Model', 'ModelStore', 'ModelRatingsService', 'WorkingBuckets', 'RatingsSummary', function ($scope, $rootScope, $state, $stateParams, $timeout, ResourceUtility, Model, ModelStore, ModelRatingsService, WorkingBuckets, RatingsSummary) {
            var vm = $scope;
            angular.extend(vm, {
                workingBuckets: $scope.workingBuckets,
                ratingsSummary: $scope.ratingsSummary
            });

        }]
    }
});