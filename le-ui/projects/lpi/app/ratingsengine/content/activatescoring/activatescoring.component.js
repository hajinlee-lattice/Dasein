angular.module('lp.ratingsengine.activatescoring', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService'
])
.controller('RatingsEngineActivateScoring', function ($scope, $rootScope, $state, $stateParams, $timeout, 
    ResourceUtility, Model, ModelStore, ModelRatingsService, CurrentConfiguration, RatingsSummary) {

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
        currentConfiguration: CurrentConfiguration,
        workingBuckets: CurrentConfiguration,
        ratingsSummary: RatingsSummary,
        bucketNames: ['A+', 'A', 'B', 'C', 'D', 'F'],
        slidersContainer: document.getElementById("sliders"),
        barColors: document.getElementById("barColors"),
        section: ($state.params && $state.params.section ? $state.params.section : '')
    });

    vm.init = function() {
        vm.Math = window.Math;
        
        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        };

        vm.chartNotUpdated = true;

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
        vm.updateContent = false;

        if (vm.buckets.length === 6) {
            vm.bucketNames = ['A+', 'A', 'B', 'C', 'D', 'F'];
            vm.canAddBucket = false;
        } else if (vm.buckets.length < 6) {
            vm.bucketNames = ['A', 'B', 'C', 'D', 'F'];
            vm.canAddBucket = true;
        };


        // loop through buckets in object and set their values
        for (var i = 0, len = vm.buckets.length; i < len; i++) { 

            var previousBucket = vm.buckets[i-1];
            if (previousBucket != null) {
              vm.previousRightBoundScore = previousBucket["right_bound_score"];
            }

            // set each buckets left_bound_score to the previous buckets right_bound_score minus one
            vm.buckets[i].left_bound_score = vm.previousRightBoundScore - 1;
            vm.buckets[0].left_bound_score = 99;

            if(vm.buckets[i].right_bound_score === 0){
                vm.buckets[i].right_bound_score = 5;
            };

            vm.rightScore = vm.buckets[i].right_bound_score - 1;
            vm.rightLeads = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_leads;
            vm.rightConverted = vm.ratingsSummary.bucketed_scores[vm.rightScore].left_num_converted;
            vm.leftScore = vm.buckets[i].left_bound_score;
            vm.leftLeads = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_leads;
            vm.leftConverted = vm.ratingsSummary.bucketed_scores[vm.leftScore].left_num_converted;
            
            vm.totalLeads = vm.rightLeads - vm.leftLeads;
            vm.totalConverted = vm.rightConverted - vm.leftConverted;
    
            vm.buckets[i].num_leads = vm.rightLeads - vm.leftLeads;

            if (vm.totalLeads == 0 || vm.ratingsSummary.total_num_converted == 0 || vm.ratingsSummary.total_num_leads == 0) {
                vm.buckets[i].lift = 0;
            } else {
                vm.buckets[i].lift = ( vm.totalConverted / vm.totalLeads ) / ( vm.ratingsSummary.total_num_converted / vm.ratingsSummary.total_num_leads );
            }

            vm.buckets[i].bucket_name = vm.bucketNames[i];
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

        vm.right = right;

        if (leftCheck && rightCheck) {
            this.right = right;
            vm.slider.style.right = right + '%';
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

        var modelId = vm.model.modelId,
            rating_id = $stateParams.rating_id;

        // console.log(vm.model);
            
        ModelRatingsService.CreateABCDBucketsRatingEngine(rating_id, modelId, vm.workingBuckets).then(function(result){
            if (result != null && result.success === true) {
                vm.showSuccess = true;
                vm.chartNotUpdated = true;
                vm.updateContent = true;
                $timeout( function(){ 
                    vm.updateContent = false;
                    vm.showSuccess = false;
                    
                }, 200);
            } else {
                vm.savingConfiguration = false;
                vm.createBucketsErrorMessage = result;
                vm.showSaveBucketsError = true;
            }
        });

    }

    vm.init();
});