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
.controller('ModelRatingsController', function ($scope, $rootScope, $state, $stateParams, $timeout, 
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
        // var tmp = {"total_num_leads":18479,"total_num_converted":965,"overall_lift":0.05222144055414254,"bar_lifts":[10.3129229266684,5.91184059533331,3.714564957457501,1.9245450048168304,1.36780162842339,1.0386019144638623,0.5922440040596122,0.6719025543132442,0.3072067828544513,0.30077313295174024,0.23808980388186896,0.13851155730869774,0.1410624147176977,0.07212513294887933,0.14237340370206292,0.2089006123410269,0.10697889831244392,0.07131926554162928,0.0,0.07267257228814976,0.0,0.03633628614407488,0.0,0.03626746742031716,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0],"bucketed_scores":[null,null,null,null,{"score":4,"num_leads":0,"num_converted":0,"left_num_leads":18479,"left_num_converted":965},{"score":5,"num_leads":1048,"num_converted":0,"left_num_leads":17431,"left_num_converted":965},{"score":6,"num_leads":176,"num_converted":0,"left_num_leads":17255,"left_num_converted":965},{"score":7,"num_leads":174,"num_converted":0,"left_num_leads":17081,"left_num_converted":965},{"score":8,"num_leads":174,"num_converted":0,"left_num_leads":16907,"left_num_converted":965},{"score":9,"num_leads":174,"num_converted":0,"left_num_leads":16733,"left_num_converted":965},{"score":10,"num_leads":174,"num_converted":0,"left_num_leads":16559,"left_num_converted":965},{"score":11,"num_leads":174,"num_converted":0,"left_num_leads":16385,"left_num_converted":965},{"score":12,"num_leads":174,"num_converted":0,"left_num_leads":16211,"left_num_converted":965},{"score":13,"num_leads":174,"num_converted":0,"left_num_leads":16037,"left_num_converted":965},{"score":14,"num_leads":179,"num_converted":0,"left_num_leads":15858,"left_num_converted":965},{"score":15,"num_leads":170,"num_converted":0,"left_num_leads":15688,"left_num_converted":965},{"score":16,"num_leads":175,"num_converted":0,"left_num_leads":15513,"left_num_converted":965},{"score":17,"num_leads":174,"num_converted":0,"left_num_leads":15339,"left_num_converted":965},{"score":18,"num_leads":176,"num_converted":0,"left_num_leads":15163,"left_num_converted":965},{"score":19,"num_leads":174,"num_converted":0,"left_num_leads":14989,"left_num_converted":965},{"score":20,"num_leads":176,"num_converted":0,"left_num_leads":14813,"left_num_converted":965},{"score":21,"num_leads":177,"num_converted":0,"left_num_leads":14636,"left_num_converted":965},{"score":22,"num_leads":177,"num_converted":0,"left_num_leads":14459,"left_num_converted":965},{"score":23,"num_leads":177,"num_converted":0,"left_num_leads":14282,"left_num_converted":965},{"score":24,"num_leads":176,"num_converted":0,"left_num_leads":14106,"left_num_converted":965},{"score":25,"num_leads":175,"num_converted":0,"left_num_leads":13931,"left_num_converted":965},{"score":26,"num_leads":177,"num_converted":0,"left_num_leads":13754,"left_num_converted":965},{"score":27,"num_leads":175,"num_converted":0,"left_num_leads":13579,"left_num_converted":965},{"score":28,"num_leads":176,"num_converted":1,"left_num_leads":13403,"left_num_converted":964},{"score":29,"num_leads":176,"num_converted":0,"left_num_leads":13227,"left_num_converted":964},{"score":30,"num_leads":176,"num_converted":0,"left_num_leads":13051,"left_num_converted":964},{"score":31,"num_leads":175,"num_converted":0,"left_num_leads":12876,"left_num_converted":964},{"score":32,"num_leads":177,"num_converted":0,"left_num_leads":12699,"left_num_converted":964},{"score":33,"num_leads":174,"num_converted":0,"left_num_leads":12525,"left_num_converted":964},{"score":34,"num_leads":176,"num_converted":1,"left_num_leads":12349,"left_num_converted":963},{"score":35,"num_leads":177,"num_converted":0,"left_num_leads":12172,"left_num_converted":963},{"score":36,"num_leads":174,"num_converted":0,"left_num_leads":11998,"left_num_converted":963},{"score":37,"num_leads":176,"num_converted":0,"left_num_leads":11822,"left_num_converted":963},{"score":38,"num_leads":176,"num_converted":0,"left_num_leads":11646,"left_num_converted":963},{"score":39,"num_leads":175,"num_converted":0,"left_num_leads":11471,"left_num_converted":963},{"score":40,"num_leads":176,"num_converted":1,"left_num_leads":11295,"left_num_converted":962},{"score":41,"num_leads":174,"num_converted":0,"left_num_leads":11121,"left_num_converted":962},{"score":42,"num_leads":177,"num_converted":1,"left_num_leads":10944,"left_num_converted":961},{"score":43,"num_leads":178,"num_converted":0,"left_num_leads":10766,"left_num_converted":961},{"score":44,"num_leads":176,"num_converted":0,"left_num_leads":10590,"left_num_converted":961},{"score":45,"num_leads":180,"num_converted":0,"left_num_leads":10410,"left_num_converted":961},{"score":46,"num_leads":178,"num_converted":1,"left_num_leads":10232,"left_num_converted":960},{"score":47,"num_leads":182,"num_converted":1,"left_num_leads":10050,"left_num_converted":959},{"score":48,"num_leads":177,"num_converted":0,"left_num_leads":9873,"left_num_converted":959},{"score":49,"num_leads":177,"num_converted":1,"left_num_leads":9696,"left_num_converted":958},{"score":50,"num_leads":176,"num_converted":0,"left_num_leads":9520,"left_num_converted":958},{"score":51,"num_leads":184,"num_converted":2,"left_num_leads":9336,"left_num_converted":956},{"score":52,"num_leads":186,"num_converted":2,"left_num_leads":9150,"left_num_converted":954},{"score":53,"num_leads":183,"num_converted":1,"left_num_leads":8967,"left_num_converted":953},{"score":54,"num_leads":181,"num_converted":3,"left_num_leads":8786,"left_num_converted":950},{"score":55,"num_leads":181,"num_converted":1,"left_num_leads":8605,"left_num_converted":949},{"score":56,"num_leads":177,"num_converted":3,"left_num_leads":8428,"left_num_converted":946},{"score":57,"num_leads":180,"num_converted":0,"left_num_leads":8248,"left_num_converted":946},{"score":58,"num_leads":178,"num_converted":0,"left_num_leads":8070,"left_num_converted":946},{"score":59,"num_leads":178,"num_converted":0,"left_num_leads":7892,"left_num_converted":946},{"score":60,"num_leads":175,"num_converted":2,"left_num_leads":7717,"left_num_converted":944},{"score":61,"num_leads":181,"num_converted":2,"left_num_leads":7536,"left_num_converted":942},{"score":62,"num_leads":186,"num_converted":1,"left_num_leads":7350,"left_num_converted":941},{"score":63,"num_leads":176,"num_converted":1,"left_num_leads":7174,"left_num_converted":940},{"score":64,"num_leads":179,"num_converted":3,"left_num_leads":6995,"left_num_converted":937},{"score":65,"num_leads":187,"num_converted":1,"left_num_leads":6808,"left_num_converted":936},{"score":66,"num_leads":187,"num_converted":0,"left_num_leads":6621,"left_num_converted":936},{"score":67,"num_leads":183,"num_converted":2,"left_num_leads":6438,"left_num_converted":934},{"score":68,"num_leads":184,"num_converted":1,"left_num_leads":6254,"left_num_converted":933},{"score":69,"num_leads":196,"num_converted":4,"left_num_leads":6058,"left_num_converted":929},{"score":70,"num_leads":192,"num_converted":2,"left_num_leads":5866,"left_num_converted":927},{"score":71,"num_leads":190,"num_converted":3,"left_num_leads":5676,"left_num_converted":924},{"score":72,"num_leads":191,"num_converted":4,"left_num_leads":5485,"left_num_converted":920},{"score":73,"num_leads":190,"num_converted":2,"left_num_leads":5295,"left_num_converted":918},{"score":74,"num_leads":181,"num_converted":3,"left_num_leads":5114,"left_num_converted":915},{"score":75,"num_leads":190,"num_converted":4,"left_num_leads":4924,"left_num_converted":911},{"score":76,"num_leads":193,"num_converted":4,"left_num_leads":4731,"left_num_converted":907},{"score":77,"num_leads":186,"num_converted":6,"left_num_leads":4545,"left_num_converted":901},{"score":78,"num_leads":191,"num_converted":10,"left_num_leads":4354,"left_num_converted":891},{"score":79,"num_leads":191,"num_converted":5,"left_num_leads":4163,"left_num_converted":886},{"score":80,"num_leads":197,"num_converted":7,"left_num_leads":3966,"left_num_converted":879},{"score":81,"num_leads":194,"num_converted":6,"left_num_leads":3772,"left_num_converted":873},{"score":82,"num_leads":194,"num_converted":16,"left_num_leads":3578,"left_num_converted":857},{"score":83,"num_leads":192,"num_converted":6,"left_num_leads":3386,"left_num_converted":851},{"score":84,"num_leads":204,"num_converted":10,"left_num_leads":3182,"left_num_converted":841},{"score":85,"num_leads":189,"num_converted":9,"left_num_leads":2993,"left_num_converted":832},{"score":86,"num_leads":197,"num_converted":17,"left_num_leads":2796,"left_num_converted":815},{"score":87,"num_leads":188,"num_converted":15,"left_num_leads":2608,"left_num_converted":800},{"score":88,"num_leads":193,"num_converted":19,"left_num_leads":2415,"left_num_converted":781},{"score":89,"num_leads":197,"num_converted":16,"left_num_leads":2218,"left_num_converted":765},{"score":90,"num_leads":207,"num_converted":25,"left_num_leads":2011,"left_num_converted":740},{"score":91,"num_leads":194,"num_converted":32,"left_num_leads":1817,"left_num_converted":708},{"score":92,"num_leads":201,"num_converted":42,"left_num_leads":1616,"left_num_converted":666},{"score":93,"num_leads":203,"num_converted":42,"left_num_leads":1413,"left_num_converted":624},{"score":94,"num_leads":193,"num_converted":43,"left_num_leads":1220,"left_num_converted":581},{"score":95,"num_leads":202,"num_converted":67,"left_num_leads":1018,"left_num_converted":514},{"score":96,"num_leads":201,"num_converted":74,"left_num_leads":817,"left_num_converted":440},{"score":97,"num_leads":196,"num_converted":77,"left_num_leads":621,"left_num_converted":363},{"score":98,"num_leads":201,"num_converted":86,"left_num_leads":420,"left_num_converted":277},{"score":99,"num_leads":420,"num_converted":277,"left_num_leads":0,"left_num_converted":0}]};
        // vm.ratingsSummary = tmp;
        console.log($stateParams);
        $rootScope.$broadcast('model-details',   { displayName: Model.ModelDetails.DisplayName });
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

        var modelId = $stateParams.modelId,
            rating_id = $stateParams.rating_id;

        if(vm.section === 'dashboard.scoring') {
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
        } else {
            ModelRatingsService.CreateABCDBuckets(modelId, vm.workingBuckets).then(function(result){
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
        bucketNames: ['A+', 'A', 'B', 'C', 'D', 'F'],
        ResourceUtility: ResourceUtility,
        historicalBuckets: HistoricalABCDBuckets
    });

    vm.init = function() {
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
        vm.Math = window.Math;

        vm.getModelJobNumber = vm.model.ModelDetails.ModelSummaryProvenanceProperties[5].ModelSummaryProvenanceProperty.value;

        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

        angular.forEach(vm.historicalBuckets, function(value, key) {
            if (value.length === 6) {
                vm.bucketNames = ['A+', 'A', 'B', 'C', 'D', 'F'];
            } else if (value.length < 6) {
                vm.bucketNames = ['A', 'B', 'C', 'D', 'F'];
            };
        });

        const ordered = {};
        Object.keys(vm.historicalBuckets).sort().reverse().forEach(function(key) {
          ordered[key] = vm.historicalBuckets[key];
        });

        vm.historicalBuckets = ordered;

        
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
        controller: ['$scope', '$rootScope', '$state', '$stateParams', '$timeout', 'ResourceUtility', 'Model', 'ModelStore', 'ModelRatingsService', 'CurrentConfiguration', 'RatingsSummary', function ($scope, $rootScope, $state, $stateParams, $timeout, ResourceUtility, Model, ModelStore, ModelRatingsService, CurrentConfiguration, RatingsSummary) {
            var vm = $scope;
            angular.extend(vm, {
                workingBuckets: $scope.workingBuckets,
                ratingsSummary: $scope.ratingsSummary
            });

        }]
    }
});