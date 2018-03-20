angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, $scope, $interval,
    Rating, RatingsEngineStore, JobsStore, Products
) {
    var vm = this,
        checkJobStatus;

    angular.extend(vm, {
        ratingEngine: Rating,
        products: Products,
        hasSettingsInfo: true,
        status: 'Preparing Modeling Job',
        progress: '1%'
    });

    vm.init = function() {

        vm.setValidation('creation', true);

    	var model = vm.ratingEngine.activeModel.AI;
        if((Object.keys(model.AI.advancedModelingConfig.cross_sell.filters).length === 0 || (model.AI.advancedModelingConfig.cross_sell.filters['PURCHASED_BEFORE_PERIOD'] && Object.keys(model.AI.advancedModelingConfig.cross_sell.filters).length === 1)) && model.AI.trainingSegment === null && model.AI.advancedModelingConfig.cross_sell.filters.trainingProducts.length === 0) {
            vm.hasSettingsInfo = false;
        }

        vm.targetProducts = model.AI.advancedModelingConfig.cross_sell.targetProducts;
        vm.modelingStrategy = model.AI.advancedModelingConfig.cross_sell.modelingStrategy;
        vm.predictionType = model.AI.predictionType;
        vm.configFilters = model.AI.advancedModelingConfig.cross_sell.filters;
        vm.trainingSegment = model.AI.trainingSegment;
        vm.trainingProducts = model.AI.advancedModelingConfig.cross_sell.trainingProducts;



    	if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
        	vm.ratingEngineType = 'First Purchase Cross-Sell'
        } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
        	vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
        }

    	if (vm.predictionType === 'PROPENSITY') {
    		vm.prioritizeBy = 'Likely to Buy';
    	} else if (vm.predictionType === 'EXPECTED_VALUE') {
    		vm.prioritizeBy = 'Likely Amount of Spend';
    	}

    	if (vm.configFilters['SPEND_IN_PERIOD']) {
    		if (vm.configFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
	    		vm.spendCriteria = 'at least';
	    	} else {
	    		vm.spendCriteria = 'at most';
	    	}
	    }

	    if (vm.configFilters['QUANTITY_IN_PERIOD']) {
	    	if (vm.configFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
	    		vm.quantityCriteria = 'at least';
	    	} else {
	    		vm.quantityCriteria = 'at most';
	    	}
	    }

        if (vm.targetProducts.length === 1) {
            vm.targetProductName = vm.returnProductNameFromId(vm.targetProducts[0]);
        }
        if (vm.trainingProducts.length === 1) {
            vm.trainingProductName = vm.returnProductNameFromId(vm.trainingProducts[0]);
        }

    };

    vm.checkJobStatus = $interval(function() { 
        JobsStore.getJobFromApplicationId(vm.ratingEngine.activeModel.AI.modelingJobId).then(function(result) {
            
            if(result.id) {
                vm.status = result.jobStatus;

                vm.jobStarted = true;
                vm.startTimestamp = result.startTimestamp;
            
                vm.completedSteps = result.completedTimes;

                vm.loadingData = vm.startTimestamp && !vm.completedSteps.load_data;
                vm.matchingToDataCloud = vm.completedSteps.load_data && !vm.completedSteps.create_global_target_market;
                vm.scoringTrainingSet = vm.completedSteps.create_global_target_market && !vm.completedSteps.score_training_set;

                // Green status bar
                if(result.stepsCompleted.length > 0){
                    vm.progress = ((result.stepsCompleted.length / 2) * 7) + '%';
                }
                // Cancel $interval when completed
                if(vm.status === 'Completed'){
                    $interval.cancel(vm.checkJobStatus);
                }
            }
        });
    }, 10 * 1000);

    $scope.$on('$destroy', function(){
        $interval.cancel(vm.checkJobStatus)
    });

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.setValidation = function (type, validated) {
        RatingsEngineStore.setValidation(type, validated);
    };

    vm.init();

});