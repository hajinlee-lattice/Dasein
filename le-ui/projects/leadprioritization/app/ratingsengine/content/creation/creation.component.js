angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, $interval,
    Rating, RatingsEngineStore, JobsStore
) {
    var vm = this;

    angular.extend(vm, {
        ratingEngine: Rating,
        status: 'Preparing Modeling Job',
        progress: '1%'
    });

    vm.init = function() {

    	// console.log(vm.ratingEngine);

    	var model = vm.ratingEngine.activeModel.AI;

    	vm.targetProducts = model.targetProducts;
        vm.modelingStrategy = model.modelingStrategy;
        vm.predictionType = model.predictionType;
        vm.modelingConfigFilters = model.modelingConfigFilters;
        vm.trainingSegment = model.trainingSegment;
        vm.trainingProducts = model.trainingProducts;

    	if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
        	vm.ratingEngineType = 'First Purchase Cross-Sell'
        } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
        	vm.ratingEngineType = 'Returning Purchase Cross-Sell'
        }

    	if (vm.predictionType === 'PROPENSITY') {
    		vm.prioritizeBy = 'Likely to buy';
    	} else if (vm.predictionType === 'EXPECTED_VALUE') {
    		vm.prioritizeBy = 'Likely to spend';
    	}

    	if (vm.modelingConfigFilters['SPEND_IN_PERIOD']) {
    		if (vm.modelingConfigFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
	    		vm.spendCriteria = 'at least';
	    	} else {
	    		vm.spendCriteria = 'at most';
	    	}
	    }

	    if (vm.modelingConfigFilters['QUANTITY_IN_PERIOD']) {
	    	if (vm.modelingConfigFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
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


        var checkJobStatus = $interval(function() { 
            JobsStore.getJobFromApplicationId(vm.ratingEngine.activeModel.AI.modelingJobId).then(function(result) {
                
                console.log(result);
                if(result.id) {
                    vm.status = result.jobStatus;

                    if(result.stepsCompleted.length > 0){
                        vm.progress = ((result.stepsCompleted.length / 2) * 7) + '%';
                    }
                    if(vm.status === 'Completed'){
                        $interval.cancel(checkJobStatus);
                    }
                }
            });
        }, 10 * 1000);

    };

    $scope.$on('$destroy', function(){
        $interval.cancel(checkJobStatus)
    });

    vm.returnProductNameFromId = function(productId) {
        var cachedProducts = RatingsEngineStore.getCachedProducts(),
            product = cachedProducts.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.init();

});