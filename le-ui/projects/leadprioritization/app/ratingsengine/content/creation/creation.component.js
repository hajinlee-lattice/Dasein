angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, Rating, RatingsEngineStore, JobsStore
) {
    var vm = this;

    angular.extend(vm, {
        ratingEngine: Rating
    });

    vm.init = function() {

    	console.log(vm.ratingEngine);

    	var model = vm.ratingEngine.activeModel.AI;

    	vm.targetProducts = model.targetProducts;
        vm.modelingStrategy = model.modelingStrategy;
        vm.predictionType = model.predictionType;
        vm.modelingConfigFilters = model.modelingConfigFilters;
        vm.trainingSegment = model.trainingSegment;
        vm.trainingProducts = model.trainingProducts;

        console.log(vm.targetProducts);

    	if (vm.targetProducts.length === 1) {
    		var productId = vm.targetProducts[0].toString(),
    			cachedProducts = RatingsEngineStore.getCachedProducts(),
    			product = cachedProducts.find(function(obj) { return obj.ProductId === productId });

    		vm.productName = product.ProductName;
    	};

    	if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
        	vm.ratingEngineType = 'First Purchase Cross-Sell'
        } else if (vm.modelingStrategy === 'CROSS_SELL_RETURNING_PURCHASE') {
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


        JobsStore.getJob(vm.ratingEngine.activeModel.AI.modelingJobId).then(function(result) {
            console.log(result);
        });
    	
    };

    vm.init();

});