angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, Rating, JobsStore
) {
    var vm = this;

    angular.extend(vm, {
        ratingEngine: Rating
    });

    vm.init = function() {

    	vm.products = vm.ratingEngine.activeModel.AI.targetProducts;
    	if (vm.products.length === 1) {
    		vm.productName = vm.products;
    	}

    	if (vm.ratingEngine.activeModel.AI.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
        	vm.ratingEngineType = 'First Purchase Cross-Sell'
        } else if (vm.ratingEngine.activeModel.AI.modelingStrategy === 'CROSS_SELL_RETURNING_PURCHASE') {
        	vm.ratingEngineType = 'Returning Purchase Cross-Sell'
        }

    	if (vm.ratingEngine.activeModel.AI.predictionType === 'PROPENSITY') {
    		vm.prioritizeBy = 'Likely to buy';
    	} else if (vm.ratingEngine.activeModel.AI.predictionType === 'EXPECTED_VALUE') {
    		vm.prioritizeBy = 'Likely to spend';
    	}

    	console.log(vm.ratingEngine.activeModel.AI.modelingConfigFilters);

        JobsStore.getJob(vm.ratingEngine.activeModel.AI.modelingJobId).then(function(result) {
            console.log(result);
        });
    	
    };

    vm.init();

});