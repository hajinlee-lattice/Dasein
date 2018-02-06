angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, Rating, JobsStore
) {
    var vm = this;

    angular.extend(vm, {
        ratingEngine: Rating
    });

    vm.init = function() {
    	console.log(vm.ratingEngine);
    	console.log($state);
    	console.log($stateParams);

    	vm.products = vm.ratingEngine.activeMode.AI.targetProducts;
    	if (vm.products.length === 1) {
    		vm.productName = vm.products;
    	}

        JobsStore.getJob(vm.ratingEngine.activeModel.AI.modelingJobId).then(function(result) {
            console.log(result);
        });

        if (vm.ratingEngine.type === 'AI_BASED') {
        	if (vm.ratingEngine.activeModel.AI.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
	        	vm.ratingEngineType = 'First Purchase Cross-Sell'
	        } else if (vm.ratingEngine.activeModel.AI.modelingStrategy === 'CROSS_SELL_RETURNING_PURCHASE') {
	        	vm.ratingEngineType = 'Returning Purchase Cross-Sell'
	        }
        }
    };

    vm.init();

});