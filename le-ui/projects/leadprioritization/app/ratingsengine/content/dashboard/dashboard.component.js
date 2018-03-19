angular.module('lp.ratingsengine.dashboard', [
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, 
    RatingsEngineStore, RatingsEngineService, 
    Dashboard, RatingEngine, Model, IsRatingEngine, IsPmml, Products
) {
    var vm = this;

    angular.extend(vm, {
        dashboard: Dashboard,
        ratingEngine: RatingEngine,
        products: Products
    });

    vm.init = function() {
        // console.log(vm.dashboard);
        console.log(vm.ratingEngine);

        vm.relatedItems = vm.dashboard.plays;
        vm.isRulesBased = (vm.ratingEngine.type === 'RULE_BASED');
        vm.hasBuckets = vm.ratingEngine.counts != null;

        var model = vm.ratingEngine.activeModel;
        
        if(!vm.isRulesBased){

            if((Object.keys(model.AI.modelingConfigFilters).length === 0 || (model.AI.modelingConfigFilters['PURCHASED_BEFORE_PERIOD'] && Object.keys(model.AI.modelingConfigFilters).length === 1)) && model.AI.trainingSegment === null && model.AI.trainingProducts.length === 0) {
                vm.hasSettingsInfo = false;
            }

            vm.targetProducts = model.AI.targetProducts;
            vm.modelingStrategy = model.AI.modelingStrategy;
            vm.predictionType = model.AI.predictionType;
            vm.modelingConfigFilters = model.AI.modelingConfigFilters;
            vm.trainingSegment = model.AI.trainingSegment;
            vm.trainingProducts = model.AI.trainingProducts;

            if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                vm.ratingEngineType = 'First Purchase Cross-Sell'
            } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
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
        }

    }

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.init();
});
