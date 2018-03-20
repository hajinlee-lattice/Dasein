angular.module('lp.ratingsengine.wizard.training', [
    'mainApp.appCommon.directives.chips',
    'mainApp.appCommon.directives.formOnChange'
])
.controller('RatingsEngineAITraining', function (
    $q, $scope, $stateParams, $timeout,
    Rating, RatingsEngineStore, RatingsEngineService, SegmentService, Segments, Products) {
    var vm = this;
    angular.extend(vm, {
        segments: Segments,
        products: Products,
        spendCriteria: "GREATER_OR_EQUAL",
        spendValue: 1500,
        quantityCriteria: "GREATER_OR_EQUAL",
        quantityValue: 2,
        periodsCriteria: "WITHIN",
        periodsValue: 2,
        configFilters: RatingsEngineStore.getConfigFilters(),
        trainingSegment: null,
        trainingProducts: null,
        ratingEngine: Rating,
        scoringCountReturned: false,
        recordsCountReturned: false,
        purchasesCountReturned: false
    });

    vm.init = function () {
        
        vm.engineId = vm.ratingEngine.id;
        vm.modelId = vm.ratingEngine.activeModel.AI.id;
        vm.modelingStrategy = vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.modelingStrategy;

        console.log(vm.configFilters);
        console.log(vm.ratingEngine);

        vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
        vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
        vm.getScoringCount(vm.engineId, vm.modelId, vm.ratingEngine);

        $scope.$on("$destroy", function() {
            delete vm.configFilters.SPEND_IN_PERIOD;
            delete vm.configFilters.QUANTITY_IN_PERIOD;
            delete vm.configFilters.TRAINING_SET_PERIOD;
        });

    }

    vm.getRecordsCount = function(engineId, modelId, ratingEngine) {
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'TRAINING').then(function(count){
            vm.recordsCount = count;
            vm.recordsCountReturned = true;
        });
    }
    vm.getPurchasesCount = function(engineId, modelId, ratingEngine) {
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'EVENT').then(function(count){
            vm.purchasesCount = count;
            vm.purchasesCountReturned = true;
        });
    }
    vm.getScoringCount = function(engineId, modelId, ratingEngine) {
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'TARGET').then(function(count){
            vm.scoringCount = count;
            vm.scoringCountReturned = true;
        });
    }

    vm.segmentCallback = function(selectedSegment) {
        vm.trainingSegment = selectedSegment[0];
        RatingsEngineStore.setTrainingSegment(vm.trainingSegment);
        vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.trainingSegment = vm.trainingSegment;

        console.log(vm.ratingEngine);

        vm.autcompleteChange();
    }

    vm.productsCallback = function(selectedProducts) {
        vm.trainingProducts = [];
        angular.forEach(selectedProducts, function(product){
            vm.trainingProducts.push(product.ProductId);
        });
        RatingsEngineStore.setTrainingProducts(vm.trainingProducts);
        vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.trainingProducts = vm.trainingProducts;

        vm.autcompleteChange();        
    }

    vm.autcompleteChange = function(){
        vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
        vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
    };

    vm.formOnChange = function(){
        vm.recordsCountReturned = false;
        vm.purchasesCountReturned = false;

        if($scope.checkboxModel) {
            if($scope.checkboxModel.spend) {
                vm.configFilters.SPEND_IN_PERIOD = {
                    "configName": "SPEND_IN_PERIOD",
                    "criteria": vm.spendCriteria,
                    "value": vm.spendValue
                };
            } else {
                delete vm.configFilters.SPEND_IN_PERIOD;
            }

            if($scope.checkboxModel.quantity) {
                vm.configFilters.QUANTITY_IN_PERIOD = {
                    "configName": "QUANTITY_IN_PERIOD",
                    "criteria": vm.quantityCriteria,
                    "value": vm.quantityValue
                };
            } else {
                delete vm.configFilters.QUANTITY_IN_PERIOD;
            }

            if($scope.checkboxModel.periods) {
                vm.configFilters.TRAINING_SET_PERIOD = {
                    "configName": "TRAINING_SET_PERIOD",
                    "criteria": vm.periodsCriteria,
                    "value": vm.periodsValue
                };
            } else {
                delete vm.configFilters.TRAINING_SET_PERIOD;
            }

            if($scope.checkboxModel.spend || $scope.checkboxModel.quantity || $scope.checkboxModel.periods) {
                RatingsEngineStore.setConfigFilters(vm.configFilters);
            }

            vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.filters = vm.configFilters;

            vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
            vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
        }

    };

    $scope.$on('$destroy', function() {
        delete vm.configFilters.SPEND_IN_PERIOD;
        delete vm.configFilters.QUANTITY_IN_PERIOD;
        delete vm.configFilters.TRAINING_SET_PERIOD;
    });

    vm.init();
});