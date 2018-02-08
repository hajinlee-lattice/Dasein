angular.module('lp.ratingsengine.wizard.training', [
    'mainApp.appCommon.directives.chips',
    'mainApp.appCommon.directives.formOnChange'
])
.controller('RatingsEngineAITraining', function ($q, $scope, RatingsEngineStore, RatingsEngineService, SegmentService) {
    var vm = this;
    angular.extend(vm, {
        segments: RatingsEngineStore.getCachedSegments(),
        products: RatingsEngineStore.getCachedProducts(),
        spendCriteria: "GREATER_OR_EQUAL",
        spendValue: 1500,
        quantityCriteria: "GREATER_OR_EQUAL",
        quantityValue: 2,
        periodsCriteria: "WITHIN",
        periodsValue: 2,
        modelingConfigFilters: {},
        trainingSegment: null,
        trainingProducts: null
    });

    vm.init = function () {
    }

    vm.segmentCallback = function(selectedSegment) {
        vm.trainingSegment = selectedSegment[0];
        RatingsEngineStore.setTrainingSegment(vm.trainingSegment);
    }

    vm.productsCallback = function(selectedProducts) {
        vm.trainingProducts = [];
        angular.forEach(selectedProducts, function(product){
            vm.trainingProducts.push(product.ProductId);
        });
        RatingsEngineStore.setTrainingProducts(vm.trainingProducts);
    }

    vm.formOnChange = function(){
        if($scope.checkboxModel) {
            if($scope.checkboxModel.spend) {
                vm.modelingConfigFilters.SPEND_IN_PERIOD = {
                    "configName": "SPEND_IN_PERIOD",
                    "criteria": vm.spendCriteria,
                    "value": vm.spendValue
                };
            } else {
                delete vm.modelingConfigFilters.SPEND_IN_PERIOD;
            }

            if($scope.checkboxModel.quantity) {
                vm.modelingConfigFilters.QUANTITY_IN_PERIOD = {
                    "configName": "QUANTITY_IN_PERIOD",
                    "criteria": vm.quantityCriteria,
                    "value": vm.quantityValue
                };
            } else {
                delete vm.modelingConfigFilters.QUANTITY_IN_PERIOD;
            }

            if($scope.checkboxModel.periods) {
                vm.modelingConfigFilters.TRAINING_SET_PERIOD = {
                    "configName": "TRAINING_SET_PERIOD",
                    "criteria": vm.periodsCriteria,
                    "value": vm.periodsValue
                };
            } else {
                delete vm.modelingConfigFilters.TRAINING_SET_PERIOD;
            }

            if($scope.checkboxModel.spend || $scope.checkboxModel.quantity || $scope.checkboxModel.periods) {
                RatingsEngineStore.setModelingConfigFilters(vm.modelingConfigFilters);
            }
        }

    };

    vm.init();
});