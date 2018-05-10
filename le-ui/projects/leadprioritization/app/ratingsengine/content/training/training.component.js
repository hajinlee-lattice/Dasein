angular.module('lp.ratingsengine.wizard.training', [
    'mainApp.appCommon.directives.chips',
    'mainApp.appCommon.directives.formOnChange',
    'common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range'
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

        // console.log(vm.configFilters);
        // console.log(vm.ratingEngine);

        vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
        vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
        vm.getScoringCount(vm.engineId, vm.modelId, vm.ratingEngine);

        $scope.$on("$destroy", function() {
            delete vm.configFilters.SPEND_IN_PERIOD;
            delete vm.configFilters.QUANTITY_IN_PERIOD;
            delete vm.configFilters.TRAINING_SET_PERIOD;
            
            RatingsEngineStore.setTrainingProducts(null);
            vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.trainingProducts = null;

            RatingsEngineStore.setTrainingSegment(null);
            vm.ratingEngine.activeModel.AI.trainingSegment = null;
        });
        $timeout(function () {
            vm.validateForm();
        });
    }

    vm.getRecordsCount = function(engineId, modelId, ratingEngine) {
        vm.recordsCountReturned = false;
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'TRAINING').then(function(count){
            vm.recordsCount = count;
            vm.recordsCountReturned = true;
        });
    }
    vm.getPurchasesCount = function(engineId, modelId, ratingEngine) {
        vm.purchasesCountReturned = false;
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'EVENT').then(function(count){
            vm.purchasesCount = count;
            vm.purchasesCountReturned = true;
        });
    }
    vm.getScoringCount = function(engineId, modelId, ratingEngine) {
        vm.scoringCountReturned = false;
        RatingsEngineStore.getTrainingCounts(engineId, modelId, ratingEngine, 'TARGET').then(function(count){
            vm.scoringCount = count;
            vm.scoringCountReturned = true;
        });
    }

    vm.segmentCallback = function(selectedSegment) {
        vm.trainingSegment = selectedSegment[0];
        RatingsEngineStore.setTrainingSegment(vm.trainingSegment);
        vm.ratingEngine.activeModel.AI.trainingSegment = vm.trainingSegment;

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

    vm.validateForm = function(){
        var valid = true;
        if($scope.trainingForm){
            valid = $scope.trainingForm.$valid;
        }
        if(valid === true){
            RatingsEngineStore.validation.training = true;
            RatingsEngineStore.validation.refine = true;
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
        }else{
            RatingsEngineStore.validation.training = false;
            RatingsEngineStore.validation.refine = false;
        }
    }
    
    vm.formOnChange = function(){
        $timeout(vm.validateForm());
    };
   
    vm.getSpendConfig = function(){
        return {
            from: { name: 'from-spend', value: undefined, position: 0, type: 'Spend', min: '0', max: '2147483647' },
            to: { name: 'to-spend', value: undefined, position: 1, type: 'Spend', min: '0', max: '2147483647' }
        };
    }

    vm.getQuantityConfig = function(){
        return {
            from: { name: 'from-quantity', value: undefined, position: 0, type: 'Quantity', min: '0', max: '2147483647', disabled: true, visible: true},
            to: { name: 'to-quantity', value: undefined, position: 1, type: 'Quantity', min: '0', max: '2147483647', disbaled: true, visible: false }
        };
    }

    vm.getPeriodConfig = function(){
        return {
            from: { name: 'from-period', value: undefined, position: 0, type: 'Period', min: '0', max: '2147483647' },
            to: { name: 'to-period', value: undefined, position: 1, type: 'Period', min: '0', max: '2147483647' }
        };
    }

    vm.getSpendConfigString = function(){
        var ret = JSON.stringify( vm.getSpendConfig());
        return ret;
    }

    vm.getQuantityConfigString = function(){
        var ret = JSON.stringify( vm.getQuantityConfig());
        return ret;
    }

    vm.getPeriodConfigString = function(){
        var ret = JSON.stringify( vm.getPeriodConfig());
        return ret;
    }

    vm.callbackSpend = function(type, position, value){
        if(value){
            vm.spendValue = value;
        }
        vm.formOnChange();
        
    }

    vm.callbackQuantity = function(type, position, value){
        if(value){
            vm.quantityValue = value;
        }
        vm.formOnChange();
        
    }

    vm.callbackPeriod = function(type, position, value){
        if(value){
            vm.periodsValue = value;
        }
        vm.formOnChange();
        
    }
    



    vm.init();
});