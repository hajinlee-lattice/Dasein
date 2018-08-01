angular.module('lp.ratingsengine.wizard.training', [
    'mainApp.appCommon.directives.chips',
    'mainApp.appCommon.directives.formOnChange',
    'common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range'
])
.component('ratingsEngineAITraining', {
    templateUrl: 'app/ratingsengine/content/training/training.component.html',
    bindings: {
        ratingEngine: '<',
        segments: '<',
        products: '<',
    },
    controller: function (
        $q, $scope, $stateParams, $timeout,
        RatingsEngineStore, RatingsEngineService, SegmentService, AtlasRemodelStore
    ) {

        var vm = this;

        angular.extend(vm, {
            spendCriteria: "GREATER_OR_EQUAL",
            spendValue: 1500,
            quantityCriteria: "GREATER_OR_EQUAL",
            quantityValue: 2,
            periodsCriteria: "WITHIN",
            periodsValue: 2,
            configFilters: RatingsEngineStore.getConfigFilters(),
            trainingSegment: null,
            trainingProducts: null,
            scoringCountReturned: false,
            recordsCountReturned: false,
            purchasesCountReturned: false,
            iteration: AtlasRemodelStore.getRemodelIteration(),
            pageTitle: ($stateParams.section === 'wizard.ratingsengine_segment') ? 'How do you want to train the model?' : 'Do you want to change the way the model is trained?'
        });

        vm.getNumericalConfig = function(){
            var config = {debounce: 800};
            var ret = JSON.stringify(config);
            return ret;
        };

        vm.$onInit = function() {

            if($stateParams.section != "wizard.ratingsengine_segment"){

                var filters = vm.ratingEngine.activeModel.AI.advancedModelingConfig.cross_sell.filters;

                $scope.checkboxModel = {
                    spend: filters.SPEND_IN_PERIOD ? true : false,
                    quantity: filters.QUANTITY_IN_PERIOD ? true : false,
                    periods: filters.TRAINING_SET_PERIOD ? true : false
                };

                vm.spendCriteria = filters.SPEND_IN_PERIOD ? filters.SPEND_IN_PERIOD.criteria : '';
                vm.spendValue = filters.SPEND_IN_PERIOD ? filters.SPEND_IN_PERIOD.value : '';

                vm.quantityCriteria = filters.QUANTITY_IN_PERIOD ? filters.QUANTITY_IN_PERIOD.criteria : '';
                vm.quantityValue = filters.QUANTITY_IN_PERIOD ? filters.QUANTITY_IN_PERIOD.value : '';

                vm.periodsCriteria = filters.TRAINING_SET_PERIOD ? filters.TRAINING_SET_PERIOD.criteria : '';
                vm.periodsValue = filters.TRAINING_SET_PERIOD ? filters.TRAINING_SET_PERIOD.value : '';

            }

            vm.activeModel = vm.iteration ? vm.iteration.AI : vm.ratingEngine.activeModel.AI; 

            vm.engineId = vm.ratingEngine.id;            
            vm.modelId = vm.activeModel.id;
            vm.modelingStrategy = vm.activeModel.advancedModelingConfig.cross_sell.modelingStrategy;

            vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
            vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
            vm.getScoringCount(vm.engineId, vm.modelId, vm.ratingEngine);

            $scope.$on("$destroy", function() {
                delete vm.configFilters.SPEND_IN_PERIOD;
                delete vm.configFilters.QUANTITY_IN_PERIOD;
                delete vm.configFilters.TRAINING_SET_PERIOD;
                
                RatingsEngineStore.setTrainingProducts(null);
                vm.activeModel.advancedModelingConfig.cross_sell.trainingProducts = null;

                RatingsEngineStore.setTrainingSegment(null);
                vm.activeModel.trainingSegment = null;
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
            vm.activeModel.trainingSegment = vm.trainingSegment;

            vm.autcompleteChange();
        }

        vm.productsCallback = function(selectedProducts) {
            vm.trainingProducts = [];
            angular.forEach(selectedProducts, function(product){
                vm.trainingProducts.push(product.ProductId);
            });
            RatingsEngineStore.setTrainingProducts(vm.trainingProducts);
            vm.activeModel.advancedModelingConfig.cross_sell.trainingProducts = vm.trainingProducts;

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

                    vm.activeModel.advancedModelingConfig.cross_sell.filters = vm.configFilters;

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
                from: { name: 'from-spend', value: vm.spendValue, position: 0, type: 'Spend', min: '0', max: '2147483647' },
                to: { name: 'to-spend', value: vm.spendValue, position: 1, type: 'Spend', min: '0', max: '2147483647' }
            };
        }

        vm.getQuantityConfig = function(){
            return {
                from: { name: 'from-quantity', value: vm.quantityValue, position: 0, type: 'Quantity', min: '0', max: '2147483647', disabled: true, visible: true},
                to: { name: 'to-quantity', value: vm.quantityValue, position: 1, type: 'Quantity', min: '0', max: '2147483647', disbaled: true, visible: false }
            };
        }

        vm.getPeriodConfig = function(){
            return {
                from: { name: 'from-period', value: vm.periodsValue, position: 0, type: 'Period', min: '0', max: '2147483647' },
                to: { name: 'to-period', value: vm.periodsValue, position: 1, type: 'Period', min: '0', max: '2147483647' }
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
        
    }
});