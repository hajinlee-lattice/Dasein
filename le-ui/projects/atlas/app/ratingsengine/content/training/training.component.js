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
        datacollectionstatus: '<',
        iteration: '<'
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
            configFilters: angular.copy(RatingsEngineStore.getConfigFilters()),
            trainingSegment: null,
            trainingProducts: null,
            scoringCountReturned: false,
            recordsCountReturned: false,
            purchasesCountReturned: false,
            pageTitle: ($stateParams.section === 'wizard.ratingsengine_segment') ? 'How do you want to train the model?' : 'Do you want to change the way the model is trained?',
            checkboxModel: {},
            repeatPurchaseRemodel: false,
        });
        

        vm.getNumericalConfig = function(){
            var config = {debounce: 800};
            var ret = JSON.stringify(config);
            return ret;
        };

        vm.$onInit = function() {

            AtlasRemodelStore.setRemodelIteration(vm.iteration);

            vm.ratingModel = vm.iteration ? vm.iteration.AI : vm.ratingEngine.latest_iteration.AI;
            vm.engineType = vm.ratingEngine.type.toLowerCase();
            vm.periodType = vm.datacollectionstatus.ApsRollingPeriod ? vm.datacollectionstatus.ApsRollingPeriod.toLowerCase() : 'quarter';

            if($stateParams.section != "wizard.ratingsengine_segment"){
                if(vm.engineType == 'cross_sell'){

                    vm.filters = vm.ratingModel.advancedModelingConfig.cross_sell.filters;

                    vm.repeatPurchase = (vm.ratingEngine.advancedRatingConfig.cross_sell.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') ? true : false;
                    if(vm.repeatPurchase){
                        vm.purchasedBeforePeriod = vm.filters.PURCHASED_BEFORE_PERIOD.value;
                        vm.repeatPurchaseRemodel = true;
                    }

                    vm.trainingSegment = vm.ratingModel.trainingSegment ? vm.ratingModel.trainingSegment : null;

                    vm.trainingProducts = vm.ratingModel.advancedModelingConfig.cross_sell.trainingProducts ? vm.ratingModel.advancedModelingConfig.cross_sell.trainingProducts : null;

                    // Setup form for Cross Sell Models
                    vm.checkboxModel = {
                        spend: vm.filters.SPEND_IN_PERIOD ? true : false,
                        quantity: vm.filters.QUANTITY_IN_PERIOD ? true : false,
                        periods: vm.filters.TRAINING_SET_PERIOD ? true : false
                    };

                    vm.spendCriteria = vm.filters.SPEND_IN_PERIOD ? vm.filters.SPEND_IN_PERIOD.criteria : "GREATER_OR_EQUAL";
                    vm.spendValue = vm.filters.SPEND_IN_PERIOD ? vm.filters.SPEND_IN_PERIOD.value : 1500;

                    vm.quantityCriteria = vm.filters.QUANTITY_IN_PERIOD ? vm.filters.QUANTITY_IN_PERIOD.criteria : "GREATER_OR_EQUAL";
                    vm.quantityValue = vm.filters.QUANTITY_IN_PERIOD ? vm.filters.QUANTITY_IN_PERIOD.value : 2;

                    vm.periodsCriteria = vm.filters.TRAINING_SET_PERIOD ? vm.filters.TRAINING_SET_PERIOD.criteria : "WITHIN";
                    vm.periodsValue = vm.filters.TRAINING_SET_PERIOD ? vm.filters.TRAINING_SET_PERIOD.value : 2;

                    vm.validateCrossSellForm();

                } else {
                    // Setup form for Custom Event Models
                    vm.filters = vm.iteration.AI.advancedModelingConfig.custom_event;

                    RatingsEngineStore.setDisplayFileName(vm.filters.sourceFileName);

                    vm.checkboxModel = {
                        datacloud: (vm.filters.dataStores.indexOf('DataCloud') > -1) ? true : false,
                        cdl: (vm.filters.dataStores.indexOf('CDL') > -1) ? true : false,
                        customFile: (vm.filters.dataStores.indexOf('CustomFileAttributes') > -1) ? true : false,
                        deduplicationType: (vm.filters.deduplicationType == 'ONELEADPERDOMAIN') ? true : false,
                        excludePublicDomains: (vm.filters.excludePublicDomains == true) ? false : true,
                        transformationGroup: (vm.filters.transformationGroup == 'NONE') ? false : true
                    }

                    console.log(vm.checkboxModel);

                    vm.configFilters = angular.copy(vm.filters);

                    vm.configFilters.dataStores = [];
                    if(vm.checkboxModel.datacloud === true) {
                        vm.configFilters.dataStores.push('DataCloud');
                    }
                    if(vm.checkboxModel.cdl === true) {
                        vm.configFilters.dataStores.push('CDL');
                    }
                    if(vm.checkboxModel.customFile === true) {
                        vm.configFilters.dataStores.push('CustomFileAttributes');
                    }

                    vm.checkForDisable();
                    vm.validateCustomEventForm();
                }
            }

            vm.engineId = vm.ratingEngine.id;            
            vm.modelId = vm.ratingModel.id;

            vm.modelingStrategy = vm.ratingModel.advancedModelingConfig[vm.engineType].modelingStrategy;

            if(vm.engineType == 'cross_sell'){
                vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
                vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
                vm.getScoringCount(vm.engineId, vm.modelId, vm.ratingEngine);
            }
        }




        // Functions for Cross Sell Models
        // ============================================================================================
        // ============================================================================================
        // ============================================================================================

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
            vm.ratingModel.trainingSegment = vm.trainingSegment;
            vm.updateSegmentSelected(vm.trainingSegment);
            vm.autcompleteChange();
        }
        vm.updateSegmentSelected = function(trainingSegment) {
            if(vm.ratingEngine.latest_iteration.AI){
                vm.ratingEngine.latest_iteration.AI.trainingSegment = (trainingSegment ? trainingSegment : null);
            }
        }

        vm.productsCallback = function(selectedProducts) {
            vm.trainingProducts = [];
            angular.forEach(selectedProducts, function(product){
                vm.trainingProducts.push(product.ProductId);
            });
            vm.updateProductsSelected(vm.trainingProducts);
            RatingsEngineStore.setTrainingProducts(vm.trainingProducts);
            vm.ratingModel.advancedModelingConfig.cross_sell.trainingProducts = vm.trainingProducts;

            vm.autcompleteChange();        
        }
        vm.updateProductsSelected = function(listProducts) {
            if(vm.ratingEngine.latest_iteration.AI){
                vm.ratingEngine.latest_iteration.AI.advancedModelingConfig.cross_sell.trainingProducts = listProducts;
            }
        }

        vm.autcompleteChange = function(){
            vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
            vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
        };
       
        vm.getSpendConfig = function(){
            return {
                from: { name: 'from-spend', value: vm.spendValue, position: 0, type: 'Spend', min: '0', max: '2147483647', pattern:'\\\d+' },
                to: { name: 'to-spend', value: vm.spendValue, position: 1, type: 'Spend', min: '0', max: '2147483647', pattern:'\\\d+' }
            };
        }

        vm.getQuantityConfig = function(){
            return {
                from: { name: 'from-quantity', value: vm.quantityValue, position: 0, type: 'Quantity', min: '0', max: '2147483647', disabled: true, visible: true, pattern:'\\\d+'},
                to: { name: 'to-quantity', value: vm.quantityValue, position: 1, type: 'Quantity', min: '0', max: '2147483647', disbaled: true, visible: false, pattern:'\\\d+' }
            };
        }

        vm.getPeriodConfig = function(){
            return {
                from: { name: 'from-period', value: vm.periodsValue, position: 0, type: 'Period', min: '0', max: '2147483647', pattern:'\\\d+' },
                to: { name: 'to-period', value: vm.periodsValue, position: 1, type: 'Period', min: '0', max: '2147483647', pattern:'\\\d+' }
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

        vm.validateCrossSellForm = function(){

            var valid = true;
            if($scope.trainingForm){
                valid = $scope.trainingForm.$valid;
            }

            if(valid == true){
                RatingsEngineStore.setValidation('training', true);
                RatingsEngineStore.setValidation('refine', true);
                vm.recordsCountReturned = false;
                vm.purchasesCountReturned = false;

                if(vm.repeatPurchaseRemodel) {
                    vm.configFilters.PURCHASED_BEFORE_PERIOD = {
                        "configName": "PURCHASED_BEFORE_PERIOD",
                        "criteria": "PRIOR_ONLY",
                        "value": vm.purchasedBeforePeriod
                    };
                    vm.ratingModel.advancedModelingConfig.cross_sell.filters = vm.configFilters;
                }

                if(vm.checkboxModel.spend) {
                    vm.configFilters.SPEND_IN_PERIOD = {
                        "configName": "SPEND_IN_PERIOD",
                        "criteria": vm.spendCriteria,
                        "value": vm.spendValue
                    };
                } else {
                    delete vm.configFilters.SPEND_IN_PERIOD;
                }

                if(vm.checkboxModel.quantity) {
                    vm.configFilters.QUANTITY_IN_PERIOD = {
                        "configName": "QUANTITY_IN_PERIOD",
                        "criteria": vm.quantityCriteria,
                        "value": vm.quantityValue
                    };
                } else {
                    delete vm.configFilters.QUANTITY_IN_PERIOD;
                }

                if(vm.checkboxModel.periods) {
                    vm.configFilters.TRAINING_SET_PERIOD = {
                        "configName": "TRAINING_SET_PERIOD",
                        "criteria": vm.periodsCriteria,
                        "value": vm.periodsValue
                    };
                } else {
                    delete vm.configFilters.TRAINING_SET_PERIOD;
                }
                    
                vm.ratingEngine.latest_iteration.AI.advancedModelingConfig.cross_sell.filters = vm.configFilters;

                // console.log(vm.ratingEngine);
                // console.log(vm.engineId, vm.modelId, vm.ratingModel);

                $timeout(function () {
                    RatingsEngineStore.setConfigFilters(vm.configFilters);
                    vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
                    vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
                }, 1500);

            } else {
                RatingsEngineStore.setValidation('training', false);
                RatingsEngineStore.setValidation('refine', false);
            }
        }

        // Functions for Custom Event Models
        // ============================================================================================
        // ============================================================================================
        // ============================================================================================
        
        vm.checkForDisable = function(){

            RatingsEngineStore.setValidation('training', false);

            if(vm.checkboxModel.datacloud == false && vm.checkboxModel.cdl == false){
                RatingsEngineStore.setValidation('training', false);
            }
        }

        vm.validateCustomEventForm = function(){

            var valid = true;
            if($scope.trainingForm){
                valid = $scope.trainingForm.$valid;
            }

            if(valid == true){
                // RatingsEngineStore.setConfigFilters(vm.configFilters);
                vm.dataStores = vm.configFilters.dataStores;

                if (vm.checkboxModel.datacloud){
                    $timeout(function () {
                        if(vm.dataStores.indexOf('DataCloud') == -1){
                            vm.configFilters.dataStores.push('DataCloud');
                        }
                    }, 100);
                } else {
                    var index = vm.dataStores.indexOf('DataCloud');
                    vm.configFilters.dataStores.splice(index, 1);
                }

                if(vm.checkboxModel.cdl) {
                    $timeout(function () {
                        if(vm.dataStores.indexOf('CDL') == -1){
                            vm.configFilters.dataStores.push('CDL');
                        }
                    }, 100);
                } else {
                    var index = vm.dataStores.indexOf('CDL');
                    vm.configFilters.dataStores.splice(index, 1);
                }

                if(vm.checkboxModel.customFile) {
                    $timeout(function () {
                        if(vm.dataStores.indexOf('CustomFileAttributes') == -1){
                            vm.configFilters.dataStores.push('CustomFileAttributes');
                        }
                    }, 200);
                } else {
                    var index = vm.dataStores.indexOf('CustomFileAttributes');
                    vm.configFilters.dataStores.splice(index, 1);
                }

                if(vm.checkboxModel.deduplicationType) {
                    vm.configFilters.deduplicationType = 'ONELEADPERDOMAIN';
                } else {
                    vm.configFilters.deduplicationType = 'MULTIPLELEADSPERDOMAIN';
                }

                vm.configFilters.excludePublicDomains = vm.checkboxModel.excludePublicDomains ? false : true;

                if(!vm.checkboxModel.transformationGroup) {
                    vm.configFilters.transformationGroup = 'NONE';
                } else {
                    vm.configFilters.transformationGroup = 'ALL';
                }

                $timeout(function () {
                    RatingsEngineStore.setConfigFilters(vm.configFilters);
                    RatingsEngineStore.setValidation('training', true);
                    vm.ratingModel.advancedModelingConfig.custom_event = vm.configFilters;
                }, 150);

            } else {
                RatingsEngineStore.setValidation('training', false);
                RatingsEngineStore.setValidation('refine', false);
            }

        }

        vm.formOnChange = function(){
            $timeout(function () {
                if(vm.engineType === 'cross_sell'){
                    vm.validateCrossSellForm();
                } else {
                    vm.validateCustomEventForm();
                }
            }, 1500);
        };
        
    }
});