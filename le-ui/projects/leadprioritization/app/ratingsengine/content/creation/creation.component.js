angular.module('lp.ratingsengine.wizard.creation', [])
.controller('RatingsEngineCreation', function (
    $q, $state, $stateParams, $scope, $interval,
    Rating, RatingsEngineStore, JobsStore, Products
) {
    var vm = this,
        checkJobStatus;

    angular.extend(vm, {
        ratingEngine: Rating,
        products: Products,
        hasSettingsInfo: true,
        status: 'Preparing Modeling Job',
        progress: '1%',
        modelSettingsSummary: {
            'cross_sell': {
                'segment': true,
                'products': true,
                'availableAttributes': false,
                'scoreExternalFile': false,
                'prioritizeBy': true,
                'modelSettingsTitle': true
            },
            'custom_event': {
                'segment': RatingsEngineStore.getCustomEventModelingType() == 'CDL',
                'products': false,
                'availableAttributes': true,
                'scoreExternalFile': true,
                'prioritizeBy': false,
                'modelSettingsTitle': false
            }
        }
    });

    vm.init = function() {

        vm.setValidation('creation', true);

    	var model = vm.ratingEngine.activeModel.AI;
        vm.type = vm.ratingEngine.type.toLowerCase();

        vm.predictionType = model.predictionType;  
        vm.trainingSegment = model.trainingSegment;

        if (vm.type === 'cross_sell') {

            console.log(model);

            if((Object.keys(model.advancedModelingConfig.cross_sell.filters) === {} || (model.advancedModelingConfig.cross_sell.filters['PURCHASED_BEFORE_PERIOD'] && Object.keys(model.advancedModelingConfig.cross_sell.filters).length === 1)) && model.trainingSegment === null && model.advancedModelingConfig.cross_sell.trainingProducts === null) {
                vm.hasSettingsInfo = false;
            }

            vm.targetProducts = model.advancedModelingConfig.cross_sell.targetProducts;
            vm.modelingStrategy = model.advancedModelingConfig.cross_sell.modelingStrategy;
            vm.configFilters = model.advancedModelingConfig.cross_sell.filters;
            vm.trainingProducts = model.advancedModelingConfig.cross_sell.trainingProducts;

            if (vm.targetProducts.length === 0) {
                vm.modelSettingsSummary.cross_sell.products = false;
            }
            
            if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                vm.ratingEngineType = 'First Purchase Cross-Sell'
            } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
            }

            if (vm.predictionType === 'PROPENSITY') {
                vm.prioritizeBy = 'Likely to Buy';
            } else if (vm.predictionType === 'EXPECTED_VALUE') {
                vm.prioritizeBy = 'Likely Amount of Spend';
            }

            if (vm.configFilters['SPEND_IN_PERIOD']) {
                if (vm.configFilters['SPEND_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                    vm.spendCriteria = 'at least';
                } else {
                    vm.spendCriteria = 'at most';
                }
            }

            if (vm.configFilters['QUANTITY_IN_PERIOD']) {
                if (vm.configFilters['QUANTITY_IN_PERIOD'].criteria === 'GREATER_OR_EQUAL') {
                    vm.quantityCriteria = 'at least';
                } else {
                    vm.quantityCriteria = 'at most';
                }
            }

            if (vm.targetProducts !== null) {
                vm.targetProductName = vm.returnProductNameFromId(vm.targetProducts[0]);
            }
            if (vm.trainingProducts !== null && vm.trainingProducts != undefined) {
                vm.trainingProductName = vm.returnProductNameFromId(vm.trainingProducts[0]);
            }

        } else if (vm.type == 'custom_event') {
            vm.hasSettingsInfo = true;
            vm.ratingEngineType = 'Custom Event'
            vm.prioritizeBy = 'Likely to Buy';

            var dataStore = model.advancedModelingConfig.custom_event.dataStores;
            vm.availableAttributes = dataStore.length == 1 ? vm.formatTrainingAttributes(dataStore[0]) : vm.formatTrainingAttributes(dataStore[0]) + ' + ' + vm.formatTrainingAttributes(dataStore[1]);

        }

    };

    vm.checkJobStatus = $interval(function() { 
        var appId = vm.ratingEngine.activeModel.AI.modelingJobId ? vm.ratingEngine.activeModel.AI.modelingJobId : RatingsEngineStore.getApplicationId(); // update once backend sets modelingjobId for CE
        JobsStore.getJobFromApplicationId(appId).then(function(result) {
            console.log(result);
            if(result.id) {
                vm.status = result.jobStatus;

                vm.jobStarted = true;
                vm.startTimestamp = result.startTimestamp;
            
                vm.completedSteps = result.completedTimes;

                vm.loadingData = vm.startTimestamp && !vm.completedSteps.load_data;
                vm.matchingToDataCloud = vm.completedSteps.load_data && !vm.completedSteps.create_global_target_market;
                vm.scoringTrainingSet = vm.completedSteps.create_global_target_market && !vm.completedSteps.score_training_set;

                // Green status bar
                if(result.stepsCompleted.length > 0){
                    vm.progress = ((result.stepsCompleted.length / 2) * 7) + '%';
                }
                // Cancel $interval when completed
                if(vm.status === 'Completed'){
                    $interval.cancel(vm.checkJobStatus);
                }
            }
        });
    }, 10 * 1000);

    $scope.$on('$destroy', function(){
        $interval.cancel(vm.checkJobStatus)
    });

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.setValidation = function (type, validated) {
        RatingsEngineStore.setValidation(type, validated);
    };


    vm.getTrainingFileName = function() {
        return RatingsEngineStore.getDisplayFileName();
    }

    vm.formatTrainingAttributes = function(type) {
        switch (type) {
            case 'DataCloud':
                return 'Lattice Data Cloud';
            case 'CDL':
                return 'Lattice Database';
            case 'CustomFileAttributes':
                return 'Training File';
        }
    }

    vm.showSetting = function(setting) {
        return vm.modelSettingsSummary[vm.type][setting];
    }

    vm.init();

});