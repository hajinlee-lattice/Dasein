angular.module('lp.ratingsengine.wizard.creation', [])
.component('ratingsEngineCreation', {
    templateUrl: 'app/ratingsengine/content/creation/creation.component.html',
    bindings: {
        ratingEngine: '<',
        products: '<',
        datacollectionstatus: '<'
    },
    controller: function(
        $q, $state, $stateParams, $scope, $interval,
        RatingsEngineStore, JobsStore
    ){
        var vm = this,
            checkJobStatus;

        angular.extend(vm, {
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
            },
            jobStarted: false,
            completedTimes: {},
            completedSteps: {}
        });

        vm.$onInit = function() {

            vm.setValidation('creation', true);

            var model = vm.ratingEngine.latest_iteration.AI;
            vm.type = vm.ratingEngine.type.toLowerCase();

            vm.periodType = vm.datacollectionstatus.ApsRollingPeriod;

            console.log(model);

            vm.predictionType = model.predictionType;  
            vm.trainingSegment = model.trainingSegment;

            if (vm.type === 'cross_sell') {

                // console.log(model);
                var keys = Object.keys(model.advancedModelingConfig.cross_sell.filters),
                    purchasedBeforePeriod = model.advancedModelingConfig.cross_sell.filters['PURCHASED_BEFORE_PERIOD'],
                    csFilters = Object.keys(model.advancedModelingConfig.cross_sell.filters).length,
                    trainingSegment = model.trainingSegment,
                    trainingProducts = model.advancedModelingConfig.cross_sell.trainingProducts;

                if((keys.length === 0 || (purchasedBeforePeriod && csFilters === 1)) && 
                    (trainingSegment === null || trainingSegment === undefined) && 
                    (trainingProducts === null || trainingProducts === undefined)) {
                    vm.hasSettingsInfo = false;
                }

                vm.targetProducts = model.advancedModelingConfig.cross_sell.targetProducts;
                vm.modelingStrategy = model.advancedModelingConfig.cross_sell.modelingStrategy;
                vm.configFilters = model.advancedModelingConfig.cross_sell.filters;
                vm.trainingProducts = model.advancedModelingConfig.cross_sell.trainingProducts;

                // console.log(vm.configFilters);

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

                vm.updateSteps();

            } else if (vm.type == 'custom_event') {

                vm.hasSettingsInfo = true;
                vm.ratingEngineType = 'Custom Event'
                vm.prioritizeBy = 'Likely to Buy';

                var dataStore = model.advancedModelingConfig.custom_event.dataStores;
                vm.availableAttributes = dataStore.length == 1 ? RatingsEngineStore.formatTrainingAttributes(dataStore[0]) : RatingsEngineStore.formatTrainingAttributes(dataStore[0]) + ' + ' + RatingsEngineStore.formatTrainingAttributes(dataStore[1]);

                vm.updateSteps();

            }
        };

        vm.updateSteps = function(){
            if (vm.type === 'cross_sell') {
                vm.steps = [
                    {
                        label: 'Gathering Data',
                        hasStarted: !vm.startTimestamp || !vm.jobStarted,
                        showSpinner: !vm.startTimestamp || !vm.jobStarted,
                        startTimestamp: vm.startTimestamp
                    },
                    {
                        label: 'Profiling',
                        hasStarted: vm.processingFile || vm.completedTimes.load_data,
                        showSpinner: vm.processingFile,
                        startTimestamp: vm.completedTimes.load_data
                    },
                    {
                        label: 'Modeling',
                        hasStarted: vm.matchingToDataCloud || vm.completedTimes.create_global_target_market,
                        showSpinner: vm.matchingToDataCloud,
                        startTimestamp: vm.completedTimes.create_global_target_market
                    },
                    {
                        label: 'Scoring',
                        hasStarted: vm.scoringTrainingSet || vm.completedTimes.score_training_set,
                        showSpinner: vm.scoringTrainingSet,
                        startTimestamp: vm.completedTimes.score_training_set
                    }
                ];
            } else {
                vm.steps = [
                    {
                        label: 'Processing File',
                        hasStarted: vm.processingFile || !vm.jobStarted,
                        showSpinner: vm.processingFile || !vm.jobStarted,
                        startTimestamp: vm.completedTimes.load_data
                    },
                    {
                        label: 'Matching to Data Cloud',
                        hasStarted: vm.matchingToDataCloud,
                        showSpinner: vm.matchingToDataCloud,
                        startTimestamp: vm.completedTimes.match_data
                    },
                    {
                        label: 'Modeling and Scoring',
                        hasStarted: vm.modelingAndScoring,
                        showSpinner: vm.modelingAndScoring,
                        startTimestamp: vm.completedTimes.create_global_model
                    }
                ];
            }
        }

        vm.checkJobStatus = function() {
            var appId = vm.ratingEngine.latest_iteration.AI.modelingJobId ? vm.ratingEngine.latest_iteration.AI.modelingJobId : RatingsEngineStore.getApplicationId(); // update once backend sets modelingjobId for CE
            if (appId) {

                JobsStore.getJobFromApplicationId(appId).then(function(result) {
                    if(result.id) {

                        console.log(result);

                        vm.jobStarted = true;
                        vm.status = result.jobStatus;
                        vm.startTimestamp = result.startTimestamp;
                        vm.completedSteps = result.completedSteps;
                        vm.completedTimes = result.completedTimes;
                        vm.globalStep = vm.type == 'cross_sell' ? vm.completedTimes.create_global_target_market : vm.completedTimes.create_global_model;

                        vm.steps = [];

                        if(vm.type == 'cross_sell'){
                            vm.stepMultiplier = 5.5;
                            vm.processingFile = result.stepRunning == 'load_data';
                            vm.matchingToDataCloud = vm.completedTimes.load_data && !vm.globalStep;
                            vm.scoringTrainingSet = vm.globalStep && !vm.completedTimes.score_training_set;
                        } else {
                            vm.stepMultiplier = 5.5;
                            vm.processingFile = result.stepRunning == 'load_data';
                            vm.matchingToDataCloud = result.stepRunning == 'match_data';
                            vm.modelingAndScoring = result.stepRunning == 'create_global_model';
                        }

                        vm.updateSteps();

                        // Green status bar
                        if(result.stepsCompleted.length > 0){
                            var tmp = ((result.stepsCompleted.length / 2) * vm.stepMultiplier);
                            if(tmp > 100 && vm.status !== 'Completed'){
                                tmp = 99;
                            }
                            vm.progress = tmp + '%';
                        }
                        // Cancel $interval when completed
                        if(vm.status === 'Completed'){
                            vm.progress = 100 + '%';
                            $interval.cancel(vm.checkJobStatus);
                        } else if (vm.status == 'Failed') {
                            $interval.cancel(vm.checkJobStatus);
                        }
                    }
                });
            }
        }
        var promise = $interval(vm.checkJobStatus, 10000);
        $scope.$on('$destroy', function(){
            if(promise) {
                $interval.cancel(promise);
            }
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

        vm.showSetting = function(setting) {
            // console.log('SETTINGS', setting, vm.modelSettingsSummary[vm.type][setting]);
            return vm.modelSettingsSummary[vm.type][setting];
        }

        vm.getPeriodType = function(value) {
            return value > 1 ? vm.periodType + 's' : vm.periodType;
        }
    }

});