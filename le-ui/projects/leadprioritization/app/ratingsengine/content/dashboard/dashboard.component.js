angular.module('lp.ratingsengine.dashboard', [
    'mainApp.appCommon.directives.barchart'
])
.controller('RatingsEngineDashboard', function(
    $q, $stateParams, $state, $rootScope, $scope, 
    RatingsEngineStore, RatingsEngineService,  ModalStore,
    Dashboard, RatingEngine, Model, IsRatingEngine, IsPmml, Products
) {
    var vm = this;

    angular.extend(vm, {
        dashboard: Dashboard,
        ratingEngine: RatingEngine,
        modelSummary: Model,
        products: Products,
        barChartConfig: {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 5,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'integer',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': false,
            },
            'vlines': {
                'suffix': ''
            },
            'columns': [{
                'field': 'num_leads',
                'label': 'Records',
                'type': 'number',
                'chart': true,
            }]
        },
        barChartLiftConfig: {
            'data': {
                'tosort': true,
                'sortBy': 'bucket_name',
                'trim': true,
                'top': 5,
            },
            'chart': {
                'header':'Value',
                'emptymsg': '',
                'usecolor': true,
                'color': '#e8e8e8',
                'mousehover': false,
                'type': 'decimal',
                'showstatcount': false,
                'maxVLines': 3,
                'showVLines': true,
            },
            'vlines': {
                'suffix': 'x'
            },
            'columns': [{
                    'field': 'lift',
                    'label': 'Lift',
                    'type': 'string',
                    'suffix': 'x',
                    'chart': true
                }
            ]
        }
    });

    vm.initModalWindow = function() {
        vm.config = {
            'name': "rating_engine_deactivate",
            'type': 'sm',
            'title': 'Deactivate Model',
            'titlelength': 100,
            'dischargetext': 'CANCEL',
            'dischargeaction' :'cancel',
            'confirmtext': 'DEACTIVATE',
            'confirmaction' : 'deactivate',
            'icon': 'ico ico-model ico-black',
            'showclose': false
        };
    
        vm.modalCallback = function (args) {
            if('closedForced' === args.action) {
            }else if(vm.config.dischargeaction === args.action){
                vm.toggleModal();
            } else if(vm.config.confirmaction === args.action){

                var modal = ModalStore.get(vm.config.name);
                modal.waiting(true);
                modal.disableDischargeButton(true);
                vm.deactivateRating();
            
            }
        }
        vm.toggleModal = function () {
            var modal = ModalStore.get(vm.config.name);
            if(modal){
                modal.toggle();
            }
        }
        vm.viewUrl = function () {
            return 'app/ratingsengine/content/dashboard/deactive-message.component.html';
        }

        $scope.$on("$destroy", function() {
            ModalStore.remove(vm.config.name);
        });
    }

    vm.isActive = function(status) {
        return (status === 'ACTIVE' ? true : false);
    }

    vm.deactivateRating = function(){
        var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
        newRating = {
            id: vm.ratingEngine.id,
            status: newStatus
        }
        RatingsEngineService.saveRating(newRating).then(function(data){
            vm.ratingEngine = data;
            // vm.status_toggle = vm.isActive(data.status);
            // vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            vm.toggleModal();
        });
    }

    vm.status_toggle = vm.isActive(vm.ratingEngine.status);

    vm.toggleActive = function() {
        var active = vm.isActive(vm.ratingEngine.status);
        if(active && vm.dashboard.plays.length > 0){
            var modal = ModalStore.get(vm.config.name);
            modal.toggle();
        } else {
            var newStatus = (vm.isActive(vm.ratingEngine.status) ? 'INACTIVE' : 'ACTIVE'),
                newRating = {
                id: vm.ratingEngine.id,
                status: newStatus
            }
            RatingsEngineService.saveRating(newRating).then(function(data){
                $rootScope.$broadcast('statusChange', { 
                    activeStatus: data.status
                });
                vm.ratingEngine = data;
                vm.status_toggle = vm.isActive(data.status);
                vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            });
        }
    }

    vm.init = function() {
        vm.initModalWindow();

        vm.relatedItems = vm.dashboard.plays;
        vm.hasBuckets = vm.ratingEngine.counts != null;
        vm.statusIsActive = (vm.ratingEngine.status === 'ACTIVE');
        vm.isRulesBased = (vm.ratingEngine.type === 'RULE_BASED');

        var model = vm.ratingEngine.activeModel;

        // console.log(vm.ratingEngine);
        // console.log(vm.dashboard);

        if(vm.ratingEngine.type === 'CROSS_SELL' || vm.ratingEngine.type === 'CUSTOM_EVENT') {
            vm.ratingEngine.chartConfig = vm.barChartLiftConfig;
            vm.publishOrActivateButtonLabel = vm.dashboard.summary.bucketMetadata ? 'New Scoring Configuration' : 'Activate Scoring';
        } else {
            vm.ratingEngine.chartConfig = vm.barChartConfig;
        }        

        var newBucketMetadata = [],
            dummyNewBucketData = [{
                "bucket_name": "A",
                "num_leads": 11,
                "lift": "0.3",
                "dummy": true
            },{
                "bucket_name": "B",
                "num_leads": 10,
                "lift": "1.3",
                "dummy": true
            },{
                "bucket_name": "C",
                "num_leads": 16,
                "lift": "0.8",
                "dummy": true
            },{
                "bucket_name": "D",
                "num_leads": 18,
                "lift": "0.9",
                "dummy": true
            },{
                "bucket_name": "F",
                "num_leads": 14,
                "lift": "0.5",
                "dummy": true
            }];

        if(vm.dashboard.summary.bucketMetadata && vm.dashboard.summary.bucketMetadata.length > 0) {
            angular.forEach(vm.dashboard.summary.bucketMetadata, function(rating, key) {
                rating.lift = (Math.round( rating.lift * 10) / 10).toString();
                if(rating.num_leads > 0) {
                    newBucketMetadata.push(rating);
                };
                if(newBucketMetadata.length === 0) {
                    newBucketMetadata = dummyNewBucketData;
                }
            });
        } else {
            newBucketMetadata = dummyNewBucketData;
        }
        vm.ratingEngine.newBucketMetadata = newBucketMetadata;
        
        if (vm.isRulesBased) {
            vm.toggleScoringButtonText = (vm.status_toggle ? 'Deactivate Scoring' : 'Activate Scoring');
            vm.modelingStrategy = 'RULE_BASED';
        } else {

            var type = vm.ratingEngine.type.toLowerCase();
            if (type === 'cross_sell') {
                if ((Object.keys(model.AI.advancedModelingConfig[type].filters).length === 0 || (model.AI.advancedModelingConfig[type].filters['PURCHASED_BEFORE_PERIOD'] && Object.keys(model.AI.advancedModelingConfig[type].filters).length === 1)) && model.AI.trainingSegment === null && model.AI.advancedModelingConfig[type].filters.trainingProducts === null) {
                    vm.hasSettingsInfo = false;
                } else {
                    vm.hasSettingsInfo = true;
                }

                vm.targetProducts = model.AI.advancedModelingConfig[type].targetProducts;
                vm.modelingStrategy = model.AI.advancedModelingConfig[type].modelingStrategy;
                vm.configFilters = model.AI.advancedModelingConfig[type].filters;
                vm.trainingProducts = model.AI.advancedModelingConfig[type].trainingProducts;

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

                if (vm.modelingStrategy === 'CROSS_SELL_FIRST_PURCHASE') {
                    vm.ratingEngineType = 'First Purchase Cross-Sell'
                } else if (vm.modelingStrategy === 'CROSS_SELL_REPEAT_PURCHASE') {
                    vm.ratingEngineType = 'Repeat Purchase Cross-Sell'
                }
            } else {
                vm.modelingStrategy = 'CUSTOM_EVENT';
                vm.ratingEngineType = 'Custom Event';
            }

            vm.modelSummary = model.AI.modelSummaryId;
            vm.predictionType = model.AI.predictionType;
            vm.trainingSegment = model.AI.trainingSegment;

            if (vm.predictionType === 'PROPENSITY') {
                vm.prioritizeBy = 'Likely to Buy';
            } else if (vm.predictionType === 'EXPECTED_VALUE') {
                vm.prioritizeBy = 'Likely Amount of Spend';
            }
        }
    }

    vm.returnProductNameFromId = function(productId) {
        var products = vm.products,
            product = products.find(function(obj) { return obj.ProductId === productId.toString() });

        return product.ProductName;
    };

    vm.getCustomEventAvailableAttributes = function(model) {
        var dataStore = model.advancedModelingConfig.custom_event.dataStores;
        return dataStore.length == 1 ? RatingsEngineStore.formatTrainingAttributes(dataStore[0]) : 
                RatingsEngineStore.formatTrainingAttributes(dataStore[0]) + ' + ' + RatingsEngineStore.formatTrainingAttributes(dataStore[1]);
    };

    vm.init();
});
